import os.path
import re

import pandas as pd
import warnings
import polars as pl
from modules.const import (
    PL_SPLIT_GENRES_FILE_SCHEMA,
    PL_SCHEMA_OVERRIDE,
    COL_NAME_REFTABLE_NAME,
)
from modules.helpers import join_path_with_random_uuid
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.types import SMALLINT

_SAFE_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _assert_scratch_file(path, directory):
    """Refuse any path that does not resolve under `directory`.

    The scratch path reaches a LOAD DATA / COPY statement as a literal (neither
    statement accepts a bound parameter for a file), so its confinement is asserted
    rather than assumed.
    """
    root = os.path.realpath(directory)
    if not os.path.realpath(path).startswith(root + os.sep):
        raise ValueError(f"{path!r} is not under {directory!r}")


def _quoted_identifier(dialect):
    """Return a function that quotes an SQL identifier after a strict charset check.

    For the statements that take no bound parameters for names (TRUNCATE, LOAD DATA),
    the charset gate is the actual boundary - the names come from the loader config and
    the CSV header it just wrote, never from anything request-borne - and the dialect's
    own quoting then handles reserved words.
    """
    quote = dialect.identifier_preparer.quote

    def quoted(name):
        if not _SAFE_IDENTIFIER.fullmatch(name):
            raise ValueError(f"refusing unsafe SQL identifier: {name!r}")
        return quote(name)

    return quoted


def clear_table(sql_engine, table_name):
    """Empty a table without dropping it.

    Used instead of `if_exists="replace"` when the table already exists, so that
    constraints, indexes and grants defined outside this tool survive a reload. The
    statement is dialect specific because neither database can express the other's:
    MySQL has no `TRUNCATE ... CASCADE`, and it refuses `TRUNCATE` outright on a table
    a foreign key points at.
    """
    quoted = sql_engine.dialect.identifier_preparer.quote(table_name)

    with sql_engine.begin() as conn:
        match sql_engine.dialect.name:
            case "postgresql":
                conn.execute(text(f"TRUNCATE TABLE {quoted} CASCADE"))
            case "mysql":
                conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
                try:
                    conn.execute(text(f"TRUNCATE TABLE {quoted}"))
                finally:
                    conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
            case _:
                conn.execute(text(f"DELETE FROM {quoted}"))


def create_reference_table(sql_engine, value_dict, column_name):
    # The table name comes from the mapping rather than the column name, so that a
    # camelCase IMDb column (`titleType`) still lands in a snake_case table
    # (`title_type_ref`) and matches what get_is_settings_match_db_shape() looks for.
    table_name = COL_NAME_REFTABLE_NAME.get(column_name, f"{column_name}_ref")
    id_col = f"{column_name}_id"
    str_col = f"{column_name}_str"

    ref_data = {id_col: list(value_dict.values()), str_col: list(value_dict.keys())}
    df = pd.DataFrame(ref_data)

    # `if_exists="replace"` drops the table, which is impossible while another table holds
    # a foreign key onto it, and throws away anything this tool did not create. Refresh the
    # rows in place when the table is already there. On Postgres the CASCADE empties the
    # dependent data tables too; they are reloaded immediately afterwards.
    if inspect(sql_engine).has_table(table_name):
        clear_table(sql_engine, table_name)
        df.to_sql(table_name, con=sql_engine, if_exists="append", index=False)
    else:
        df.to_sql(
            table_name,
            con=sql_engine,
            if_exists="replace",
            index=False,
            dtype={id_col: SMALLINT()},
        )


def table_to_sql(
    table_info,
    table_name,
    sql_engine,
    main_file_path,
    genres_file_path,
    settings,
    tmpdir,
    sql_uri,
    is_updater,
):
    dtype_dict = table_info["dtype_dict"]
    values_dict = table_info["values_dict"]
    cols_needed = values_dict.keys()

    # handle using the split genres file if needed
    if settings.get("is_split_genres_into_reftable") and "genres" in cols_needed:
        # check if its just tconst and genres
        # if not then warn and merge the main and genres files
        if {"genres", "tconst"} == set(cols_needed):
            lf = pl.scan_csv(
                genres_file_path, schema=PL_SPLIT_GENRES_FILE_SCHEMA
            ).rename(values_dict)
        else:
            lf0 = pl.scan_csv(genres_file_path, schema=PL_SPLIT_GENRES_FILE_SCHEMA)

            main_file_cols = list(cols_needed)
            main_file_cols.remove("genres")
            lf1 = pl.scan_csv(
                main_file_path,
                schema_overrides=PL_SCHEMA_OVERRIDE,
            ).select(main_file_cols)

            lf = lf0.join(lf1, on="tconst", how="inner").rename(values_dict)

            warnings.warn(
                "\nWARNING: It's not recommended to store values other than `tconst` in the same table as a split `genres` column.\n"
                + "It's better to use a Foreign Key constraint on tconst of the split `genres` table and the table with your other columns."
            )
    else:
        lf = (
            pl.scan_csv(
                main_file_path,
                schema_overrides=PL_SCHEMA_OVERRIDE,
            )
            .select(cols_needed)
            .rename(values_dict)
        )

    # supported_dialect:[supported_drivers]
    NATIVE_IMPORT_SUPPORTED_DIALECTS = {
        "mysql": ["mysqldb", "pymysql"],
        "postgresql": ["psycopg2"],
    }

    sql_dialect_name = sql_engine.dialect.name
    sql_dialect_driver = sql_engine.dialect.driver
    # Native bulk import (COPY from a scratch CSV) is Postgres-only: psycopg2's
    # sql.Identifier composes table and column names as first-class identifier objects,
    # while MySQL's LOAD DATA and TRUNCATE accept neither bound parameters nor any safe
    # composition API, so building those statements as strings is not an option here.
    # MySQL keeps a correct, slower path in the fallback below: FK-safe clear_table()
    # plus pandas' fully parameterized to_sql().
    is_native_import = (
        sql_dialect_name == "postgresql"
        and sql_dialect_driver in NATIVE_IMPORT_SUPPORTED_DIALECTS["postgresql"]
    )

    if is_native_import:

        tmp_path = join_path_with_random_uuid(tmpdir)
        lf.sink_csv(tmp_path)
        _assert_scratch_file(tmp_path, tmpdir)

        # Name the columns being loaded rather than relying on positional order. Without a
        # column list both dialects expect every column of the table, in declaration order,
        # so a table carrying anything this tool does not write - a column with a default, a
        # generated column - fails the load.
        with open(tmp_path) as header_file:
            csv_columns = header_file.readline().strip().split(",")

        # create the table using the csv headers and dtype_dict
        # skip if updating to not mess up indices
        if not is_updater:
            df = pd.read_csv(tmp_path, nrows=0)
            df[:0].to_sql(
                name=table_name,
                con=sql_engine,
                if_exists="replace",
                index=False,
                dtype=dtype_dict,
            )

        from psycopg2 import sql as pgsql

        conn = sql_engine.raw_connection()
        # raw_connection() returns SQLAlchemy's pooled proxy around the DBAPI connection;
        # psycopg2's C-level quote_ident (inside as_string below) rejects the proxy, so
        # pass the driver-level connection it wraps.
        dbapi_conn = getattr(conn, "driver_connection", None) or getattr(conn, "connection", conn)
        try:
            cur = conn.cursor()

            if is_updater:
                # CASCADE because the loaded tables reference each other, and Postgres
                # refuses a plain TRUNCATE on a referenced table even when the referencing
                # table is empty.
                cur.execute(
                    pgsql.SQL("TRUNCATE TABLE {} CASCADE").format(
                        pgsql.Identifier(table_name)
                    )
                )

            # The row data travels on stdin; only names appear in the statement, and they
            # are composed as identifier objects, never interpolated into the text.
            copy_query = pgsql.SQL(
                "COPY {} ({}) FROM stdin WITH CSV HEADER DELIMITER as ','"
            ).format(
                pgsql.Identifier(table_name),
                pgsql.SQL(", ").join(pgsql.Identifier(c) for c in csv_columns),
            )

            with open(tmp_path, "r") as f:
                cur.copy_expert(sql=copy_query.as_string(dbapi_conn), file=f)

            conn.commit()
        finally:
            cur.close()
            conn.close()
    else:
        if sql_dialect_name in NATIVE_IMPORT_SUPPORTED_DIALECTS:
            warnings.warn(
                "\nWARNING: Falling back to Pandas.to_sql().\n"
                + f"Native {sql_dialect_name} import is not used here because its bulk-load "
                + "statements cannot be written without assembling SQL strings; the fallback "
                + "is parameterized and FK-safe, at pandas speed."
            )

        df = lf.collect().to_pandas(use_pyarrow_extension_array=True)

        if is_updater:
            # The table is migration-owned: refresh the rows in place so indexes,
            # constraints and grants survive, mirroring the Postgres COPY path.
            clear_table(sql_engine, table_name)
            df.to_sql(
                name=table_name,
                con=sql_engine,
                if_exists="append",
                index=False,
                dtype=dtype_dict,
            )
        else:
            df.to_sql(
                name=table_name,
                con=sql_engine,
                if_exists="replace",
                index=False,
                dtype=dtype_dict,
            )
