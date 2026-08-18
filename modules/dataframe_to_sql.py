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
    is_dialect_supported = sql_dialect_name in NATIVE_IMPORT_SUPPORTED_DIALECTS

    if (
        is_dialect_supported
        and sql_dialect_driver in NATIVE_IMPORT_SUPPORTED_DIALECTS[sql_dialect_name]
    ):

        tmp_path = join_path_with_random_uuid(tmpdir)
        lf.sink_csv(tmp_path)

        # Name the columns being loaded rather than relying on positional order. Without a
        # column list both dialects expect every column of the table, in declaration order,
        # so a table carrying anything this tool does not write - a column with a default, a
        # generated column - fails the load.
        with open(tmp_path) as header_file:
            csv_columns = header_file.readline().strip().split(",")
        quote = sql_engine.dialect.identifier_preparer.quote
        column_list = ", ".join(quote(c) for c in csv_columns)

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

        try:
            match sql_dialect_name:
                case "mysql":
                    sql_engine = create_engine(sql_uri + "?local_infile=1")
                    conn = sql_engine.raw_connection()
                    cur = conn.cursor()
                    cur.execute("SET GLOBAL local_infile=1;")

                    if is_updater:
                        # MySQL refuses TRUNCATE on a table a foreign key references, so
                        # the checks come off for the duration.
                        cur.execute("SET FOREIGN_KEY_CHECKS=0")
                        try:
                            cur.execute(f"""TRUNCATE TABLE {table_name}""")
                        finally:
                            cur.execute("SET FOREIGN_KEY_CHECKS=1")

                    sql_load = f"""
                    LOAD DATA LOCAL INFILE '{tmp_path}'
                    INTO TABLE {table_name}
                    FIELDS TERMINATED BY ','
                    ENCLOSED BY '"'
                    LINES TERMINATED BY '\\n'
                    IGNORE 1 ROWS
                    ({column_list});
                    """

                    cur.execute(sql_load)
                    cur.execute("SET GLOBAL local_infile=0;")

                case "postgresql":
                    conn = sql_engine.raw_connection()
                    cur = conn.cursor()

                    if is_updater:
                        # CASCADE because the loaded tables reference each other, and
                        # Postgres refuses a plain TRUNCATE on a referenced table even when
                        # the referencing table is empty.
                        cur.execute(f"""TRUNCATE TABLE {table_name} CASCADE""")

                    copy_sql = f"""
                    COPY {table_name} ({column_list}) FROM stdin WITH CSV HEADER
                    DELIMITER as ','
                    """

                    with open(tmp_path, "r") as f:
                        cur.copy_expert(sql=copy_sql, file=f)

        finally:
            conn.commit()
            cur.close()
            conn.close()
    else:
        if is_dialect_supported:
            warnings.warn(
                "\nWARNING: Falling back to Pandas.to_sql().\n"
                + f"Dialect is supported for native data-import but driver isn't, please use one of the following drivers for improved import speed: {NATIVE_IMPORT_SUPPORTED_DIALECTS[sql_dialect_name]}"
            )

        df = lf.collect().to_pandas(use_pyarrow_extension_array=True)

        df.to_sql(
            name=table_name,
            con=sql_engine,
            if_exists="replace",
            index=False,
            dtype=dtype_dict,
        )
