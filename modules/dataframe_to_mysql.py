import pandas as pd
import warnings
import polars as pl
from modules.const import PL_SPLIT_GENRES_FILE_SCHEMA, PL_SCHEMA_OVERRIDE
from modules.helpers import join_path_with_random_uuid
from sqlalchemy import create_engine
from sqlalchemy.types import SMALLINT


def create_reference_table(sql_engine, value_dict, column_name):
    table_name = f"{column_name}_ref"
    id_col = f"{column_name}_id"
    str_col = f"{column_name}_str"

    ref_data = {id_col: list(value_dict.values()), str_col: list(value_dict.keys())}
    df = pd.DataFrame(ref_data)

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
                "WARNING: It's not recommended to store values other than `tconst` in the same table as a split `genres` column.\n"
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
    NATIVE_IMPORT_SUPPORTED_DIALECTS = {"mysql": ["pymysql", "mysqldb"]}
    sql_dialect = sql_engine.dialect
    sql_dialect_name = sql_dialect.name
    if (
        sql_dialect_name == "mysql"
        and sql_dialect.driver in NATIVE_IMPORT_SUPPORTED_DIALECTS["mysql"]
    ):
        sql_engine = create_engine(sql_uri + "?local_infile=1")

        conn = sql_engine.raw_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SET GLOBAL local_infile=1;")

            tmp_path = join_path_with_random_uuid(tmpdir)
            lf.sink_csv(tmp_path)

            # create table with csv header and dtypes
            df = pd.read_csv(tmp_path, nrows=0)
            df[:0].to_sql(
                name=table_name,
                con=sql_engine,
                if_exists="replace",
                index=False,
                dtype=dtype_dict,
            )

            sql_load = f"""
                LOAD DATA LOCAL INFILE '{tmp_path}'
                INTO TABLE {table_name}
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                IGNORE 1 ROWS;
                """

            cursor.execute(sql_load)
            cursor.execute("SET GLOBAL local_infile=0;")
            conn.commit()
        finally:
            cursor.close()
            conn.close()
    else:
        if sql_dialect_name in NATIVE_IMPORT_SUPPORTED_DIALECTS:
            warnings.warn(
                "WARNING: Falling back to Pandas.to_sql().\n"
                + f"Dialect is supported for native data-import but driver isn't, please use one of the following drivers for improved import speed: {NATIVE_IMPORT_SUPPORTED_DIALECTS[sql_dialect_name]}"
            )

        df = lf.collect(new_streaming=True).to_pandas(use_pyarrow_extension_array=True)

        df.to_sql(
            name=table_name,
            con=sql_engine,
            if_exists="replace",
            index=False,
            dtype=dtype_dict,
        )
