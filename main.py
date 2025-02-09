import modules.data_clean_modules as dm
import modules.dataframe_to_mysql as dfsql
import globals
from os import path, remove
from sqlalchemy import create_engine, inspect
import configs.default
import tempfile
import sqlalchemy.types as sqltypes
from modules.helpers import join_path_with_random_uuid, download_imdb_dataset
from typing import Any

# generate names for temp files here


with tempfile.TemporaryDirectory() as tmpdir:

    MAIN_FILE_PATH = join_path_with_random_uuid(tmpdir)
    RATINGS_FILE_PATH = join_path_with_random_uuid(tmpdir)
    GENRES_FILE_PATH = join_path_with_random_uuid(tmpdir)

    # Download ratings and title files from IMDb
    download_imdb_dataset(globals.IMDB_TITLE_BASICS_URL, MAIN_FILE_PATH)
    download_imdb_dataset(globals.IMDB_TITLE_RATINGS_URL, RATINGS_FILE_PATH)

    SELECTED_CONFIG: dict[str, Any] = configs.default.config_dict
    SETTINGS: dict | None = SELECTED_CONFIG.get("settings")
    if not SETTINGS:
        raise Exception(
            "`settings`not found in config dict. Config is incomplete or incorrectly formatted."
        )
    TABLES: dict | None = SELECTED_CONFIG.get("tables")
    if not TABLES:
        raise Exception(
            "`tables` not found in config dict. Config is incomplete or incorrectly formatted."
        )

    if SETTINGS.get("is_split_genres_into_reftable"):
        globals.IMDB_DATA_ALLOWED_COLUMNS["genres"] = sqltypes.SMALLINT()

    if SETTINGS.get("is_convert_title_type_str_to_int"):
        globals.IMDB_DATA_ALLOWED_COLUMNS["titleType"] = sqltypes.SMALLINT()

    IS_STREAMING = SETTINGS.get("use_streaming")

    sql_creds = SETTINGS.get("database")
    if not sql_creds:
        raise Exception("SQL credentials not found in config")

    SQL_ENGINE = create_engine(
        f"mysql+mysqlconnector://{sql_creds["user"]}:{sql_creds["password"]}@{sql_creds["host"]}:{sql_creds["port"]}/{sql_creds["database"]}"
    )

    if (
        not SETTINGS.get("is_ignore_db_has_tables_warning")
        and inspect(SQL_ENGINE).get_table_names()
    ):
        raise Exception(
            "Given database already has tables, cancelling operation.\n"
            "If you'd like to ignore this warning and continue then change `is_ignore_db_has_tables_warning` to `true` in config.json. (It's recommended to make a back-up of your data before doing this.)"
        )

    lf = dm.clean_title_data(
        file_path=MAIN_FILE_PATH,
        schema=globals.PL_TITLE_SCHEMA,
    )

    lf = dm.join_title_ratings(
        title_lf=lf,
        ratings_path=RATINGS_FILE_PATH,
        ratings_schema=globals.PL_RATINGS_SCHEMA,
    )

    df = lf.collect(streaming=IS_STREAMING)
    df.write_parquet(MAIN_FILE_PATH)
    del df

    if SETTINGS.get("is_split_genres_into_reftable") == True:

        genres_column_name = "genres"

        dm.create_genres_file_from_title_file(
            main_file_path=MAIN_FILE_PATH,
            genres_file_path=GENRES_FILE_PATH,
            tmpdir=tmpdir,
            column_name=genres_column_name,
        )

        dm.drop_genres_from_title(main_file_path=MAIN_FILE_PATH)

        genres_values = dm.change_str_to_int(
            column_name=genres_column_name, file_path=GENRES_FILE_PATH
        )

        dfsql.create_reference_table(
            sql_engine=SQL_ENGINE,
            value_dict=genres_values,
            column_name=genres_column_name,
        )

    if SETTINGS.get("is_convert_title_type_str_to_int"):
        titleType_values = dm.change_str_to_int(
            column_name="titleType", file_path=MAIN_FILE_PATH
        )

        dfsql.create_reference_table(
            sql_engine=SQL_ENGINE,
            value_dict=titleType_values,
            column_name="titleType",
        )

    for table_name, table_dict in TABLES.items():
        dfsql.table_to_sql(
            table_dict=table_dict,
            table_name=table_name,
            sql_engine=SQL_ENGINE,
            main_file_path=MAIN_FILE_PATH,
            genres_file_path=GENRES_FILE_PATH,
            settings=SETTINGS,
        )
