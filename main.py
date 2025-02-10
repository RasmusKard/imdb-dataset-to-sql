import modules.data_clean_modules as dm
import modules.dataframe_to_mysql as dfsql
import modules.const as const
from os import getenv
from sqlalchemy import create_engine, inspect
import configs.default
import tempfile
import sqlalchemy.types as sqltypes
from modules import settings_parsers
from modules.helpers import join_path_with_random_uuid, download_imdb_dataset
from typing import Any

# generate names for temp files here

is_updater = getenv("ISUPDATER", "False").lower() in ("true", "1", "t")

with tempfile.TemporaryDirectory() as tmpdir:

    MAIN_FILE_PATH = join_path_with_random_uuid(tmpdir)
    RATINGS_FILE_PATH = join_path_with_random_uuid(tmpdir)
    GENRES_FILE_PATH = join_path_with_random_uuid(tmpdir)

    # Download ratings and title files from IMDb
    download_imdb_dataset(const.IMDB_TITLE_BASICS_URL, MAIN_FILE_PATH)
    download_imdb_dataset(const.IMDB_TITLE_RATINGS_URL, RATINGS_FILE_PATH)

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

    IS_STREAMING = SETTINGS.get("use_streaming")
    IS_SPLIT_GENRES = SETTINGS.get("is_split_genres_into_reftable")
    if IS_SPLIT_GENRES:
        const.IMDB_DATA_ALLOWED_COLUMNS["genres"] = sqltypes.SMALLINT()
    IS_CONVERT_TTYPE = SETTINGS.get("is_convert_title_type_str_to_int")
    if IS_CONVERT_TTYPE:
        const.IMDB_DATA_ALLOWED_COLUMNS["titleType"] = sqltypes.SMALLINT()

    sql_creds = SETTINGS.get("database")
    if not sql_creds:
        raise Exception("SQL credentials not found in config")

    sql_dialect = sql_creds["dialect"]
    sql_driver = sql_creds.get("driver")
    SQL_ENGINE = create_engine(
        f"{sql_dialect}{f"+{sql_driver}" if sql_driver else ""}://{sql_creds["user"]}:{sql_creds["password"]}@{sql_creds["host"]}:{sql_creds["port"]}/{sql_creds["database"]}"
    )

    if (
        not SETTINGS.get("is_ignore_db_has_tables_warning")
        and not is_updater
        and inspect(SQL_ENGINE).get_table_names()
    ):
        raise Exception(
            "Given database already has tables, cancelling operation.\n"
            "If you'd like to ignore this warning and continue then change `is_ignore_db_has_tables_warning` to `true` in config.json. (It's recommended to make a back-up of your data before doing this.)"
        )

    # verify that settings are valid
    tables_info = settings_parsers.get_settings_tables_validity(
        tables=TABLES,
        ALLOWED_COLUMNS=const.IMDB_DATA_ALLOWED_COLUMNS,
        is_updater=is_updater,
    )
    # if updating check that target tables match shape in settings
    if is_updater:
        settings_parsers.get_is_settings_match_db_shape(
            tables_info=tables_info, sql_engine=SQL_ENGINE
        )

    lf = dm.clean_title_data(
        file_path=MAIN_FILE_PATH,
        schema=const.PL_TITLE_SCHEMA,
    )

    lf = dm.join_title_ratings(
        title_lf=lf,
        ratings_path=RATINGS_FILE_PATH,
        ratings_schema=const.PL_RATINGS_SCHEMA,
    )

    df = lf.collect(streaming=IS_STREAMING)
    df.write_parquet(MAIN_FILE_PATH)
    del df

    if IS_SPLIT_GENRES:
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

    if IS_CONVERT_TTYPE:
        titleType_values = dm.change_str_to_int(
            column_name="titleType", file_path=MAIN_FILE_PATH
        )

        dfsql.create_reference_table(
            sql_engine=SQL_ENGINE,
            value_dict=titleType_values,
            column_name="titleType",
        )

    tbl_message_info = {}
    for tbl_name, tbl_info in tables_info.items():
        tbl_message_info[tbl_name] = dfsql.table_to_sql(
            table_info=tbl_info,
            table_name=tbl_name,
            sql_engine=SQL_ENGINE,
            main_file_path=MAIN_FILE_PATH,
            genres_file_path=GENRES_FILE_PATH,
            settings=SETTINGS,
        )

    print("Processing complete. Tables are as follows:\n")
    for tbl_name, tbl_msg_dict in tbl_message_info.items():
        print(
            f"Table name: `{tbl_name}`\n"
            f"Row count: {tbl_msg_dict.get('row_count')}\n"
            f"Columns: {tbl_msg_dict.get('columns')}\n"
            f"Dtypes: {tbl_msg_dict.get('dtypes')}\n",
        )
