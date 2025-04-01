import modules.data_clean_modules as dm
import modules.dataframe_to_mysql as dfsql
import modules.const as const
from os import getenv, environ, listdir
import shutil
from sqlalchemy import create_engine, inspect
import configs.default
import tempfile
import sqlalchemy.types as sqltypes
from modules import settings_parsers
from modules.helpers import join_path_with_random_uuid, download_imdb_dataset
from typing import Any
from dotenv import load_dotenv


load_dotenv()

is_updater = getenv("IS_UPDATER", "False").lower() in ("true", "1", "t")
print(is_updater)

environ["POLARS_FORCE_NEW_STREAMING"] = "1"


with tempfile.TemporaryDirectory() as tmpdir:
    MAIN_FILE_PATH = join_path_with_random_uuid(tmpdir)
    RATINGS_FILE_PATH = join_path_with_random_uuid(tmpdir)
    GENRES_FILE_PATH = join_path_with_random_uuid(tmpdir)

    # Download ratings and title files from IMDb
    # download_imdb_dataset(const.IMDB_TITLE_BASICS_URL, MAIN_FILE_PATH)
    # download_imdb_dataset(const.IMDB_TITLE_RATINGS_URL, RATINGS_FILE_PATH)

    # FOR DEV TO AVOID REDOWNLOADING
    shutil.copy2("title.basics.tsv", MAIN_FILE_PATH)
    shutil.copy2("title.ratings.tsv", RATINGS_FILE_PATH)

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

    IS_STREAMING = SETTINGS.get("is_streaming", False)
    IS_BATCHING = SETTINGS.get("is_batching", False)
    IS_SPLIT_GENRES = SETTINGS.get("is_split_genres_into_reftable", False)
    if IS_SPLIT_GENRES:
        const.IMDB_DATA_ALLOWED_COLUMNS["genres"] = sqltypes.SMALLINT()
    IS_CONVERT_TTYPE = SETTINGS.get("is_convert_title_type_str_to_int")
    if IS_CONVERT_TTYPE:
        const.IMDB_DATA_ALLOWED_COLUMNS["titleType"] = sqltypes.SMALLINT()

    sql_url = getenv("SQL_URL")
    if not sql_url:
        raise Exception("SQL credentials not found in config")
    SQL_ENGINE = create_engine(sql_url)

    if (
        not SETTINGS.get("is_ignore_db_has_tables_error")
        and not is_updater
        and inspect(SQL_ENGINE).get_table_names()
    ):
        raise ValueError(
            "Given database already has tables, cancelling operation.\n"
            "If you'd like to ignore this warning and continue then change `is_ignore_db_has_tables_error` to `true` in config.json. (It's recommended to make a back-up of your data before doing this.)"
        )

    # verify that settings are valid
    tables_info = settings_parsers.get_settings_tables_validity(
        tables=TABLES,
        ALLOWED_COLUMNS=const.IMDB_DATA_ALLOWED_COLUMNS,
    )
    # if updating check that target tables match shape in settings
    if is_updater:
        reftable_names = [
            const.COL_NAME_REFTABLE_NAME[key]
            for key, value in {
                "genres": IS_SPLIT_GENRES,
                "titleType": IS_CONVERT_TTYPE,
            }.items()
            if value
        ]

        settings_parsers.get_is_settings_match_db_shape(
            tables_info=tables_info,
            sql_engine=SQL_ENGINE,
            reftable_names=reftable_names,
        )

    BATCH_COUNT = SETTINGS.get("batch_count", 1)
    dm.clean_title_and_join_with_ratings(
        title_path=MAIN_FILE_PATH,
        ratings_path=RATINGS_FILE_PATH,
        title_schema=const.PL_TITLE_SCHEMA,
        ratings_schema=const.PL_RATINGS_SCHEMA,
        is_batched=IS_BATCHING,
        tmpdir=tmpdir,
        batch_count=BATCH_COUNT,
        settings=SETTINGS,
    )

    if IS_SPLIT_GENRES:
        genres_column_name = "genres"

        dm.create_genres_file_from_title_file(
            main_file_path=MAIN_FILE_PATH,
            genres_file_path=GENRES_FILE_PATH,
            tmpdir=tmpdir,
            column_name=genres_column_name,
        )

        dm.drop_genres_from_title(main_file_path=MAIN_FILE_PATH, tmpdir=tmpdir)

        genres_values = dm.change_str_to_int(
            column_name=genres_column_name,
            file_path=GENRES_FILE_PATH,
            is_streaming=IS_STREAMING,
        )

        if not is_updater:
            dfsql.create_reference_table(
                sql_engine=SQL_ENGINE,
                value_dict=genres_values,
                column_name=genres_column_name,
            )

    if IS_CONVERT_TTYPE:
        titleType_column_name = "titleType"

        titleType_values = dm.change_str_to_int(
            column_name=titleType_column_name,
            file_path=MAIN_FILE_PATH,
            is_streaming=IS_STREAMING,
        )

        if not is_updater:
            dfsql.create_reference_table(
                sql_engine=SQL_ENGINE,
                value_dict=titleType_values,
                column_name=titleType_column_name,
            )

    for tbl_name, tbl_info in tables_info.items():
        dfsql.table_to_sql(
            table_info=tbl_info,
            table_name=tbl_name,
            sql_engine=SQL_ENGINE,
            sql_uri=sql_url,
            main_file_path=MAIN_FILE_PATH,
            genres_file_path=GENRES_FILE_PATH,
            settings=SETTINGS,
            tmpdir=tmpdir,
            is_updater=is_updater,
        )

    print("Processing complete")
