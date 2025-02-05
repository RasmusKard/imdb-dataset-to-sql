import data_clean_modules as dm
import dataframe_to_mysql as dfsql
import globals
from os import path, remove
from sqlalchemy import create_engine, inspect
import configs.default
import tempfile
import os.path
import os
import polars as pl
import sqlalchemy.types as sqltypes

# generate names for temp files here

with tempfile.TemporaryDirectory() as tmpdir:

    MAIN_FILE = "title_file"
    RATINGS_FILE = "ratings_file"
    # Download ratings and title files from IMDb
    # dm.download_imdb_dataset(globals.IMDB_TITLE_BASICS_URL, "title.basics.tsv")
    # dm.download_imdb_dataset(
    #     config.IMDB_TITLE_RATINGS_URL, os.path.join(tmpdir, RATINGS_FILE)
    # )

    SELECTED_CONFIG = configs.default.config_dict
    SETTINGS = SELECTED_CONFIG.get("settings")
    TABLES = SELECTED_CONFIG.get("tables").items()

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
            "If you'd like to ignore this warning and continue then change `is_ignore_db_has_tables_warning` in config.json. (It's recommended to make a back-up of your data before doing this.)"
        )

    lf = dm.clean_title_data(
        file_path=globals.TITLE_FILE_PATH,
        schema=globals.PL_TITLE_SCHEMA,
    )

    lf = dm.join_title_ratings(
        title_lf=lf,
        ratings_path=globals.RATINGS_FILE_PATH,
        ratings_schema=globals.PL_RATINGS_SCHEMA,
    )

    dm.split_columns_into_files(tmpdir=tmpdir, lf=lf)

    # create temp folder
    # do cleaning in that folder
    # once cleaning is done split files into tconst:other_column files
    #

    if SETTINGS.get("is_split_genres_into_reftable") == True:

        # dm.create_genres_file_from_title_file(
        #     title_file_path=globals.TITLE_FILE_PATH,
        #     genres_file_path=globals.GENRES_FILE_PATH,
        # )

        # dm.drop_genres_from_title(title_file_path=globals.TITLE_FILE_PATH)

        genres_column_name = "genres"

        genres_values = dm.change_str_to_int(
            tmpdir=tmpdir,
            column_name=genres_column_name,
        )

        dfsql.create_reference_table(
            sql_engine=SQL_ENGINE,
            value_dict=genres_values,
            column_name=genres_column_name,
        )

    if SETTINGS.get("is_convert_title_type_str_to_int"):
        titleType_values = dm.change_str_to_int(
            df_file_path=globals.TITLE_FILE_PATH, column_name="titleType"
        )

        dfsql.create_reference_table(
            sql_engine=SQL_ENGINE,
            value_dict=titleType_values,
            column_name="titleType",
        )

    for table_name, table_dict in TABLES:
        dfsql.table_to_sql(
            tmpdir=tmpdir,
            table_dict=table_dict,
            table_name=table_name,
            sql_engine=SQL_ENGINE,
            settings=SETTINGS,
        )
