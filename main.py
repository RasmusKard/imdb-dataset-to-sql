import data_clean_modules as dm
import dataframe_to_mysql as dfsql
import globals
from os import path, remove
import json
from sqlalchemy import create_engine, inspect

# generate names for temp files here


# Download ratings and title files from IMDb
# dm.download_imdb_dataset(config.IMDB_TITLE_BASICS_URL, config.TITLE_FILE_PATH)
# dm.download_imdb_dataset(config.IMDB_TITLE_RATINGS_URL, config.RATINGS_FILE_PATH)

with open("./config.json") as conf_file:
    CONFIG = json.load(conf_file)


sql_creds = CONFIG.get("database")
if not sql_creds:
    raise Exception("SQL credentials not found in config")

SQL_ENGINE = create_engine(
    f"mysql+mysqlconnector://{sql_creds["user"]}:{sql_creds["password"]}@{sql_creds["host"]}:{sql_creds["port"]}/{sql_creds["database"]}"
)

if (
    not CONFIG.get("is_ignore_db_has_tables_warning")
    and inspect(SQL_ENGINE).get_table_names()
):
    raise Exception(
        "Given database already has tables, cancelling operation.\n"
        "If you'd like to ignore this warning and continue then change `is_ignore_db_has_tables_warning` in config.json. (It's recommended to make a back-up of your data before doing this.)"
    )

dm.clean_title_data(
    file_path=globals.TITLE_FILE_PATH,
    schema=globals.PL_TITLE_SCHEMA,
)

dm.join_title_ratings(
    title_path=globals.TITLE_FILE_PATH,
    ratings_path=globals.RATINGS_FILE_PATH,
    ratings_schema=globals.PL_RATINGS_SCHEMA,
)


if CONFIG.get("is_split_genres_into_reftable") == True:

    dm.create_genres_file_from_title_file(
        title_file_path=globals.TITLE_FILE_PATH,
        genres_file_path=globals.GENRES_FILE_PATH,
    )

    dm.drop_genres_from_title(title_file_path=globals.TITLE_FILE_PATH)

    genres_values = dm.change_str_to_int(
        df_file_path=globals.GENRES_FILE_PATH, column_name="genres"
    )

    dfsql.create_reference_table(
        sql_engine=SQL_ENGINE, value_dict=genres_values, column_name="genres"
    )
    dfsql.parquet_to_sql(
        parquet_file_path=globals.GENRES_FILE_PATH,
        table_name=globals.GENRES_TABLE_NAME,
        sql_engine=SQL_ENGINE,
    )

print("done cleaning")

titleType_values = dm.change_str_to_int(
    df_file_path=globals.TITLE_FILE_PATH, column_name="titleType"
)


dfsql.create_reference_table(
    sql_engine=SQL_ENGINE,
    value_dict=titleType_values,
    column_name="titleType",
)


# title and ratings parquet files to sql tables
dfsql.parquet_to_sql(
    parquet_file_path=globals.TITLE_FILE_PATH,
    table_name=globals.TITLE_TABLE_NAME,
    sql_engine=SQL_ENGINE,
)


# for file_path in [globals.TITLE_FILE_PATH, globals.GENRES_FILE_PATH]:
#     if path.exists(file_path):
#         remove(file_path)
