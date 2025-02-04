import data_clean_modules as dm
import dataframe_to_mysql as dfsql
import config
from os import path, remove
import json

# generate names for temp files here


# Download ratings and title files from IMDb
# dm.download_imdb_dataset(config.IMDB_TITLE_BASICS_URL, config.TITLE_FILE_PATH)
# dm.download_imdb_dataset(config.IMDB_TITLE_RATINGS_URL, config.RATINGS_FILE_PATH)


dm.clean_title_data(
    file_path=config.TITLE_FILE_PATH,
    schema=config.PL_TITLE_SCHEMA,
)

dm.join_title_ratings(
    title_path=config.TITLE_FILE_PATH,
    ratings_path=config.RATINGS_FILE_PATH,
    ratings_schema=config.PL_RATINGS_SCHEMA,
)

dm.create_genres_file_from_title_file(
    title_file_path=config.TITLE_FILE_PATH, genres_file_path=config.GENRES_FILE_PATH
)

dm.drop_genres_from_title(title_file_path=config.TITLE_FILE_PATH)

print("done cleaning")

titleType_values = dm.change_str_to_int(
    df_file_path=config.TITLE_FILE_PATH, column_name="titleType"
)


dfsql.create_reference_table(
    sql_engine=config.MYSQL_ENGINE, value_dict=titleType_values, column_name="titleType"
)

genres_values = dm.change_str_to_int(
    df_file_path=config.GENRES_FILE_PATH, column_name="genres"
)

dfsql.create_reference_table(
    sql_engine=config.MYSQL_ENGINE, value_dict=genres_values, column_name="genres"
)


dfsql.title_data_to_sql(
    title_table_creation_sql=config.TITLE_TABLE_CREATION_SQL,
    title_table_drop_sql=config.TITLE_TABLE_DROP_SQL,
    title_file_path=config.TITLE_FILE_PATH,
    title_table_name=config.TITLE_TABLE_NAME,
    sql_engine=config.MYSQL_ENGINE,
)

dfsql.genre_data_to_sql(
    genres_table_drop_sql=config.GENRES_TABLE_DROP_SQL,
    genres_table_creation_sql=config.GENRES_TABLE_CREATION_SQL,
    genres_file_path=config.GENRES_FILE_PATH,
    genres_table_name=config.GENRES_TABLE_NAME,
    sql_engine=config.MYSQL_ENGINE,
)

for file_path in [config.TITLE_FILE_PATH, config.GENRES_FILE_PATH]:
    if path.exists(file_path):
        remove(file_path)
