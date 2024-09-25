import data_clean_modules as dm
import dataframe_to_mysql as dfsql
import config
from os import path, remove


dm.download_imdb_dataset(config.IMDB_TITLE_BASICS_URL, config.TITLE_FILE_PATH)
dm.download_imdb_dataset(config.IMDB_TITLE_RATINGS_URL, config.RATINGS_FILE_PATH)

dm.clean_title_data(
    file_path=config.TITLE_FILE_PATH,
    schema=config.PL_TITLE_SCHEMA,
    allowed_titles=config.ALLOWED_TITLETYPES,
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

dfsql.add_foreign_key_sql(
    foreign_key_creation_sql=config.FOREIGN_KEY_CREATION_SQL,
    sql_engine=config.MYSQL_ENGINE,
)

for file_path in [config.TITLE_FILE_PATH, config.GENRES_FILE_PATH]:
    if path.exists(file_path):
        remove(file_path)
