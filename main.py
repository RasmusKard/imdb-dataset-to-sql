from sqlalchemy import create_engine
from config import (
    IMDB_TITLE_BASICS_URL,
    IMDB_TITLE_RATINGS_URL,
    title_file_path,
    ratings_file_path,
    mysql_engine,
    pl_title_schema,
    pl_ratings_schema,
    ALLOWED_TITLETYPES,
    genres_file_path,
)
import data_clean_modules as dm


# Define configuration variables
IMDB_TITLE_BASICS_URL = IMDB_TITLE_BASICS_URL
IMDB_TITLE_RATINGS_URL = IMDB_TITLE_RATINGS_URL
TITLE_FILE_PATH = title_file_path
RATINGS_FILE_PATH = ratings_file_path
GENRES_FILE_PATH = genres_file_path
MYSQL_ENGINE = mysql_engine
TITLE_SCHEMA = pl_title_schema
RATINGS_SCHEMA = pl_ratings_schema
ALLOWED_TITLETYPES = ALLOWED_TITLETYPES


dm.download_imdb_dataset(IMDB_TITLE_BASICS_URL, TITLE_FILE_PATH)
dm.download_imdb_dataset(IMDB_TITLE_RATINGS_URL, RATINGS_FILE_PATH)

dm.clean_title_data(
    file_path=TITLE_FILE_PATH, schema=TITLE_SCHEMA, allowed_titles=ALLOWED_TITLETYPES
)

dm.join_title_ratings(
    title_path=TITLE_FILE_PATH,
    ratings_path=RATINGS_FILE_PATH,
    ratings_schema=RATINGS_SCHEMA,
)

dm.create_genres_file_from_title_file(
    title_file_path=TITLE_FILE_PATH, genres_file_path=GENRES_FILE_PATH
)

dm.drop_genres_from_title(title_file_path=TITLE_FILE_PATH)
