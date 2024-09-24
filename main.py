from sqlalchemy import create_engine
import config
import data_clean_modules as dm


# Define configuration variables
IMDB_TITLE_BASICS_URL = config.IMDB_TITLE_BASICS_URL
IMDB_TITLE_RATINGS_URL = config.IMDB_TITLE_RATINGS_URL
TITLE_FILE_PATH = config.title_file_path
RATINGS_FILE_PATH = config.ratings_file_path
MYSQL_ENGINE = config.mysql_engine
TITLE_SCHEMA = config.title_schema
RATINGS_SCHEMA = config.ratings_schema
ALLOWED_TITLETYPES = config.ALLOWED_TITLETYPES

mysql_engine = create_engine(MYSQL_ENGINE)


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
    title_file_path=TITLE_FILE_PATH, ratings_file_path=RATINGS_FILE_PATH
)

dm.drop_genres_from_title(title_file_path=TITLE_FILE_PATH)
