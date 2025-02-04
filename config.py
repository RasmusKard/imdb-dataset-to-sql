import polars as pl
from sqlalchemy import create_engine, text

# ADD YOUR OWN DATABASE DETAILS!
SQL_USERNAME = "root"
SQL_PASSWORD = "1234"
SQL_ADDRESS = "localhost"
SQL_PORT = "3306"
SQL_DATABASE_NAME = "dataset_sql"


MYSQL_ENGINE = create_engine(
    f"mysql+mysqlconnector://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_ADDRESS}:{SQL_PORT}/{SQL_DATABASE_NAME}"
)

# DATA PREPROCESSING VARIABLES
TITLE_FILE_PATH = "temp_file_title"
RATINGS_FILE_PATH = "temp_file_ratings"
GENRES_FILE_PATH = "temp_file_genres"
TITLE_TABLE_NAME = "title"
GENRES_TABLE_NAME = "title_genres"

IMDB_TITLE_BASICS_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_TITLE_RATINGS_URL = "https://datasets.imdbws.com/title.ratings.tsv.gz"
PL_TITLE_SCHEMA = {
    "tconst": pl.String,
    "titleType": pl.Categorical,
    "primaryTitle": pl.String,
    "originalTitle": pl.String,
    "isAdult": pl.UInt8,
    "startYear": pl.UInt16,
    "endYear": pl.UInt16,
    "runtimeMinutes": pl.UInt32,
    "genres": pl.String,
}

PL_RATINGS_SCHEMA = {
    "tconst": pl.String,
    "averageRating": pl.Float32,
    "numVotes": pl.UInt32,
}
