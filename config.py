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


# GLOBAL VARIABLES
ALLOWED_TITLETYPES = ["movie", "tvSeries", "tvMovie", "tvSpecial", "tvMiniSeries"]
ALLOWED_GENRES = [
    "Comedy",
    "Musical",
    "Romance",
    "Western",
    "Drama",
    "Music",
    "Action",
    "Adventure",
    "Crime",
    "Mystery",
    "War",
    "Biography",
    "History",
    "Documentary",
    "Sport",
    "Horror",
    "Thriller",
    "Family",
    "Sci-Fi",
    "Fantasy",
    "Film-Noir",
    "Animation",
    "News",
    "Game-Show",
    "Talk-Show",
    "Reality-TV",
    "Short",
]

# DATA PREPROCESSING VARIABLES
TITLE_FILE_PATH = "temp_file_title"
RATINGS_FILE_PATH = "temp_file_ratings"
GENRES_FILE_PATH = "temp_file_genres"

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

# SQL VARIABLES

TITLE_TABLE_NAME = "title"
TITLE_TABLE_DROP_SQL = text(f"""DROP TABLE IF EXISTS `{TITLE_TABLE_NAME}`;""")
TITLE_TABLE_CREATION_SQL = text(
    f"""
    CREATE TABLE `{TITLE_TABLE_NAME}` (
        `tconst` VARCHAR(12) NOT NULL UNIQUE PRIMARY KEY,
        `titleType` TINYINT UNSIGNED NOT NULL,
        `primaryTitle` VARCHAR(400) NOT NULL,
        `startYear` SMALLINT UNSIGNED NOT NULL,
        `runtimeMinutes` SMALLINT UNSIGNED NULL,
        `averageRating` DECIMAL(3, 1) UNSIGNED NOT NULL,
        `numVotes` MEDIUMINT UNSIGNED NOT NULL
    );"""
)

GENRES_TABLE_NAME = "title_genres"
GENRES_TABLE_DROP_SQL = text(f"""DROP TABLE IF EXISTS `{GENRES_TABLE_NAME}`;""")
GENRES_TABLE_CREATION_SQL = text(
    f"""
    CREATE TABLE `{GENRES_TABLE_NAME}` (
        `tconst` VARCHAR(12) NOT NULL,
        `genres` TINYINT UNSIGNED NOT NULL,
        PRIMARY KEY (`tconst`, `genres`)
    );"""
)
