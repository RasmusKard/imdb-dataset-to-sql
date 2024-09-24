import random
import string
import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mysql import ENUM, SMALLINT, DECIMAL, MEDIUMINT, VARCHAR

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
title_file_path = "temp_file_title"
ratings_file_path = "temp_file_ratings"
genres_file_path = "temp_file_genres"

IMDB_TITLE_BASICS_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_TITLE_RATINGS_URL = "https://datasets.imdbws.com/title.ratings.tsv.gz"
pl_title_schema = {
    "tconst": pl.String,
    "titleType": pl.Categorical,
    "primaryTitle": pl.String,
    "originalTitle": pl.String,
    "isAdult": pl.UInt8,
    "startYear": pl.UInt16,
    "endYear": pl.UInt16,
    "runtimeMinutes": pl.UInt16,
    "genres": pl.String,
}

pl_ratings_schema = {
    "tconst": pl.String,
    "averageRating": pl.Float32,
    "numVotes": pl.UInt32,
}

# SQL VARIABLES

mysql_engine = create_engine(
    "mysql+mysqlconnector://root:1234@localhost:3306/dataset_sql"
)
TITLE_TABLE_NAME = "title"
TITLE_TABLE_DROP_SQL = text(f"""DROP TABLE IF EXISTS `{TITLE_TABLE_NAME}`;""")
TITLE_TABLE_CREATION_SQL = text(
    f"""
    CREATE TABLE `{TITLE_TABLE_NAME}` (
        `tconst` VARCHAR(12) NOT NULL,
        `titleType` ENUM({','.join([f"'{titletype}'" for titletype in ALLOWED_TITLETYPES])}
        ) NOT NULL,
        `primaryTitle` VARCHAR(400) NOT NULL,
        `startYear` SMALLINT UNSIGNED NOT NULL,
        `runtimeMinutes` SMALLINT UNSIGNED NULL,
        `averageRating` DECIMAL(3, 1) UNSIGNED NOT NULL,
        `numVotes` MEDIUMINT UNSIGNED NOT NULL,
        PRIMARY KEY (`tconst`),
        UNIQUE INDEX `tconst_UNIQUE` (`tconst` ASC) VISIBLE
    );"""
)

GENRES_TABLE_NAME = "title_genres"
GENRES_TABLE_DROP_SQL = text(f"""DROP TABLE IF EXISTS `{GENRES_TABLE_NAME}`;""")
GENRES_TABLE_CREATION_SQL = text(
    f"""
    CREATE TABLE `{GENRES_TABLE_NAME}` (
        `tconst` VARCHAR(12) NOT NULL,
        `genres` ENUM({','.join([f"'{genre}'" for genre in ALLOWED_GENRES])}
        ) NOT NULL,
        PRIMARY KEY (`tconst`, `genres`)
    );"""
)
# TITLE_TABLE_SCHEMA = {
#     "tconst": (VARCHAR(12), {"primary_key": True, "nullable": False}),
#     "titleType": (
#         ENUM(*ALLOWED_TITLETYPES),
#         {"nullable": False},
#     ),
#     "primaryTitle": (VARCHAR(400), {"nullable": False}),
#     "startYear": (SMALLINT(unsigned=True), {"nullable": False}),
#     "runtimeMinutes": (SMALLINT(unsigned=True), {"nullable": True}),
#     "averageRating": (DECIMAL(3, 1, unsigned=True), {"nullable": False}),
#     "numVotes": (MEDIUMINT(unsigned=True), {"nullable": False}),
# }

# GENRES_TABLE_SCHEMA = {
#     "tconst": (VARCHAR(12), {"primary_key": True, "nullable": False}),
#     "titleType": (
#         ENUM("tvMiniSeries", "tvSeries", "movie", "tvMovie", "tvSpecial"),
#         {"nullable": False},
#     ),
# }
