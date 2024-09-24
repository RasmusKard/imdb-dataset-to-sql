import random
import string
import polars as pl

# Random temp file names
title_file_path = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
ratings_file_path = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))

IMDB_TITLE_BASICS_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_TITLE_RATINGS_URL = "https://datasets.imdbws.com/title.ratings.tsv.gz"

mysql_engine = "mysql+mysqlconnector://root:1234@localhost:3306/dataset_sql"

title_schema = {
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

ALLOWED_TITLETYPES = ["movie", "tvSeries", "tvMovie", "tvSpecial", "tvMiniSeries"]

ratings_schema = {
    "tconst": pl.String,
    "averageRating": pl.Float32,
    "numVotes": pl.UInt32,
}
