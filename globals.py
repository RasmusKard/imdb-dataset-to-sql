import polars as pl
import sqlalchemy.types as sqltypes


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

# Default dtype values for SQL tables
IMDB_DATA_ALLOWED_COLUMNS = {
    "tconst": sqltypes.String(20),
    "titleType": sqltypes.String(20),
    "primaryTitle": sqltypes.String(400),
    "originalTitle": sqltypes.String(400),
    "isAdult": sqltypes.SMALLINT(),
    "startYear": sqltypes.SMALLINT(),
    "endYear": sqltypes.SMALLINT(),
    "runtimeMinutes": sqltypes.INT(),
    "genres": sqltypes.String(40),
    "averageRating": sqltypes.Float(),
    "numVotes": sqltypes.INT(),
}
