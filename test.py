import polars as pl
import random
import string
from urllib.request import urlretrieve
from sqlalchemy import create_engine
import os
import gzip
import shutil

# Random temp file names
title_file_path = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
ratings_file_path = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
IMDB_TITLE_BASICS_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_TITLE_RATINGS_URL = "https://datasets.imdbws.com/title.ratings.tsv.gz"


urlretrieve(IMDB_TITLE_BASICS_URL, title_file_path + ".gz")
urlretrieve(IMDB_TITLE_RATINGS_URL, ratings_file_path + ".gz")

for file_path in [title_file_path, ratings_file_path]:
    with gzip.open(file_path + ".gz", "rb") as f_in:
        with open(file_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    if os.path.exists(file_path + ".gz"):
        os.remove(file_path + ".gz")


mysql_engine = create_engine(
    "mysql+mysqlconnector://root:1234@localhost:3306/dataset_sql"
)

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

# Filter IMDb public dataset title.basics.tsv file

lf = (
    pl.scan_csv(
        title_file_path,
        separator="\t",
        null_values=r"\N",
        quote_char=None,
        schema=title_schema,
    )
    .with_columns(pl.col("startYear").forward_fill())
    .filter(pl.col("isAdult") == 0)
    .filter(pl.col("titleType").is_in(ALLOWED_TITLETYPES))
    .drop("originalTitle", "endYear", "isAdult")
    .filter(pl.col("genres").is_not_null())
    .with_columns(pl.col("genres").str.split(","))
    .filter((pl.col("genres").list.contains("Adult")).not_())
    .collect(streaming=True)
)

if os.path.exists(title_file_path):
    os.remove(title_file_path)

lf.write_parquet(title_file_path)

ratings_schema = {
    "tconst": pl.String,
    "averageRating": pl.Float32,
    "numVotes": pl.UInt32,
}

title_lf = pl.scan_parquet(title_file_path)

ratings_lf = pl.scan_csv(
    ratings_file_path,
    separator="\t",
    null_values=r"\N",
    schema=ratings_schema,
)

joined_lf = title_lf.join(ratings_lf, how="inner", on="tconst").collect(streaming=True)

if os.path.exists(title_file_path):
    os.remove(title_file_path)


joined_lf.write_parquet(title_file_path)


lf = (
    pl.scan_parquet(title_file_path)
    .explode("genres")
    .select(["tconst", "genres"])
    .collect(streaming=True)
)


lf.write_parquet(ratings_file_path)

lf = pl.scan_parquet(title_file_path).drop("genres").collect(streaming=True)

if os.path.exists(title_file_path):
    os.remove(title_file_path)

lf.write_parquet(title_file_path)

print(lf)

# title file drop ratings column


# set sql schema (correct dtypes and one to many relationship between title and genres)

# both to sql
