import polars as pl
import pathlib
from sqlalchemy import create_engine


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

# lf = (
#     pl.scan_csv(
#         "title.tsv",
#         separator="\t",
#         null_values=r"\N",
#         quote_char=None,
#         schema=title_schema,
#     )
#     .with_columns(pl.col("startYear").forward_fill())
#     .filter(pl.col("isAdult") == 0)
#     .filter(pl.col("titleType").is_in(ALLOWED_TITLETYPES))
#     .drop("originalTitle", "endYear", "isAdult")
#     .filter(pl.col("genres").is_not_null())
#     .with_columns(pl.col("genres").str.split(","))
#     .filter((pl.col("genres").list.contains("Adult")).not_())
#     .collect(streaming=True)
# )


# lf.write_parquet("title.parquet")

# ratings_schema = {
#     "tconst": pl.String,
#     "averageRating": pl.Float32,
#     "numVotes": pl.UInt32,
# }

# title_lf = pl.scan_parquet("title.parquet")

# ratings_lf = pl.scan_csv(
#     "title.ratings.tsv",
#     separator="\t",
#     null_values=r"\N",
#     schema=ratings_schema,
# )

# joined_lf = title_lf.join(ratings_lf, how="inner", on="tconst").collect(streaming=True)


# joined_lf.write_parquet("title.v2.parquet")


lf = (
    pl.scan_parquet("title.v2.parquet")
    .explode("genres")
    .select(["tconst", "genres"])
    .collect(streaming=True)
)


lf.write_parquet("ratings.final.parquet")


# title file drop ratings column


# set sql schema (correct dtypes and one to many relationship between title and genres)

# both to sql
