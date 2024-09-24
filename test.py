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
    "isAdult": pl.Int8,
    "startYear": pl.Int16,
    "endYear": pl.Int16,
    "runtimeMinutes": pl.Int64,
    "genres": pl.String,
}

ALLOWED_TITLETYPES = ["movie", "tvSeries", "tvMovie", "tvSpecial", "tvMiniSeries"]


lf = (
    pl.scan_csv(
        "title.tsv",
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
    .filter((pl.col("genres").str.split(",").list.contains("Adult")).not_())
    .collect(streaming=True)
)

lf.write_parquet("title.parquet")

lf1 = pl.scan_parquet("title.parquet").group_by("titleType").agg(pl.count())

print(lf1.collect())

# lf2 = pl.scan_csv("title.ratings.tsv", separator="\t", null_values=r"\N")

# lf1.join(lf2, on=pl.col("tconst"), how="inner").collect()

# lf1.write_parquet("final.parquet")


# lf.write_database(table_name='titles',connection=mysql_engine, if_table_exists='replace')
