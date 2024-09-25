import gzip
from os import path, remove
from shutil import copyfileobj
from urllib.request import urlretrieve
import polars as pl


def download_imdb_dataset(url, output_path):
    gz_file_path = output_path + ".gz"
    urlretrieve(url, gz_file_path)

    with gzip.open(gz_file_path, "rb") as f_in:
        with open(output_path, "wb") as f_out:
            copyfileobj(f_in, f_out)

    if path.exists(gz_file_path):
        remove(gz_file_path)


def remove_old_save_new_file(dataframe_to_write, file_path):
    if path.exists(file_path):
        remove(file_path)
    dataframe_to_write.write_parquet(file_path)


def clean_title_data(file_path, schema, allowed_titles):

    lf = (
        pl.scan_csv(
            file_path,
            separator="\t",
            null_values=r"\N",
            quote_char=None,
            schema=schema,
        )
        # where startyear is None, replace it with last seen value
        .with_columns(pl.col("startYear").forward_fill())
        .filter(pl.col("isAdult") == 0)
        .filter(pl.col("titleType").is_in(allowed_titles))
        .drop("originalTitle", "endYear", "isAdult")
        .filter(pl.col("genres").is_not_null())
        .with_columns(pl.col("genres").str.split(","))
        # remove content with the genre "Adult"
        .filter((pl.col("genres").list.contains("Adult")).not_())
        .collect(streaming=True)
    )

    remove_old_save_new_file(dataframe_to_write=lf, file_path=file_path)


def join_title_ratings(title_path, ratings_path, ratings_schema):
    title_lf = pl.scan_parquet(title_path)

    ratings_lf = pl.scan_csv(
        ratings_path,
        separator="\t",
        null_values=r"\N",
        schema=ratings_schema,
    )

    title_lf = title_lf.join(ratings_lf, how="inner", on="tconst").collect(
        streaming=True
    )

    if path.exists(ratings_path):
        remove(ratings_path)

    remove_old_save_new_file(dataframe_to_write=title_lf, file_path=title_path)


def create_genres_file_from_title_file(title_file_path, genres_file_path):
    lf = (
        pl.scan_parquet(title_file_path)
        .explode("genres")
        .select(["tconst", "genres"])
        .collect(streaming=True)
    )
    remove_old_save_new_file(dataframe_to_write=lf, file_path=genres_file_path)


def drop_genres_from_title(title_file_path):
    lf = pl.scan_parquet(title_file_path).drop("genres").collect(streaming=True)
    remove_old_save_new_file(dataframe_to_write=lf, file_path=title_file_path)
