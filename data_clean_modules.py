import gzip
from os import path, remove
from shutil import copyfileobj
from urllib.request import urlretrieve
import polars as pl
import configs.default
import os.path


title_file = "title.basics.tsv"
ratings_file = "title.ratings.tsv"

SETTINGS = configs.default.config_dict.get("settings")
IS_STREAMING = SETTINGS.get("use_streaming")


def download_imdb_dataset(url, output_path):
    gz_file_path = output_path + ".gz"
    urlretrieve(url, gz_file_path)

    with gzip.open(gz_file_path, "rb") as f_in:
        with open(output_path, "wb") as f_out:
            copyfileobj(f_in, f_out)

    if path.exists(gz_file_path):
        remove(gz_file_path)


# replace this with polars overwrite
def remove_old_save_new_file(dataframe_to_write, file_path):
    if path.exists(file_path):
        remove(file_path)

    dataframe_type = type(dataframe_to_write)
    if dataframe_type == pl.DataFrame:
        dataframe_to_write.write_parquet(file_path)
    else:
        raise Exception(
            "Unexpected type found when trying to save DataFrame to Parquet file."
        )


def split_columns_into_files(tmpdir, lf):
    df = lf.collect(streaming=IS_STREAMING)
    print(df.head())
    column_list = df.columns
    column_list.remove("tconst")

    for column in column_list:
        temp_df = df.select(["tconst", column])
        temp_df.write_parquet(os.path.join(tmpdir, column))


def clean_title_data(file_path, schema):

    lf = (
        pl.scan_csv(
            title_file,
            separator="\t",
            null_values=r"\N",
            quote_char=None,
            schema=schema,
        )
        # where startyear is None, replace it with last seen value
        .with_columns(pl.col("startYear").forward_fill()).filter(
            pl.col("genres").is_not_null()
        )
    )

    blocked_titletypes_arr = SETTINGS.get("blocked_titletypes")
    if blocked_titletypes_arr:
        # reverse the is_in using tilde operator
        lf = lf.filter(~pl.col("titleType").is_in(blocked_titletypes_arr))

    if SETTINGS.get("is_remove_adult") == True:
        lf = lf.filter(~pl.col("genres").str.split(",").list.contains("Adult"))

    return lf.drop("originalTitle", "endYear", "isAdult")
    # remove_old_save_new_file(
    #     dataframe_to_write=lf.drop("originalTitle", "endYear", "isAdult").collect(
    #         streaming=True
    #     ),
    #     file_path=file_path,
    # )


def join_title_ratings(title_lf, ratings_path, ratings_schema):
    # title_lf = pl.scan_parquet(title_path)

    ratings_lf = pl.scan_csv(
        ratings_file,
        separator="\t",
        null_values=r"\N",
        schema=ratings_schema,
    )

    title_lf = title_lf.join(ratings_lf, how="inner", on="tconst")

    if path.exists(ratings_path):
        remove(ratings_path)

    return title_lf


def create_genres_file_from_title_file(title_file_path, genres_file_path):
    lf = pl.scan_parquet(title_file_path).explode("genres").select(["tconst", "genres"])

    blocked_genres_arr = SETTINGS.get("blocked_genres")
    if blocked_genres_arr and len(blocked_genres_arr) > 0:
        # reverse the is_in using tilde operator
        lf = lf.filter(~pl.col("genres").is_in(blocked_genres_arr))
    remove_old_save_new_file(
        dataframe_to_write=lf.collect(streaming=True), file_path=genres_file_path
    )


def drop_genres_from_title(title_file_path):
    lf = pl.scan_parquet(title_file_path).drop("genres").collect(streaming=True)
    remove_old_save_new_file(dataframe_to_write=lf, file_path=title_file_path)


def change_str_to_int(file_path, column_name):

    df = pl.read_parquet(file_path)

    unique_values = df[column_name].explode().unique().to_list()
    unique_values.sort()
    value_dict = {}

    for counter, value in enumerate(unique_values):
        value_dict[value] = counter

    print(df.head())
    df = df.with_columns(
        pl.col(column_name)
        .str.split(",")
        .cast(pl.String)
        .replace_strict(value_dict, return_dtype=pl.UInt8)
    )

    return df
