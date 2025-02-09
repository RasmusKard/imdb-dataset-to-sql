import gzip
from os import path, remove
from shutil import copyfileobj
from urllib.request import urlretrieve
import polars as pl
import configs.default
import os.path
import uuid


title_file = "title.basics.tsv"
ratings_file = "title.ratings.tsv"

SETTINGS: dict | None = configs.default.config_dict.get("settings")
if not SETTINGS:
    raise Exception("Settings not found")
IS_STREAMING = SETTINGS.get("use_streaming") or False


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
    # if path.exists(file_path):
    #     remove(file_path)

    dataframe_type = type(dataframe_to_write)
    if dataframe_type == pl.DataFrame:
        dataframe_to_write.write_parquet(file_path)
    elif dataframe_type == pl.LazyFrame:
        dataframe_to_write.sink_parquet(file_path)
    else:
        raise Exception(
            "Unexpected type found when trying to save DataFrame to Parquet file."
        )


# def split_columns_into_files(tmpdir, lf):
#     df = lf.collect(streaming=IS_STREAMING)

#     column_list = df.columns
#     column_list.remove("tconst")

#     for column in column_list:
#         temp_df = df.select(["tconst", column])
#         temp_df.write_parquet(os.path.join(tmpdir, column))


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

    blocked_titletypes_set = SETTINGS.get("blocked_titletypes")
    if blocked_titletypes_set:
        # reverse the is_in using tilde operator
        lf = lf.filter(~pl.col("titleType").is_in(blocked_titletypes_set))

    # add adult to blocked genres based on `settings` bool and remove rows based on `isAdult`
    blocked_genres_list = SETTINGS.get("blocked_genres")
    if SETTINGS.get("is_remove_adult") == True:
        if blocked_genres_list:
            blocked_genres_list.append("Adult")
        else:
            blocked_genres_list = {"Adult"}
        lf = lf.filter(pl.col("isAdult") == 0)

    # remove blocked genres
    if blocked_genres_list:

        lf = lf.filter(
            pl.col("genres")
            .str.split(",")
            .list.set_intersection(blocked_genres_list)
            .list.len()
            .eq(0)
        )

    col_drop_arr = SETTINGS.get("columns_to_drop") or []
    return lf.drop(col_drop_arr)


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


def create_genres_file_from_title_file(
    main_file_path, genres_file_path, tmpdir, column_name
):

    lf = (
        pl.scan_parquet(main_file_path)
        .with_columns(pl.col(column_name).str.split(","))
        .explode(column_name)
        .select(["tconst", column_name])
    )

    genres_file_path = os.path.join(tmpdir, genres_file_path)

    lf.sink_parquet(genres_file_path)


def drop_genres_from_title(main_file_path):

    lf = pl.scan_parquet(main_file_path).drop("genres").collect(streaming=IS_STREAMING)
    remove_old_save_new_file(dataframe_to_write=lf, file_path=main_file_path)


def change_str_to_int(tmpdir, column_name, file_path):

    file_path = os.path.join(tmpdir, file_path)
    df = (pl.scan_parquet(file_path).with_columns(pl.col(column_name))).collect(
        streaming=IS_STREAMING
    )

    unique_values = df[column_name].unique().to_list()
    unique_values.sort()
    value_dict = {}

    for counter, value in enumerate(unique_values):
        value_dict[value] = counter

    df = df.with_columns(
        pl.col(column_name)
        .cast(pl.String)
        .replace_strict(value_dict, return_dtype=pl.UInt8)
    )

    tempfile_name = str(uuid.uuid4())
    tempfile_path = os.path.join(tmpdir, tempfile_name)

    df.write_parquet(tempfile_path)
    os.replace(tempfile_path, file_path)

    return value_dict
