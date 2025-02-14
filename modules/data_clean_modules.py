from os import path, remove
import polars as pl
import os.path
from modules.helpers import join_path_with_random_uuid
import os
from modules.const import PL_SCHEMA_OVERRIDE


# replace this with polars overwrite
def save_dataframe(dataframe_to_write, file_path, tmpdir=None):

    dataframe_type = type(dataframe_to_write)
    if dataframe_type == pl.DataFrame:
        if path.exists(file_path):
            remove(file_path)

        dataframe_to_write.write_csv(file_path)
    elif dataframe_type == pl.LazyFrame:
        tmp_path = join_path_with_random_uuid(tmpdir)
        dataframe_to_write.sink_csv(tmp_path)

        os.replace(tmp_path, file_path)
    else:
        raise Exception(
            "Unexpected type found when trying to save DataFrame to Parquet file."
        )


def clean_title_and_join_with_ratings(
    title_path,
    ratings_path,
    title_schema,
    ratings_schema,
    is_batched,
    tmpdir,
    settings,
    batch_count=None,
):

    if not is_batched:
        lf = pl.scan_csv(
            title_path,
            separator="\t",
            null_values=r"\N",
            quote_char=None,
            schema=title_schema,
        )

        lf = apply_title_cleaners(lf, settings=settings)

        lf = join_title_ratings(
            title_lf=lf,
            ratings_path=ratings_path,
            ratings_schema=ratings_schema,
        )

        save_dataframe(file_path=title_path, tmpdir=tmpdir, dataframe_to_write=lf)

        # forwardfill needs to be done separately due to RAM concerns
        # for some reason collecting is also less RAM intensive here than sinking?
        lf = (
            pl.scan_parquet(title_path)
            .with_columns(pl.col("startYear").fill_null(strategy="forward"))
            .collect(new_streaming=settings.get("is_streaming", False))
        )
        save_dataframe(file_path=title_path, tmpdir=tmpdir, dataframe_to_write=lf)

    else:
        batched_clean_title_data_and_join_with_ratings(
            file_path=title_path,
            schema=title_schema,
            batch_count=batch_count,
            tmpdir=tmpdir,
            ratings_path=ratings_path,
            ratings_schema=ratings_schema,
            settings=settings,
        )


def batched_clean_title_data_and_join_with_ratings(
    file_path, schema, batch_count, tmpdir, ratings_path, ratings_schema, settings
):
    reader = pl.read_csv_batched(
        file_path,
        separator="\t",
        null_values=r"\N",
        quote_char=None,
        schema_overrides=schema,
    )
    batches = reader.next_batches(batch_count)
    tmp_path = join_path_with_random_uuid(tmpdir)

    with open(tmp_path, mode="a") as f:

        ratings_lf = pl.scan_csv(
            ratings_path,
            separator="\t",
            null_values=r"\N",
            schema=ratings_schema,
        )

        # handle not writing header constantly
        is_first_write = True
        while batches:
            for df in batches:
                lf = df.lazy()
                lf = apply_title_cleaners(lf, settings=settings, is_batching=True)
                lf = lf.join(ratings_lf, how="inner", on="tconst")
                lf.collect(new_streaming=True).write_csv(
                    f, include_header=is_first_write
                )

            batches = reader.next_batches(batch_count)

            if is_first_write:
                is_first_write = False

    os.replace(tmp_path, file_path)


def apply_title_cleaners(df, settings, is_batching=False):
    # drop rows where genres is null
    df = df.filter(pl.col("genres").is_not_null())

    # if it's not batching then forwardfill at this stage HEAVILY increases RAM usage
    if is_batching:
        # where startyear is None, replace it with last seen value
        df = df.with_columns(pl.col("startYear").fill_null(strategy="forward"))

    blocked_titletypes_set = settings.get("blocked_titletypes")
    if blocked_titletypes_set:
        # reverse the is_in using tilde operator
        df = df.filter(~pl.col("titleType").is_in(blocked_titletypes_set))

    # add adult to blocked genres based on `settings` bool and remove rows based on `isAdult`
    blocked_genres_set = settings.get("blocked_genres")
    if settings.get("is_remove_adult") == True:
        if blocked_genres_set:
            blocked_genres_set.add("Adult")
        else:
            blocked_genres_set = {"Adult"}
        df = df.filter(pl.col("isAdult") == 0)

    # remove blocked genres
    if blocked_genres_set:
        df = df.filter(
            pl.col("genres")
            .str.split(",")
            .list.set_intersection(list(blocked_genres_set))
            .list.len()
            .eq(0)
        )

    col_drop_arr = settings.get("columns_to_drop") or []
    df.drop(col_drop_arr)
    return df


def join_title_ratings(title_lf, ratings_path, ratings_schema):
    ratings_lf = pl.scan_csv(
        ratings_path,
        separator="\t",
        null_values=r"\N",
        schema=ratings_schema,
    )

    title_lf = title_lf.join(ratings_lf, how="inner", on="tconst")

    return title_lf


def create_genres_file_from_title_file(
    main_file_path, genres_file_path, tmpdir, column_name
):

    lf = (
        pl.scan_csv(main_file_path, schema_overrides=PL_SCHEMA_OVERRIDE)
        .with_columns(pl.col(column_name).str.split(","))
        .explode(column_name)
        .select(["tconst", column_name])
    )

    save_dataframe(dataframe_to_write=lf, file_path=genres_file_path, tmpdir=tmpdir)


def drop_genres_from_title(main_file_path, tmpdir):

    lf = pl.scan_csv(main_file_path, schema_overrides=PL_SCHEMA_OVERRIDE).drop("genres")

    save_dataframe(dataframe_to_write=lf, file_path=main_file_path, tmpdir=tmpdir)


def change_str_to_int(column_name, file_path, is_streaming):

    df = (
        pl.scan_csv(file_path, schema_overrides=PL_SCHEMA_OVERRIDE)
        .with_columns(pl.col(column_name))
        .collect(new_streaming=is_streaming)
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

    save_dataframe(dataframe_to_write=df, file_path=file_path)

    return value_dict
