import polars as pl
import pandas as pd
from sqlalchemy.types import String
import globals
import os.path
import uuid
import warnings


def create_reference_table(sql_engine, value_dict, column_name):
    table_name = f"{column_name}_ref"
    id_col = f"{column_name}_id"
    str_col = f"{column_name}_str"

    ref_data = {id_col: list(value_dict.values()), str_col: list(value_dict.keys())}
    df = pd.DataFrame(ref_data)

    df.to_sql(
        table_name,
        con=sql_engine,
        if_exists="replace",
        index=False,
    )


def join_parquet_files_to_df(column_list, tmpdir):
    path_arr = [
        os.path.join(tmpdir, column) for column in column_list if column != "tconst"
    ]

    lf_arr = []
    for path in path_arr:
        lf_arr.append(pl.scan_parquet(path))

    # badbadbad?
    lf = pl.concat(lf_arr, how="align").collect(streaming=True)
    print("joining", lf.dtypes)
    file_name = str(uuid.uuid4())
    file_path = os.path.join(tmpdir, file_name)
    lf.write_parquet(file_path)

    return file_path


def table_to_sql(
    table_dict, table_name, sql_engine, main_file_path, genres_file_path, settings
):
    values_dict = table_dict.get("values")
    if not values_dict:
        raise Exception("`values` empty in `tables`")

    new_col_names = values_dict.values()
    cols_needed = values_dict.keys()

    # check col_names and col_values for duplicates
    if len(new_col_names) != len(set(new_col_names)) or len(cols_needed) != len(
        set(cols_needed)
    ):
        raise ValueError("Duplicates found in `values` dict.")

    dtype_dict = table_dict.get("dtypes")
    # check that dtype value matches a column_name
    if dtype_dict != None:
        dtype_col_name_set = set(dtype_dict.keys())
        if not dtype_col_name_set.issubset(set(new_col_names)):
            raise KeyError("`dtypes` references a column not found in `values`")
    # if dtype dict is None init it for use below
    else:
        dtype_dict = {}

    ALLOWED_COLUMNS = globals.IMDB_DATA_ALLOWED_COLUMNS
    for new_col_name, value_col_name in zip(new_col_names, cols_needed):
        # check that column values match allowed values
        if value_col_name not in ALLOWED_COLUMNS:
            raise ValueError(f"Invalid value `{value_col_name}` in `{table_name}`")

        # if dtype not declared by user then use default
        if new_col_name not in dtype_dict:
            dtype_dict[new_col_name] = ALLOWED_COLUMNS[value_col_name]

    # handle using the split genres file if needed
    if settings.get("is_split_genres_into_reftable") and "genres" in cols_needed:
        # check if its just tconst and genres
        # if not then warn and merge the main and genres files
        if {"genres", "tconst"} == set(cols_needed):
            df = pd.read_parquet(genres_file_path, dtype_backend="pyarrow").rename(
                columns=values_dict
            )
        else:
            df1 = pd.read_parquet(genres_file_path, dtype_backend="pyarrow")

            main_file_cols = list(cols_needed)
            main_file_cols.remove("genres")
            df2 = pd.read_parquet(
                main_file_path,
                columns=main_file_cols,
                dtype_backend="pyarrow",
            )

            df = pd.merge(df1, df2, on="tconst", how="left").rename(values_dict)

            warnings.warn(
                "WARNING: It's not recommended to store values other than `tconst` in the same table as a split `genres` column. "
                "It's better to use a Foreign Key constraint on tconst of the split `genres` table and the table with your other columns."
            )
    else:
        df = pd.read_parquet(
            main_file_path, columns=list(cols_needed), dtype_backend="pyarrow"
        ).rename(columns=values_dict)

    df.to_sql(
        name=table_name,
        con=sql_engine,
        if_exists="replace",
        chunksize=10000,
        index=False,
        dtype=dtype_dict,
    )
