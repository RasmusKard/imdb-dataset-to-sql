import pandas as pd
import warnings
from sqlalchemy import MetaData, create_engine, Table
from sqlalchemy.types import VARCHAR, String
from modules.helpers import join_path_with_random_uuid


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


def table_to_sql(
    table_info,
    table_name,
    sql_engine,
    main_file_path,
    genres_file_path,
    settings,
):
    dtype_dict = table_info["dtype_dict"]
    values_dict = table_info["values_dict"]
    cols_needed = values_dict.keys()

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

            df = pd.merge(df1, df2, on="tconst", how="left").rename(columns=values_dict)

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
        index=False,
        dtype=dtype_dict,
    )

    return {
        "row_count": len(df.index),
        "dtypes": list(dtype_dict.values()),
        "columns": list(dtype_dict.keys()),
    }
