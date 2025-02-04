import pandas as pd
from sqlalchemy.types import String


def parquet_to_sql(
    parquet_file_path,
    table_name,
    sql_engine,
):

    df = pd.read_parquet(parquet_file_path)
    df.to_sql(
        name=table_name,
        con=sql_engine,
        if_exists="replace",
        chunksize=10000,
        index=False,
        dtype={"tconst": String(80)},
    )


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
