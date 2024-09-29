import pandas as pd
import config
import polars as pl
from sqlalchemy import text


def title_data_to_sql(
    title_table_creation_sql,
    title_table_drop_sql,
    title_file_path,
    title_table_name,
    sql_engine,
):
    with sql_engine.begin() as connection:
        for sql in [title_table_drop_sql, title_table_creation_sql]:
            connection.execute(sql)

    df = pd.read_parquet(title_file_path)
    df.to_sql(
        title_table_name,
        con=sql_engine,
        if_exists="append",
        index=False,
        chunksize=10000,
    )


def genre_data_to_sql(
    genres_table_drop_sql,
    genres_table_creation_sql,
    genres_file_path,
    genres_table_name,
    sql_engine,
):
    with sql_engine.begin() as connection:
        for sql in [genres_table_drop_sql, genres_table_creation_sql]:
            connection.execute(sql)

    df = pd.read_parquet(genres_file_path)
    df.to_sql(
        genres_table_name,
        con=sql_engine,
        if_exists="append",
        index=False,
        chunksize=10000,
    )


def create_reference_table(sql_engine, value_dict, column_name):
    table_name = f"{column_name}_ref"
    id_col = f"{column_name}_id"
    str_col = f"{column_name}_str"
    table_creation_sql = text(
        f"""
    CREATE TABLE `{table_name}` (
        `{id_col}` TINYINT UNSIGNED NOT NULL,
        `{str_col}` VARCHAR(15) NOT NULL
    );"""
    )
    with sql_engine.begin() as connection:
        connection.execute(table_creation_sql)

    ref_data = {id_col: list(value_dict.values()), str_col: list(value_dict.keys())}
    df = pd.DataFrame(ref_data)

    df.to_sql(
        table_name,
        con=sql_engine,
        if_exists="append",
        index=False,
    )
