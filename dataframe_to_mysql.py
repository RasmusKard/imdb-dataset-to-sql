import pandas as pd
from sqlalchemy import text
from config import (
    mysql_engine,
    title_file_path,
    genres_file_path,
    TITLE_TABLE_NAME,
    TITLE_TABLE_DROP_SQL,
    TITLE_TABLE_CREATION_SQL,
    GENRES_TABLE_NAME,
    GENRES_TABLE_DROP_SQL,
    GENRES_TABLE_CREATION_SQL,
)


def title_data_to_sql():
    with mysql_engine.begin() as connection:
        for sql in [TITLE_TABLE_DROP_SQL, TITLE_TABLE_CREATION_SQL]:
            connection.execute(sql)

    df = pd.read_parquet(title_file_path)
    df.to_sql(
        TITLE_TABLE_NAME,
        con=mysql_engine,
        if_exists="append",
        index=False,
        chunksize=10000,
    )


def genre_data_to_sql():
    with mysql_engine.begin() as connection:
        for sql in [GENRES_TABLE_DROP_SQL, GENRES_TABLE_CREATION_SQL]:
            connection.execute(sql)

    df = pd.read_parquet(genres_file_path)
    df.to_sql(
        GENRES_TABLE_NAME,
        con=mysql_engine,
        if_exists="append",
        index=False,
        chunksize=10000,
    )


title_data_to_sql()
genre_data_to_sql()
