import pandas as pd


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


def add_foreign_key_sql(foreign_key_creation_sql, sql_engine):
    with sql_engine.begin() as connection:
        connection.execute(foreign_key_creation_sql)
