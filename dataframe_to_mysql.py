import pandas as pd
import config
import polars as pl

genres_dict = {
    "Comedy": 0,
    "Musical": 1,
    "Romance": 2,
    "Western": 3,
    "Drama": 4,
    "Music": 5,
    "Action": 6,
    "Adventure": 7,
    "Crime": 8,
    "Mystery": 9,
    "War": 10,
    "Biography": 11,
    "History": 12,
    "Documentary": 13,
    "Sport": 14,
    "Horror": 15,
    "Thriller": 16,
    "Family": 17,
    "Sci-Fi": 18,
    "Fantasy": 19,
    "Film-Noir": 20,
    "Animation": 21,
    "News": 22,
    "Game-Show": 23,
    "Talk-Show": 24,
    "Reality-TV": 25,
    "Short": 26,
}

title_type_dict = {
    "movie": 0,
    "tvSeries": 1,
    "tvMovie": 2,
    "tvSpecial": 3,
    "tvMiniSeries": 4,
}


def change_str_to_int_and_create_ref_table(df_file_path, sql_engine, column_name):
    df = pd.read_parquet(df_file_path)
    unique_values = df[column_name].unique()
    value_dict = {}

    for counter, value in enumerate(unique_values):
        value_dict[value] = counter

    df.replace(to_replace=value_dict, inplace=True)
    print(df)


change_str_to_int_and_create_ref_table(
    df_file_path="temp_file_title",
    sql_engine=config.MYSQL_ENGINE,
    column_name="titleType",
)


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
