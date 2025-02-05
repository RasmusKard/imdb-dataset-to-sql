import polars as pl
from sqlalchemy.types import String
import globals
import os.path


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

    lf = pl.concat(lf_arr, how="align")
    print(lf.collect().head())


def table_to_sql(tmpdir, table_dict, table_name, sql_engine, settings):
    values_dict = table_dict.get("values")
    if not values_dict:
        raise Exception("`values` empty in `tables`")

    dtype_dict = table_dict.get("dtypes")
    new_dtype_dict = {}
    if dtype_dict:
        for column_name, dtype in dtype_dict.items():
            if column_name not in values_dict.values():
                raise Exception(
                    f"`{column_name}` in dtypes doesn't match any column in `values`"
                )

            new_dtype_dict[column_name] = dtype

    values_to_grab = {}
    ALLOWED_COLUMNS = globals.IMDB_DATA_ALLOWED_COLUMNS
    for imdb_column_name, new_column_name in table_dict["values"].items():
        # check if value is in allowedvalues
        if imdb_column_name not in ALLOWED_COLUMNS.keys():
            raise Exception(f"Invalid value `{imdb_column_name}` in `{table_name}`")

        if new_column_name not in new_dtype_dict.keys():
            new_dtype_dict[new_column_name] = ALLOWED_COLUMNS[imdb_column_name]
        values_to_grab[imdb_column_name] = new_column_name

    join_parquet_files_to_df(column_list=values_to_grab.keys(), tmpdir=tmpdir)
    # path_arr = [os.path.join(tmpdir, column) for column in values_to_grab.keys()]
    # df = pl.read_parquet(path_arr).rename(columns=values_to_grab)

    # df.to_sql(
    #     name=table_name,
    #     con=sql_engine,
    #     if_exists="replace",
    #     chunksize=10000,
    #     index=False,
    #     dtype=new_dtype_dict,
    # )
