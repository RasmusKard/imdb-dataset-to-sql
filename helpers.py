import tables
import pandas as pd
import json
from sqlalchemy import create_engine

with open("./config.json") as conf_file:
    CONFIG = json.load(conf_file)


sql_creds = CONFIG.get("database")
if not sql_creds:
    raise Exception("SQL credentials not found in config")

SQL_ENGINE = create_engine(
    f"mysql+mysqlconnector://{sql_creds["user"]}:{sql_creds["password"]}@{sql_creds["host"]}:{sql_creds["port"]}/{sql_creds["database"]}"
)


for table_name, table_dict in tables.tables_dict.items():
    values_to_grab = {}
    for imdb_column_name, new_column_name in table_dict["values"].items():
        # check if value is in allowedvalues
        if imdb_column_name not in tables.ALLOWED_VALUES:
            raise Exception(f"Invalid value `{imdb_column_name}` in `{table_name}`")
        else:
            values_to_grab[imdb_column_name] = new_column_name

    dtype_dict = {}
    for column_name, dtype in table_dict["dtypes"].items():
        # check if value is in allowedvalues
        if column_name not in values_to_grab.values():
            raise Exception(
                f"`{column_name}` in dtypes doesn't match any column in `values`"
            )
        else:
            dtype_dict[column_name] = dtype

    df = pd.read_parquet("temp_file_title", columns=list(values_to_grab.keys())).rename(
        columns=values_to_grab
    )

    df.to_sql(
        name=table_name,
        con=SQL_ENGINE,
        if_exists="replace",
        chunksize=10000,
        index=False,
        dtype=dtype_dict,
    )
