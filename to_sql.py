import pandas as pd
from sqlalchemy import create_engine

mysql_engine = create_engine(
    "mysql+mysqlconnector://root:1234@localhost:3306/dataset_sql"
)

df = pd.read_parquet("title.final.parquet")

df.to_sql("content", con=mysql_engine, if_exists="append", index=False, chunksize=10000)
