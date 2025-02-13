import sqlalchemy.types as dtype

##### DO NOT EDIT THIS FILE #####
# (The "default" file is reserved for special behavior, if you'd like to use different settings then create a new config file)

# `imdb_dataset_value_column` ALLOW LIST
# "tconst",
# "titleType",
# "primaryTitle",
# "originalTitle",
# "isAdult",
# "startYear",
# "endYear",
# "runtimeMinutes",
# "genres",
# "averageRating",
# "numVotes",


config_dict = {
    "tables": {
        "bruhtable": {
            "dtypes": {
                # dtype key should match a `column_name`
                # dtypes can be empty but then whatever default is set in `globals` is used for SQL dtypes
            },
            "values": {
                # `imdb_dataset_value_column` : `column_name`
                "tconst": "movieID",
                "titleType": "titleType",
                "runtimeMinutes": "runtimeMinutes",
                "numVotes": "numVotes",
            },
        },
        "title": {
            "dtypes": {"movieID": dtype.String(20)},
            "values": {
                "tconst": "movieID",
                "genres": "Genres",
            },
        },
    },
    "settings": {
        "blocked_genres": {"Horror", "Musical", "Short"},
        "blocked_titletypes": {"tvEpisode", "videoGame", "tvShort"},
        "columns_to_drop": {"isAdult", "endYear", "originalTitle"},
        "database": {
            # password is set in .env
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "database": "dataset_sql",
            "dialect": "mysql",  # Supports every dialect supported by SQLAlchemy
            # "driver": "mysqldb", # Uses SQLAlchemy recommended driver if left empty.
        },
        # split the comma separated genres string and convert it to an int with a lookup table
        "is_split_genres_into_reftable": False,
        # convert to int for lookup table creation
        "is_convert_title_type_str_to_int": False,
        "is_remove_adult": True,
        "is_streaming": True,
        "is_ignore_db_has_tables_warning": True,
        # Load datasets in batches to significantly reduce memory usage
        "is_batching": True,
    },
}
