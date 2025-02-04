import sqlalchemy.types as dtype


ALLOWED_VALUES = [
    "tconst",
    "titleType",
    "primaryTitle",
    "originalTitle",
    "isAdult",
    "startYear",
    "endYear",
    "runtimeMinutes",
    "genres",
    "averageRating",
    "numVotes",
]

# dtypes can be empty but then whatever default is given by pandas is used for sql dtypes

# first parse values and check if there is a corresponding entry in dtypes for it
# if not dont add anything into the to_sql() dtypes
tables_dict = {
    "title": {
        "dtypes": {},
        "values": {"movieID": "tconst", "typeOfContent": "titleType"},
    }
}


# for loop of values
# if dtypes.get(values.key)
# add dtype
# else
# dont
