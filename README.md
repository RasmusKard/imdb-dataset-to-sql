# imdb-dataset-to-mysql

Main opinionated choices:

1. Remove rows where `genres` is NULL
2. If startYear is NULL then use the last seen value
3. Keeping `tconst` row in all tables as an identifier is mandatory.
