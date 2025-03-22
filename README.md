# imdb-dataset-to-mysql

## Main opinionated choices:

1. Remove rows where `genres` is `NULL`
2. If `startYear` is `NULL` then use the last seen value
3. Keeping `tconst` column in all tables as an identifier is mandatory.

## Overview of code

1. Download gzip files for `title` and `ratings` from IMDb
1. Initialize `settings` and SQL engine
1. Clean `title` data
   - Remove blocked `titleType`s and `genres` outlined in `settings`
   - Remove `isAdult` if set to `True` in `settings`
   - Drop columns outlined in `settings`
   - If `startYear` is `NULL` then use the last seen value
   - Remove rows where `genres` is `NULL`
1. Join `ratings` file with cleaned `title` file
1. Split `genres` column from comma-separated string to list and explode it into separate rows (if enabled in `settings`). This also converts the genres value to `int` and creates a ref-table with the corresponding string values.
1. Convert `titleType`s to `int` and create ref-table (if enabled in `settings`)
1. Parse table info outlined in `settings` and use the cleaned data to create the tables based on it

## Settings parsers error catching

On init or update

1. `values` in `settings` is empty
1. Duplicates in `values` dict on either key or values (key=imdb_data_col_name and value=sql_col_name)
1. `values` dict has key (imdb_data_col_name) that is not present in the dataset
1. `dtypes` dict has key not matching any value (sql_col_name) in `values`

Only on update

1. Target database has tables matching the `values` dict .values() (sql_col_name)
1. The target tables and source tables have an exact match of **COL_NAMES**
1. Target and source tables have an exact match of **DTYPES** and **DTYPE.LENGTH**(if the dtype has a length attr)
