# imdb-dataset-to-mysql

Main opinionated choices:

1. Remove rows where `genres` is `NULL`
2. If `startYear` is `NULL` then use the last seen value
3. Keeping `tconst` row in all tables as an identifier is mandatory.

## OVERVIEW OF CURRENT CODE

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
