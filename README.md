# imdb-dataset-to-mysql

Main opinionated choices:

1. Remove rows where `genres` is `NULL`
2. If `startYear` is `NULL` then use the last seen value
3. Keeping `tconst` row in all tables as an identifier is mandatory.

## OVERVIEW OF CURRENT CODE

1. Download gzip files for `title` and `ratings` from IMDb
1. Initialize `settings` and SQL engine
1. Clean `title` data
   - Remove blocked `titleType`'s outlined in `settings`
   - Remove `isAdult` if set to `True` in `settings`
   -

option a

separate all columns into different parquet files
join them when needed

option b
keep everything in one file and when reading only select needed columns
if user has enabled rating column split then generate a second file and join it if needed
(add a warning for it being a bad idea due to duplication of tconst)
