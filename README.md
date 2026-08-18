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

## Choosing a config

`configs/default.py` is used unless `CONFIG` names another module in `configs/`:

```sh
CONFIG=my_project uv run main.py
```

## Updater mode against a schema you did not create

`IS_UPDATER=True` loads into tables that already exist instead of replacing them, so it can
target a schema owned by something else — a migration tool, for instance. Three things follow
from that:

- **The target may have columns this tool does not write.** A column with a default or a
  generated column is left alone; `settings` only has to name a subset of the target's
  columns. `COPY` and `LOAD DATA` name the columns they load rather than relying on
  positional order.
- **Foreign keys between the loaded tables are preserved.** Reference tables are emptied and
  refilled rather than dropped, which also keeps their indexes and grants. Postgres clears
  with `TRUNCATE ... CASCADE`; MySQL drops `FOREIGN_KEY_CHECKS` for the duration, since it
  has no `CASCADE` and refuses `TRUNCATE` on a referenced table outright.
- **Reference tables are rebuilt on every run.** Genre and title type ids are assigned
  positionally over the sorted distinct values in the dump being loaded, so a reference table
  carried over from an earlier dump silently points at the wrong strings as soon as IMDb's set
  of genres or title types changes.

To reach a non-default schema, set it on the connection: tables are addressed by bare name,
both in raw SQL and when reflecting the target's shape.

```
SQL_URL=postgresql://user:pw@host:5432/db?options=-csearch_path%3Dmyschema
```

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
