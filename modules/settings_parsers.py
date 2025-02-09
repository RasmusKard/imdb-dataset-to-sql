from sqlalchemy import MetaData, Table


def get_settings_tables_validity(tables, ALLOWED_COLUMNS, is_updater):
    tables_info_dict = {}

    for table_name, table_dict in tables.items():
        values_dict = table_dict.get("values")
        if not values_dict:
            raise Exception("`values` empty in `tables`")

        new_col_names = values_dict.values()
        cols_needed = values_dict.keys()

        # check col_names and col_values for duplicates
        if len(new_col_names) != len(set(new_col_names)) or len(cols_needed) != len(
            set(cols_needed)
        ):
            raise ValueError("Duplicates found in `values` dict.")

        dtype_dict = table_dict.get("dtypes", {})
        # check that dtype value matches a column_name
        if dtype_dict:
            dtype_col_name_set = set(dtype_dict.keys())
            if not dtype_col_name_set.issubset(set(new_col_names)):
                raise KeyError("`dtypes` references a column not found in `values`")

        for new_col_name, value_col_name in zip(new_col_names, cols_needed):
            # check that column values match allowed values
            if value_col_name not in ALLOWED_COLUMNS:
                raise ValueError(f"Invalid value `{value_col_name}` in `{table_name}`")

            # if dtype not declared by user then use default
            if new_col_name not in dtype_dict:
                dtype_dict[new_col_name] = ALLOWED_COLUMNS[value_col_name]

        tables_info_dict[table_name] = {
            "dtype_dict": dtype_dict,
            "values_dict": values_dict,
        }
    return tables_info_dict


def get_is_settings_match_db_shape(tables_info, sql_engine):
    metadata = MetaData()
    metadata.reflect(bind=sql_engine)

    for tbl_name, tbl_info in tables_info.items():
        table = Table(tbl_name, metadata)
        target_columns = table.c

        dtype_dict = tbl_info["dtype_dict"]

        source_col_set = set(dtype_dict.keys())
        target_col_set = set(target_columns.keys())
        if not target_col_set == source_col_set:
            raise ValueError(
                f"Target SQL table `{tbl_name}` column names don't match `settings`. target: `{target_col_set}` source: `{source_col_set}`"
            )

        for col in target_columns:
            target_dtype = col.type.as_generic()
            source_dtype = dtype_dict[col.name].as_generic()

            if not target_dtype.__class__ == source_dtype.__class__:
                raise TypeError(
                    f"Target SQL table `{tbl_name}` column types don't match `settings`. target: `{target_dtype}` source: `{source_dtype}`"
                )
