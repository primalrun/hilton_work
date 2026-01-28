import config
import common_functions as cfx
import polars as pl
import sys
import time
import datetime
import re
from pathlib import Path

# time start
time_start = time.perf_counter()
script_start_time = datetime.datetime.now()
print(f"Script started at {script_start_time.strftime('%Y-%m-%d %H:%M:%S')}")

# config variables
file_data_profile_in = config.file_data_profile_in
file_data_profile_out = config.file_data_profile_out
data_subject = config.data_subject
data_profile_column_active_dict = config.data_profile_column_active_dict
data_profile_calculation_column_sort = config.data_profile_calculation_column_sort

file_temp = r'c:\temp\1.xlsx'
file_csv_temp = r'c:\temp\1.csv'

print(f"processing {file_data_profile_in}")

# convert xlsx file to csv so code change not required below
file_path = Path(file_data_profile_in)
file_ext = file_path.suffix
if file_ext == '.xlsx':
    cfx.convert_xlsx_to_csv(file_data_profile_in, file_csv_temp)
    file_data_profile_in = file_csv_temp

# read file into dataframe
in_lf = pl.scan_csv(file_data_profile_in, encoding = 'utf8')
columns = [col for col in in_lf.collect_schema().names()]

if 'DAYS LEFT' in columns:
    new_schema = {
        'DAYS LEFT': pl.Utf8
    }
    in_lf = pl.scan_csv(file_data_profile_in, encoding = 'utf8', schema_overrides = new_schema)

dtypes = [str(dtype) for dtype in in_lf.collect_schema().dtypes()]
columns = [col for col in in_lf.collect_schema().names()]
string_cols = [k for k, v in in_lf.collect_schema().items() if v == pl.String or v == pl.Utf8]

# data profiling calculations
data_profiling_df_list = []
profile_calculation_columns = []

if data_profile_column_active_dict['row_count'] == 1:
    print('processing row_count')
    row_count_df = in_lf.select(pl.all().len()).collect(engine='streaming')
    data_profiling_df_list.append(row_count_df)
    profile_calculation_columns.append('row_count')

if data_profile_column_active_dict['character_blank_count'] == 1:
    print('processing character_blank_count')
    expr = [
        (pl.col(col).str.len_chars() == 0).sum().alias(col) for col in string_cols
    ]
    character_blank_count_df = in_lf.select(expr).collect(engine='streaming')
    data_profiling_df_list.append(character_blank_count_df)
    profile_calculation_columns.append('character_blank_count')

if data_profile_column_active_dict['null_count'] == 1:
    print('processing null_count')
    null_count_df = in_lf.select(pl.all().is_null().sum()).collect(engine='streaming')
    data_profiling_df_list.append(null_count_df)
    profile_calculation_columns.append('null_count')

if data_profile_column_active_dict['min_length_of_characters'] == 1:
    print('processing min_length_of_characters')
    min_length_of_characters_df = in_lf.select(
        [
            pl.col(col).str.len_chars().min().alias(col)
            for col in string_cols
        ]
    ).collect(engine='streaming')
    data_profiling_df_list.append(min_length_of_characters_df)
    profile_calculation_columns.append('min_length_of_characters')

if data_profile_column_active_dict['value_with_min_length_of_characters'] == 1:
    print('processing value_with_min_length_of_characters')
    char_length_min_df = in_lf.select(
        pl.col(col).str.len_chars().min().alias(col) for col in string_cols
    ).collect(engine='streaming')

    min_length_dict = {col: char_length_min_df[col][0] for col in char_length_min_df.collect_schema().names() if
                       char_length_min_df[col][0] is not None}

    shortest_string_dict = {}
    for col in min_length_dict:
        shortest_string_df = in_lf.filter(
            pl.col(col).str.len_chars() == min_length_dict[col]
        ).select(col).first().collect(engine='streaming')

        if not shortest_string_df.is_empty():
            shortest_string_dict[col] = shortest_string_df[col][0]

    min_string_value_df = pl.from_dict(shortest_string_dict)
    data_profiling_df_list.append(min_string_value_df)
    profile_calculation_columns.append('value_with_min_length_of_characters')

if data_profile_column_active_dict['min_value'] == 1:
    print('processing min_value')
    min_value_df = in_lf.select(pl.all().min()).collect(engine='streaming')
    data_profiling_df_list.append(min_value_df)
    profile_calculation_columns.append('min_value')

if data_profile_column_active_dict['max_length_of_characters'] == 1:
    print('processing max_length_of_characters')
    max_length_of_characters_df = in_lf.select(
        [
            pl.col(col).str.len_chars().max().alias(col)
            for col in string_cols
        ]
    ).collect(engine='streaming')
    data_profiling_df_list.append(max_length_of_characters_df)
    profile_calculation_columns.append('max_length_of_characters')

if data_profile_column_active_dict['value_with_max_length_of_characters'] == 1:
    print('processing value_with_max_length_of_characters')
    char_length_max_df = in_lf.select(
        pl.col(col).str.len_chars().max().alias(col) for col in string_cols
    ).collect(engine='streaming')

    max_length_dict = {col: char_length_max_df[col][0] for col in char_length_max_df.collect_schema().names() if
                       char_length_max_df[col][0] is not None}

    longest_string_dict = {}
    for col in max_length_dict:
        longest_string_df = in_lf.filter(
            pl.col(col).str.len_chars() == max_length_dict[col]
        ).select(col).first().collect(engine='streaming')

        if not longest_string_df.is_empty():
            longest_string_dict[col] = longest_string_df[col][0]

    max_string_value_df = pl.from_dict(longest_string_dict)
    data_profiling_df_list.append(max_string_value_df)
    profile_calculation_columns.append('value_with_max_length_of_characters')

if data_profile_column_active_dict['max_value'] == 1:
    print('processing max_value')
    max_value_df = in_lf.select(pl.all().max()).collect(engine='streaming')
    data_profiling_df_list.append(max_value_df)
    profile_calculation_columns.append('max_value')


if data_profile_column_active_dict['max_octet_length'] == 1:
    print('processing max_octet_length')
    max_octet_length_df = in_lf.select(
        [
            pl.col(col).str.len_bytes().max().alias(col)
            for col in string_cols
        ]
    ).collect(engine='streaming')
    data_profiling_df_list.append(max_octet_length_df)
    profile_calculation_columns.append('max_octet_length')

if data_profile_column_active_dict['value_with_max_octet_length'] == 1:
    print('processing value_with_max_octet_length')
    octet_length_max_df = in_lf.select(
        pl.col(col).str.len_bytes().max().alias(col) for col in string_cols
    ).collect(engine='streaming')

    max_octet_length_dict = {col: octet_length_max_df[col][0] for col in octet_length_max_df.collect_schema().names() if
                       octet_length_max_df[col][0] is not None}

    longest_octet_string_dict = {}
    for col in max_octet_length_dict:
        longest_octet_string_df = in_lf.filter(
            pl.col(col).str.len_bytes() == max_octet_length_dict[col]
        ).select(col).first().collect(engine='streaming')

        if not longest_octet_string_df.is_empty():
            longest_octet_string_dict[col] = longest_octet_string_df[col][0]

    max_octet_string_value_df = pl.from_dict(longest_octet_string_dict)
    data_profiling_df_list.append(max_octet_string_value_df)
    profile_calculation_columns.append('value_with_max_octet_length')

if data_profile_column_active_dict['avg_length_of_characters'] == 1:
    print('processing avg_length_of_characters')
    avg_length_of_characters_df = in_lf.select(
        [
            pl.col(col).str.len_chars().mean().ceil().cast(pl.Int32).alias(col)
            for col in string_cols
        ]
    ).collect(engine='streaming')
    data_profiling_df_list.append(avg_length_of_characters_df)
    profile_calculation_columns.append('avg_length_of_characters')

if data_profile_column_active_dict['value_with_avg_length_of_characters'] == 1:
    print('processing value_with_avg_length_of_characters')
    char_length_avg_df = in_lf.select(
        pl.col(col).str.len_chars().mean().ceil().cast(pl.Int32).alias(col) for col in string_cols
    ).collect(engine='streaming')

    avg_length_dict = {col: char_length_avg_df[col][0] for col in char_length_avg_df.collect_schema().names() if
                       char_length_avg_df[col][0] is not None}

    filter_count_df = in_lf.select(
        (pl.col(col).str.len_chars() == avg_length_dict[col]).sum().alias(col) for col in avg_length_dict
    ).collect(engine='streaming')

    # get only columns that have a column at average length
    filter_count_dict = {col: filter_count_df[col][0] for col in filter_count_df.collect_schema().names() if
                       filter_count_df[col][0] > 0}

    avg_length_string_dict = {}
    for col in filter_count_dict:
        avg_length_string_df = in_lf.filter(
            pl.col(col).str.len_chars() == avg_length_dict[col]
        ).select(col).first().collect(engine='streaming')

        if not avg_length_string_df.is_empty():
            avg_length_string_dict[col] = avg_length_string_df[col][0]

    avg_length_character_value_df = pl.from_dict(avg_length_string_dict)
    data_profiling_df_list.append(avg_length_character_value_df)
    profile_calculation_columns.append('value_with_avg_length_of_characters')

if data_profile_column_active_dict['count_of_distinct_values'] == 1:
    print('processing count_of_distinct_values')
    count_of_distinct_values_df = in_lf.select(pl.all().n_unique()).collect(engine='streaming')
    data_profiling_df_list.append(count_of_distinct_values_df)
    profile_calculation_columns.append('count_of_distinct_values')

if data_profile_column_active_dict['count_of_values_with_leading_whitespace'] == 1:
    print('processing count_of_values_with_leading_whitespace')
    expr = []
    for col in string_cols:
        has_leading_space = pl.col(col).str.contains(r"^\s", literal=False).sum().alias(col)
        expr.append(has_leading_space)

    count_of_values_with_leading_whitespace_df = in_lf.select(expr).collect(engine='streaming')
    data_profiling_df_list.append(count_of_values_with_leading_whitespace_df)
    profile_calculation_columns.append('count_of_values_with_leading_whitespace')

if data_profile_column_active_dict['count_of_values_with_ending_whitespace'] == 1:
    print('processing count_of_values_with_ending_whitespace')
    expr = []
    for col in string_cols:
        has_trailing_space = pl.col(col).str.contains(r"\s$", literal=False).sum().alias(col)
        expr.append(has_trailing_space)

    count_of_values_with_ending_whitespace_df = in_lf.select(expr).collect(engine='streaming')
    data_profiling_df_list.append(count_of_values_with_ending_whitespace_df)
    profile_calculation_columns.append('count_of_values_with_ending_whitespace')

if data_profile_column_active_dict['count_of_values_with_beginning_and_ending_whitespace'] == 1:
    print('processing count_of_values_with_beginning_and_ending_whitespace')
    expr = []
    for col in string_cols:
        has_leading_space = pl.col(col).str.contains(r"^\s", literal=False)
        has_trailing_space = pl.col(col).str.contains(r"\s$", literal=False)
        has_trailing_and_leading_space = (has_leading_space & has_trailing_space).sum().alias(col)
        expr.append(has_trailing_and_leading_space)

    count_of_values_with_beginning_and_ending_whitespace_df = in_lf.select(expr).collect(engine='streaming')
    data_profiling_df_list.append(count_of_values_with_beginning_and_ending_whitespace_df)
    profile_calculation_columns.append('count_of_values_with_beginning_and_ending_whitespace')


print('processing remaining steps')
# convert dataframe values to string so the results can stack without dtype errors
profile_calculation_dfs = [cfx.pl_convert_df_to_string(df) for df in data_profiling_df_list]

# stack vertically each data profile calculation df
profile_calculation_df = pl.concat(profile_calculation_dfs, how='diagonal')

# reorder column order back to original order, the diagonal concat can change the column order
profile_calculation_df = profile_calculation_df.select(columns)

# transpose dataframe, provide column names for calculations
profile_calculation_df = profile_calculation_df.transpose()
profile_calculation_df.columns = profile_calculation_columns

# add column for field
field_series = pl.Series('field', columns)
profile_calculation_df.insert_column(0, field_series)

# add column for data type
profile_calculation_df = profile_calculation_df.with_columns(pl.Series('data_type', dtypes))

# add column for is_completely_null
if 'row_count' in profile_calculation_df.columns and 'null_count' in profile_calculation_df.columns:
    profile_calculation_df = profile_calculation_df.with_columns(
        pl.when(pl.col('row_count') == pl.col('null_count'))
                .then(pl.lit('yes'))
                .otherwise(pl.lit('no'))
                .alias('is_completely_null')
        )

# change column order to reporting preference
col_current = [col for col in profile_calculation_df.columns]
col_sort_if_exist = [col for col in data_profile_calculation_column_sort if col in col_current]
profile_calculation_df = profile_calculation_df.select(col_sort_if_exist)

# prepare output file
cfx.delete_file_if_exists(file_data_profile_out)

# write results to output file
cfx.write_pl_df_to_excel(profile_calculation_df, file_data_profile_out, data_subject)
time.sleep(4)
cfx.load_excel_file_and_clean(file_data_profile_out)

# time end
time_end = time.perf_counter()
elapsed_time = time_end - time_start

print('success')
print(f'total execution time: {elapsed_time:.4f} seconds')
print(f'total execution time: {float(elapsed_time/60):.4f} minutes')
