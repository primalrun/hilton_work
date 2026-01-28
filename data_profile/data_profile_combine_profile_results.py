import config
import common_functions as cfx
import pandas as pd
from pathlib import Path
import os
import sys
import math

data_subject = 'Compliance'
file_temp = r'c:\temp\1.xlsx'
dir_data_profile_file_processed = config.dir_data_profile_file_processed
dir_data_profile_processed_data_subject_total = config.dir_data_profile_processed_data_subject_total
file_results_name = f"data profile results {data_subject}.xlsx"
file_results = os.path.join(dir_data_profile_processed_data_subject_total, file_results_name)


def get_file_count(value_list):
    return sum(value_list)


def get_data_type(value_list):
    if len(set(value_list)) <= 1:
        return value_list[0]
    else:
        if set(value_list) == {'Int64', 'Float64'}:
            return 'Float64'
        if set(value_list) == {'Int64', 'String'}:
            return 'String'
        else:
            print('multiple data types')
            return None


def get_row_count(value_list):
    return sum(value_list)


def get_null_count(value_list):
    return sum(value_list)


def get_is_completely_null(value_list):
    if all(x == 'yes' for x in value_list):
        return 'yes'
    else:
        return 'no'


def get_character_blank_count(value_list):
    non_empty_string = [s for s in value_list if s != '']
    if len(non_empty_string) > 0:
        result = [int(x) for x in non_empty_string]
        return sum(result)
    else:
        return None


def get_min_length_of_characters(value_list):
    no_null_list = [x for x in value_list if not (isinstance(x, float) and math.isnan(x))]
    no_null_list = [int(x) for x in no_null_list if x != '']
    if len(no_null_list) > 0:
        return min(no_null_list)
    else:
        return None


def get_min_value(value_list):
    no_null_list = [x for x in value_list if not (isinstance(x, float) and math.isnan(x))]
    no_null_list = [x for x in no_null_list if x != '']
    if len(no_null_list) > 0:
        return min(no_null_list)
    else:
        return None


def get_value_with_min_length_of_characters(value_list):
    string_list = [elem for elem in value_list if isinstance(elem, str)]
    no_null_list = [x for x in string_list if x != '']
    len_list = [len(x) for x in no_null_list]
    if len(len_list) > 0 and len(no_null_list) > 0:
        lowest_len = min(len_list)
        lowest_len_index = len_list.index(lowest_len)
        lowest_min_len_string = no_null_list[lowest_len_index]
        return lowest_min_len_string
    else:
        return None


def get_max_value(value_list):
    no_null_list = [x for x in value_list if not (isinstance(x, float) and math.isnan(x))]
    no_null_list = [x for x in no_null_list if x != '']
    if len(no_null_list) > 0:
        return max(no_null_list)
    else:
        return None



def get_max_length_of_characters(value_list):
    no_null_list = [x for x in value_list if not (isinstance(x, float) and math.isnan(x))]
    no_null_list = [int(x) for x in no_null_list if x != '']
    if len(no_null_list) > 0:
        return max(no_null_list)
    else:
        return None


def get_value_with_max_length_of_characters(value_list):
    string_list = [elem for elem in value_list if isinstance(elem, str)]
    no_null_list = [x for x in string_list if x != '']
    len_list = [len(x) for x in no_null_list]
    if len(len_list) > 0 and len(no_null_list) > 0:
        highest_len = max(len_list)
        highest_len_index = len_list.index(highest_len)
        highest_max_len_string = no_null_list[highest_len_index]
        return highest_max_len_string
    else:
        return None


def get_max_octet_length(value_list):
    no_null_list = [x for x in value_list if not (isinstance(x, float) and math.isnan(x))]
    no_null_list = [int(x) for x in no_null_list if x != '']
    if len(no_null_list) > 0:
        return max(no_null_list)
    else:
        return None



def get_value_with_max_octet_length(value_list):
    string_list = [elem for elem in value_list if isinstance(elem, str)]
    no_null_list = [x for x in string_list if x != '']
    octet_list = [len(x.encode('utf-8')) for x in no_null_list]
    if len(octet_list) > 0 and len(no_null_list) > 0:
        highest_octet_len = max(octet_list)
        highest_octet_index = octet_list.index(highest_octet_len)
        highest_octet_string = no_null_list[highest_octet_index]
        return highest_octet_string
    else:
        return None


def get_avg_length_of_characters(value_list):
    no_null_list = [x for x in value_list if not (isinstance(x, float) and math.isnan(x))]
    no_null_list = [int(x) for x in no_null_list if x != '']
    if len(no_null_list) > 0:
        return max(no_null_list)
    else:
        return None


def get_value_with_avg_length_of_characters(value_list):
    string_list = [elem for elem in value_list if isinstance(elem, str)]
    no_null_list = [x for x in string_list if x != '']
    len_list = [len(x) for x in no_null_list]
    if len(len_list) > 0 and len(no_null_list) > 0:
        highest_len = max(len_list)
        highest_len_index = len_list.index(highest_len)
        highest_max_len_string = no_null_list[highest_len_index]
        return highest_max_len_string
    else:
        return None


def get_count_of_distinct_values(value_list):
    if len(value_list) > 0:
        return math.ceil(sum(value_list) / len(value_list))


def get_count_of_values_with_leading_whitespace(value_list):
    return max(value_list)


def get_count_of_values_with_ending_whitespace(value_list):
    return max(value_list)


def get_count_of_values_with_beginning_and_ending_whitespace(value_list):
    return max(value_list)



files_to_process = [os.path.join(dir_data_profile_file_processed, p.name) for p in Path(dir_data_profile_file_processed).iterdir() if data_subject in p.name]

cols = []
data_rows = []
fields = []
for file in files_to_process:
    print(f'processing {file}')
    df_iter = pd.read_excel(file, keep_default_na=False, na_values=[])
    file_count = [1] * len(df_iter)
    df_iter.insert(1, 'file_count', file_count)
    if len(cols) == 0:
        cols = df_iter.columns.values.tolist()
    data_list = df_iter.to_numpy().tolist()
    if len(fields) == 0:
        fields = [row[0] for row in data_list]
    data_rows.append(data_list)

print('\n')
print(f'fields: {fields}')
print('\n')

data_rows = [elem for row in data_rows for elem in row]

print('\n')
print(f'cols: {cols}')

calc_position_dict = {
    'file_count': 1,
    'data_type': 2,
    'row_count': 3,
    'null_count': 4,
    'is_completely_null': 5,
    'character_blank_count': 6,
    'min_value': 7,
    'min_length_of_characters': 8,
    'value_with_min_length_of_characters': 9,
    'max_value': 10,
    'max_length_of_characters': 11,
    'value_with_max_length_of_characters': 12,
    'max_octet_length': 13,
    'value_with_max_octet_length': 14,
    'avg_length_of_characters': 15,
    'value_with_avg_length_of_characters': 16,
    'count_of_distinct_values': 17,
    'count_of_values_with_leading_whitespace': 18,
    'count_of_values_with_ending_whitespace': 19,
    'count_of_values_with_beginning_and_ending_whitespace': 20
}

file_count_list = []
data_type_list = []
row_count_list = []
null_count_list = []
is_completely_null_list = []
character_blank_count_list = []
min_value_list = []
min_length_of_characters_list = []
value_with_min_length_of_characters_list = []
max_value_list = []
max_length_of_characters_list = []
value_with_max_length_of_characters_list = []
max_octet_length_list = []
value_with_max_octet_length_list = []
avg_length_of_characters_list = []
value_with_avg_length_of_characters_list = []
count_of_distinct_values_list = []
count_of_values_with_leading_whitespace_list = []
count_of_values_with_ending_whitespace_list = []
count_of_values_with_beginning_and_ending_whitespace_list = []

for field in fields:
    print(f'processing {field}')
    file_count_values = [row[calc_position_dict['file_count']] for row in data_rows if row[0] == field]
    file_count = get_file_count(file_count_values)
    file_count_list.append(file_count)

    data_type_values = [row[calc_position_dict['data_type']] for row in data_rows if row[0] == field]
    data_type = get_data_type(data_type_values)
    if data_type is not None:
        data_type = data_type
        data_type_list.append(data_type)
    else:
        print('multiple data types detected for field ' + field)
        print('process cancelled')
        sys.exit()

    row_count_values = [row[calc_position_dict['row_count']] for row in data_rows if row[0] == field]
    row_count = get_row_count(row_count_values)
    row_count_list.append(row_count)

    null_count_values = [row[calc_position_dict['null_count']] for row in data_rows if row[0] == field]
    null_count = get_null_count(null_count_values)
    null_count_list.append(null_count)

    is_completely_null_values = [row[calc_position_dict['is_completely_null']] for row in data_rows if row[0] == field]
    is_completely_null = get_is_completely_null(is_completely_null_values)
    is_completely_null_list.append(is_completely_null)

    character_blank_count_values = [row[calc_position_dict['character_blank_count']] for row in data_rows if row[0] == field]
    character_blank_count = get_character_blank_count(character_blank_count_values)
    character_blank_count_list.append(character_blank_count)

    min_value_values = [row[calc_position_dict['min_value']] for row in data_rows if row[0] == field]
    min_value = get_min_value(min_value_values)
    min_value_list.append(min_value)

    min_length_of_characters_values = [row[calc_position_dict['min_length_of_characters']] for row in data_rows if row[0] == field]
    min_length_of_characters = get_min_length_of_characters(min_length_of_characters_values)
    min_length_of_characters_list.append(min_length_of_characters)

    value_with_min_length_of_characters_values = [row[calc_position_dict['value_with_min_length_of_characters']] for row in data_rows if row[0] == field]
    value_with_min_length_of_characters = get_value_with_min_length_of_characters(value_with_min_length_of_characters_values)
    value_with_min_length_of_characters_list.append(value_with_min_length_of_characters)

    max_value_values = [row[calc_position_dict['max_value']] for row in data_rows if row[0] == field]
    max_value = get_max_value(max_value_values)
    max_value_list.append(max_value)

    max_length_of_characters_values = [row[calc_position_dict['max_length_of_characters']] for row in data_rows if row[0] == field]
    max_length_of_characters = get_max_length_of_characters(max_length_of_characters_values)
    max_length_of_characters_list.append(max_length_of_characters)

    value_with_max_length_of_characters_values = [row[calc_position_dict['value_with_max_length_of_characters']] for row in data_rows if row[0] == field]
    value_with_max_length_of_characters = get_value_with_max_length_of_characters(value_with_max_length_of_characters_values)
    value_with_max_length_of_characters_list.append(value_with_max_length_of_characters)

    max_octet_length_values = [row[calc_position_dict['max_octet_length']] for row in data_rows if row[0] == field]
    max_octet_length = get_max_octet_length(max_octet_length_values)
    max_octet_length_list.append(max_octet_length)

    value_with_max_octet_length_values = [row[calc_position_dict['value_with_max_octet_length']] for row in data_rows if row[0] == field]
    value_with_max_octet_length = get_value_with_max_octet_length(value_with_max_octet_length_values)
    value_with_max_octet_length_list.append(value_with_max_octet_length)

    avg_length_of_characters_values = [row[calc_position_dict['avg_length_of_characters']] for row in data_rows if row[0] == field]
    avg_length_of_characters = get_avg_length_of_characters(avg_length_of_characters_values)
    avg_length_of_characters_list.append(avg_length_of_characters)

    value_with_avg_length_of_characters_values = [row[calc_position_dict['value_with_avg_length_of_characters']] for row in data_rows if row[0] == field]
    value_with_avg_length_of_characters = get_value_with_avg_length_of_characters(value_with_avg_length_of_characters_values)
    value_with_avg_length_of_characters_list.append(value_with_avg_length_of_characters)

    count_of_distinct_values_values = [row[calc_position_dict['count_of_distinct_values']] for row in data_rows if row[0] == field]
    count_of_distinct_values = get_count_of_distinct_values(count_of_distinct_values_values)
    count_of_distinct_values_list.append(count_of_distinct_values)

    count_of_values_with_leading_whitespace_values = [row[calc_position_dict['count_of_values_with_leading_whitespace']] for row in data_rows if row[0] == field]
    count_of_values_with_leading_whitespace = get_count_of_values_with_leading_whitespace(count_of_values_with_leading_whitespace_values)
    count_of_values_with_leading_whitespace_list.append(count_of_values_with_leading_whitespace)

    count_of_values_with_ending_whitespace_values = [row[calc_position_dict['count_of_values_with_ending_whitespace']] for row in data_rows if row[0] == field]
    count_of_values_with_ending_whitespace = get_count_of_values_with_ending_whitespace(count_of_values_with_ending_whitespace_values)
    count_of_values_with_ending_whitespace_list.append(count_of_values_with_ending_whitespace)

    count_of_values_with_beginning_and_ending_whitespace_values = [row[calc_position_dict['count_of_values_with_beginning_and_ending_whitespace']] for row in data_rows if row[0] == field]
    count_of_values_with_beginning_and_ending_whitespace = get_count_of_values_with_beginning_and_ending_whitespace(count_of_values_with_beginning_and_ending_whitespace_values)
    count_of_values_with_beginning_and_ending_whitespace_list.append(count_of_values_with_beginning_and_ending_whitespace)

# write results to df
results_df = pd.DataFrame({
    'field': fields
    ,'file_count': file_count_list
    ,'data_type': data_type_list
    ,'row_count': row_count_list
    ,'null_count': null_count_list
    ,'is_completely_null': is_completely_null_list
    ,'character_blank_count': character_blank_count_list
    ,'min_value': min_value_list
    ,'min_length_of_characters': min_length_of_characters_list
    ,'value_with_min_length_of_characters': value_with_min_length_of_characters_list
    ,'max_value': max_value_list
    ,'max_length_of_characters': max_length_of_characters_list
    ,'value_with_max_length_of_characters': value_with_max_length_of_characters_list
    ,'max_octet_length': max_octet_length_list
    ,'value_with_max_octet_length': value_with_max_octet_length_list
    ,'avg_length_of_characters': avg_length_of_characters_list
    ,'value_with_avg_length_of_characters': value_with_avg_length_of_characters_list
    ,'count_of_distinct_values': count_of_distinct_values_list
    ,'count_of_values_with_leading_whitespace': count_of_values_with_leading_whitespace_list
    ,'count_of_values_with_ending_whitespace': count_of_values_with_ending_whitespace_list
    ,'count_of_values_with_beginning_and_ending_whitespace': count_of_values_with_beginning_and_ending_whitespace_list
})

cfx.delete_file_if_exists(file_results)
cfx.create_excel_workbook(file_results)
cfx.append_df_to_existing_excel_workbook(results_df, file_results, data_subject)
cfx.load_excel_file_and_clean(file_results)

# print('\n')
# print(results_df)

print('\n')
print('success')

