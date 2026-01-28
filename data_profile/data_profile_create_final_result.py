import numpy as np
import pandas as pd
import config
import common_functions as cfx
from pathlib import Path
import os
import math
import sys

data_subject = 'TestReservation'
file_result_name = f"Data Profile Results {data_subject}.xlsx"
file_temp = r'c:\temp\1.xlsx'
file_data_type_name = 'data_type_target.xlsx'

definitions_data = {
    'column_name': ['data_dict_is_required_for_reporting', 'is_required_field', 'is_required_for_business_reporting']
    ,'definition': [
        'from data dictionary, text indication if field is required for reporting'
        ,'if field exists in data dictionary, then yes else no'
        ,'if data dictionary requirement has any yes character, then yes else no'
    ]
}

def add_df_column_using_dict_lookup(field_name, lookup_dict, dict_value_pos):
    field_name = field_name.lower()
    if field_name in lookup_dict:
        return lookup_dict[field_name][dict_value_pos]
    else:
        return None


def get_is_required(is_required):
    if is_required is None:
        return 'no'
    if 'yes' in str(is_required.lower()):
        return 'yes'
    else:
        return 'no'


def get_source_data_type(data_type_source, data_type_target, transform_needed_str)->str:
    if 'int' in data_type_source.lower():
        return 'integer'
    if 'float' in data_type_source.lower():
        return 'float'
    if data_type_source.lower() == 'string':
        if data_type_target == 'date' and transform_needed_str is None:
            return 'date'
        if transform_needed_str is not None:
            return 'string'
        else:
            return 'string'
    else:
        return 'string'


def get_target_data_type(source_data_type, target_data_type, max_char_length, max_octet_length):
    if source_data_type == 'string':
        if math.isnan(max_char_length) is False and math.isnan(max_octet_length) is False:
            max_length = int(max(max_char_length, max_octet_length))
        if target_data_type is None and math.isnan(max_octet_length) is True:
            max_length = int(max_char_length)
        if target_data_type is None:
            return f"varchar({max_length})"
        if (target_data_type != source_data_type) and (target_data_type is not None):
            return target_data_type
        else:
            return f"varchar({max_length})"
    if source_data_type != 'string':
        if target_data_type is not None:
            return target_data_type
        else:
            return source_data_type


def get_target_data_type_with_buffer(source_data_type, target_data_type, max_char_length, max_octet_length):
    if source_data_type == 'string':
        if math.isnan(max_char_length) is False and math.isnan(max_octet_length) is False:
            max_length = int(max(max_char_length, max_octet_length))
            max_length = math.ceil((max_length * 1.2))
        if target_data_type is None and math.isnan(max_octet_length) is True:
            max_length = int(max_char_length)
            max_length = math.ceil((max_length * 1.2))
        if target_data_type is None:
            return f"varchar({max_length})"
        if (target_data_type != source_data_type) and (target_data_type is not None):
            return target_data_type
        else:
            return f"varchar({max_length})"
    if source_data_type != 'string':
        if target_data_type is not None:
            return target_data_type
        else:
            return source_data_type



dir_data_profile_processed_data_subject_total = config.dir_data_profile_processed_data_subject_total
dir_data_profile_final_result = config.dir_data_profile_final_result
file_result = os.path.join(dir_data_profile_final_result, file_result_name)
file_data_dict_fornova_processed = config.file_data_dict_fornova_processed
dir_data_profile = config.dir_data_profile
file_data_type = os.path.join(dir_data_profile, file_data_type_name)

files_to_process = [
    os.path.join(dir_data_profile_processed_data_subject_total, p.name)
    for p in Path(dir_data_profile_processed_data_subject_total).iterdir()
    if data_subject in p.name]
file_data_profile_in = files_to_process[0]

# data dictionary
data_dict_df = pd.read_excel(file_data_dict_fornova_processed)

data_dict_subject_name = {
    'TestReservation': 'Test Reservation'
    ,'Users': 'User'
}

if data_subject in data_dict_subject_name:
    data_dict_subject_search = data_dict_subject_name[data_subject]
else:
    data_dict_subject_search = data_subject

data_dict_columns = [
    'Field Name'
    ,'Data Type'
    ,'Description'
    ,'char_max_length'
    ,'is_required_for_reporting'
]


data_dict_df = data_dict_df[data_dict_columns][data_dict_df['Filename'] == data_dict_subject_search].replace({np.nan: None})
field_name_dd = data_dict_df['Field Name'].values.tolist()
data_type_dd = data_dict_df['Data Type'].values.tolist()
description_dd = data_dict_df['Description'].values.tolist()
char_max_length_dd = data_dict_df['char_max_length'].values.tolist()
is_required_for_reporting_dd = data_dict_df['is_required_for_reporting'].values.tolist()

data_dict = {}
for i in range(len(field_name_dd)):
    field_name = field_name_dd[i]
    if field_name is not None:
        field_name = field_name.lower()
    data_type = data_type_dd[i]
    if data_type is not None:
        data_type = data_type.lower()
    description = description_dd[i]
    if description is not None:
        description = description.lower()
    char_max_length = char_max_length_dd[i]
    is_required_for_reporting = is_required_for_reporting_dd[i]
    if is_required_for_reporting is not None:
        is_required_for_reporting = is_required_for_reporting.lower()
    data_dict[field_name] = [data_type, description, char_max_length, is_required_for_reporting]

# data type target
data_type_target_df = pd.read_excel(file_data_type)
data_type_target_df = data_type_target_df[data_type_target_df['data_subject'] == data_subject].replace({np.nan: None})
data_type_target_dict = {}
field_dtt = data_type_target_df['field'].values.tolist()
data_type_new_dtt = data_type_target_df['data_type_new'].values.tolist()
transform_needed_dtt = data_type_target_df['transform_needed'].values.tolist()
source_format_dtt = data_type_target_df['source_format'].values.tolist()

for i in range(len(field_dtt)):
    field_name = field_dtt[i].lower()
    data_type_new = data_type_new_dtt[i]
    transform_needed = transform_needed_dtt[i]
    source_format = source_format_dtt[i]
    data_type_target_dict[field_name] = [data_type_new, transform_needed, source_format]

# data profile
data_profile_df = pd.read_excel(file_data_profile_in)
data_profile_df['data dict data_type'] = data_profile_df.apply(lambda x: add_df_column_using_dict_lookup(x['field'], data_dict, 0), axis=1)
data_profile_df['data dict description'] = data_profile_df.apply(lambda x: add_df_column_using_dict_lookup(x['field'], data_dict, 1), axis=1)
data_profile_df['data dict char_max_length'] = data_profile_df.apply(lambda x: add_df_column_using_dict_lookup(x['field'], data_dict, 2), axis=1)
data_profile_df['data dict is_required_for_reporting'] = data_profile_df.apply(lambda x: add_df_column_using_dict_lookup(x['field'], data_dict, 3), axis=1)
data_profile_df['is_required_field'] = data_profile_df.apply(lambda x: 'yes' if x['data dict data_type'] is not None else 'no', axis=1)
data_profile_df['is_required_for_business_reporting'] = data_profile_df.apply(lambda x: get_is_required(x['data dict is_required_for_reporting']), axis=1)
data_profile_df['data_type_new'] = data_profile_df.apply(lambda x: add_df_column_using_dict_lookup(x['field'], data_type_target_dict, 0), axis=1)
data_profile_df['transform_needed'] = data_profile_df.apply(lambda x: add_df_column_using_dict_lookup(x['field'], data_type_target_dict, 1), axis=1)
data_profile_df['source_format'] = data_profile_df.apply(lambda x: add_df_column_using_dict_lookup(x['field'], data_type_target_dict, 2), axis=1)
data_profile_df['source_data_type'] = data_profile_df.apply(lambda x: get_source_data_type(x['data_type'], x['data_type_new'], x['transform_needed']), axis=1)

# cfx.write_df_to_excel_temp(data_profile_df, file_temp, '1')
# sys.exit()

data_profile_df['target_data_type'] = data_profile_df.apply(
    lambda x: get_target_data_type(
        x['source_data_type'], x['data_type_new'], x['max_length_of_characters'], x['max_octet_length']), axis=1)
data_profile_df['target_data_type_with_20_pct_buffer'] = data_profile_df.apply(
    lambda x: get_target_data_type_with_buffer(
        x['source_data_type'], x['data_type_new'], x['max_length_of_characters'], x['max_octet_length']), axis=1)

result_columns = [
'field'
,'file_count'
,'source_data_type'
,'target_data_type'
,'target_data_type_with_20_pct_buffer'
,'transform_needed'
,'source_format'
,'row_count'
,'null_count'
,'is_completely_null'
,'character_blank_count'
,'min_value'
,'min_length_of_characters'
,'value_with_min_length_of_characters'
,'max_value'
,'max_length_of_characters'
,'value_with_max_length_of_characters'
,'max_octet_length'
,'value_with_max_octet_length'
,'avg_length_of_characters'
,'value_with_avg_length_of_characters'
,'count_of_distinct_values'
,'count_of_values_with_leading_whitespace'
,'count_of_values_with_ending_whitespace'
,'count_of_values_with_beginning_and_ending_whitespace'
,'data dict data_type'
,'data dict description'
,'data dict char_max_length'
,'data dict is_required_for_reporting'
,'is_required_field'
,'is_required_for_business_reporting'
]

data_profile_df = data_profile_df[result_columns].copy()

cfx.delete_file_if_exists(file_result)
cfx.create_excel_workbook(file_result)

definition_df = pd.DataFrame(definitions_data)
cfx.append_df_to_existing_excel_workbook(definition_df, file_result, 'column_definition')

cfx.append_df_to_existing_excel_workbook(data_profile_df, file_result, data_subject)
cfx.load_excel_file_and_clean(file_result)

print('success')
