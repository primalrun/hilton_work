import os
import common_functions as cfx
import pandas as pd
import configparser

file_data_profile_in_method_pick = 'auto'
file_name_data_profile_in = r'Users_20251224_chain_Hilton Corporate.csv'
file_data_profile_in_subject = 'Parity' # Parity, Extranet, Compliance, Test Reservation, User
file_data_profile_in_date_start = '20251213'
file_data_profile_in_date_end = '20251213'
file_data_profile_in_file_type = 'daily'

file_version_data_dict_fornova = '20250417_1'
file_name_data_dict_fornova = r'Fornova Data Dictionaries 2025_04_17.xlsx'

file_name_fornova_data_type_overwrite = r'data_type_overwrite.xlsx'
file_name_data_dict_fornova_processed = r'fornova_data_dict_processed.xlsx'


data_profile_column_active_dict = {
    'row_count': 1
    , 'null_count': 1
    , 'character_blank_count': 1
    , 'min_value': 1
    , 'min_length_of_characters': 1
    , 'value_with_min_length_of_characters': 1
    , 'max_value': 1
    , 'max_length_of_characters': 1
    , 'value_with_max_length_of_characters': 1
    , 'max_octet_length': 1
    , 'value_with_max_octet_length': 1
    , 'avg_length_of_characters': 1
    , 'value_with_avg_length_of_characters': 1
    , 'count_of_distinct_values': 1
    , 'count_of_values_with_leading_whitespace': 1
    , 'count_of_values_with_ending_whitespace': 1
    , 'count_of_values_with_beginning_and_ending_whitespace': 1
}

if file_data_profile_in_method_pick == 'auto':
    data_subject, file_date_start, file_type = cfx.data_profile_file_in_get_attributes(file_name_data_profile_in)
    file_date_end = file_date_start
else:
    data_subject = file_data_profile_in_subject
    file_date_start = file_data_profile_in_date_start
    file_date_end = file_data_profile_in_date_end
    file_type = file_data_profile_in_file_type

file_name_data_profile_out = f"data profile processed {data_subject} ({file_type}) {file_date_start}-{file_date_end}.xlsx"


file_config = r'C:\Users\jwalker221\OneDrive - Hilton\Documents\cred.ini'
config = configparser.ConfigParser()
config.read(file_config)
dw_prod_config = config['dw_prod']
host_prod = dw_prod_config['host']
dbname_prod = dw_prod_config['dbname']
user_prod = dw_prod_config['user']
password_prod = dw_prod_config['password']

rs_cred_prod = [
    host_prod
    ,dbname_prod
    ,user_prod
    ,password_prod
]

dir_main = r'C:\Users\jwalker221\OneDrive - Hilton\Jira\Fornova'
dir_data_dict = os.path.join(dir_main, 'data_dictionary')
dir_data_dict_fornova = os.path.join(dir_data_dict, 'fornova')
dir_data_profile = os.path.join(dir_main, 'data_profile')
dir_data_profile_file_in = os.path.join(dir_data_profile, 'file_in')
dir_data_profile_file_processed = os.path.join(dir_data_profile, 'file_processed')
file_data_dict_fornova = os.path.join(dir_data_dict_fornova, file_name_data_dict_fornova)
file_fornova_data_type_overwrite = os.path.join(dir_data_dict, file_name_fornova_data_type_overwrite)
file_data_dict_fornova_processed = os.path.join(dir_data_dict, file_name_data_dict_fornova_processed)
file_data_profile_in = os.path.join(dir_data_profile_file_in, file_name_data_profile_in)
file_data_profile_out = os.path.join(dir_data_profile_file_processed, file_name_data_profile_out)
dir_data_profile_processed_data_subject_total = os.path.join(dir_data_profile_file_processed, 'data_subject_total')
dir_data_profile_final_result = os.path.join(dir_data_profile, 'final_result')

data_type_normalization_dict = {
    'double': 'double precision'
    ,'float': 'double precision'
    ,'int': 'integer'
    ,'string': 'varchar'
}


data_profile_calculation_column_sort = [
    'field'
    ,'data_type'
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
]


