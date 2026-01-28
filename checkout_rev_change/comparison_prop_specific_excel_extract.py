import os

import config
import common_functions as cfx
import pandas as pd
import openpyxl
import sys
import time
import datetime

time_start = time.perf_counter()
script_start_time = datetime.datetime.now()
print(f"Script started at {script_start_time.strftime('%Y-%m-%d %H:%M:%S')}")

charge_category = config.charge_category
date_logic = config.date_logic
period_compare_start_yyyymmdd = config.period_compare_start_yyyymmdd
period_compare_end_yyyymmdd = config.period_compare_end_yyyymmdd
file_in_sql_prop_specific_checkout_comparison_path = config.file_in_sql_prop_specific_checkout_comparison_path
file_in_prop_specific_path = config.file_in_prop_specific_path
dir_comparison_prop_specific_out = config.dir_comparison_prop_specific_out
dict_sql_replacement = config.dict_sql_prop_specific_replacement[charge_category]
excel_format = config.dict_excel_format_prop_specific[charge_category]
df_version = config.df_version

df_prop = pd.read_excel(file_in_prop_specific_path)
prop_to_process = tuple(df_prop['prop_cd'])

# redshift connection
rs_host, rs_database, rs_user, rs_pword = config.rs_cred
rs_conn = cfx.connect_rs(rs_host, rs_database, rs_user, rs_pword)
cursor = rs_conn.cursor()

# loop through properties to process
for prop in prop_to_process:
    print(f"processing {prop}")
    sheet_format_dict = {}
    file_out_name = f"{prop} {date_logic} date ({period_compare_start_yyyymmdd}-{period_compare_end_yyyymmdd}).xlsx"
    file_out_path = os.path.join(dir_comparison_prop_specific_out, file_out_name)
    cfx.delete_file_if_exists(file_out_path)
    cfx.create_excel_workbook(file_out_path)

    cfx.append_df_to_existing_excel_workbook(df_version, file_out_path, 'comparison versions')

    dict_sql_replacement['prop_cd_variable'] = prop

    with open(file_in_sql_prop_specific_checkout_comparison_path, 'r') as file_r:
        sql = file_r.read()

    sql = cfx.replace_sql_from_dict(sql, dict_sql_replacement)
    cursor.execute(sql)
    df = cursor.fetch_dataframe()
    df = cfx.replace_df_column_name_prop_specific(df, ['prop_cd', 'stay_id'], '_', ' ', charge_category)
    df['v2 vs v1 var amt'] = df.apply(lambda x: x[f'{charge_category} rev EDP v2'] - x[f'{charge_category} rev EDP v1'], axis=1)
    df['v2 vs v1 var pct'] = df.apply(lambda x: cfx.calc_variance_pct(x['v2 vs v1 var amt'], x[f'{charge_category} rev EDP v1']), axis=1)
    df['v3 vs v1 var amt'] = df.apply(lambda x: x[f'{charge_category} rev EDP v3'] - x[f'{charge_category} rev EDP v1'], axis=1)
    df['v3 vs v1 var pct'] = df.apply(lambda x: cfx.calc_variance_pct(x['v3 vs v1 var amt'], x[f'{charge_category} rev EDP v1']), axis=1)
    df['v3 vs v2 var amt'] = df.apply(lambda x: x[f'{charge_category} rev EDP v3'] - x[f'{charge_category} rev EDP v2'], axis=1)
    df['v3 vs v2 var pct'] = df.apply(lambda x: cfx.calc_variance_pct(x['v3 vs v2 var amt'], x[f'{charge_category} rev EDP v2']), axis=1)
    df['v4 vs v1 var amt'] = df.apply(lambda x: x[f'{charge_category} rev EDP v4'] - x[f'{charge_category} rev EDP v1'], axis=1)
    df['v4 vs v1 var pct'] = df.apply(lambda x: cfx.calc_variance_pct(x['v4 vs v1 var amt'], x[f'{charge_category} rev EDP v1']), axis=1)
    cfx.append_df_to_existing_excel_workbook(df, file_out_path, prop)

    #clean excel file
    sheet_format_dict[prop] = excel_format
    wb = openpyxl.load_workbook(filename=file_out_path)
    cfx.format_excel_data(wb, sheet_format_dict, file_out_path)
    cfx.clean_excel_file(wb, file_out_path)
    wb.close()

cursor.close()
rs_conn.close()

time_end = time.perf_counter()
elapsed_time = time_end - time_start

print('success')
print(f'total execution time: {elapsed_time:.4f} seconds')
print(f'total execution time: {float(elapsed_time/60):.4f} minutes')



