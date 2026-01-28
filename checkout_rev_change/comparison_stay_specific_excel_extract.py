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
file_in_sql_stay_specific_checkout_comparison_path = config.file_in_sql_stay_specific_checkout_comparison_path
file_in_stay_specific_path = config.file_in_stay_specific_path
dir_comparison_stay_specific_out = config.dir_comparison_stay_specific_out
dict_sql_replacement = config.dict_sql_stay_specific_replacement[charge_category]
excel_format = config.dict_excel_format_stay_specific[charge_category]
df_version = config.df_version

df_stay = pd.read_excel(file_in_stay_specific_path)
stay_to_process = list(df_stay.itertuples(index=False, name=None))

# redshift connection
rs_host, rs_database, rs_user, rs_pword = config.rs_cred
rs_conn = cfx.connect_rs(rs_host, rs_database, rs_user, rs_pword)
cursor = rs_conn.cursor()

# loop through properties to process
for prop_cd, stay_id in stay_to_process:
    stay_id = str(stay_id)
    print(f"processing {prop_cd}: {stay_id}")
    sheet_format_dict = {}
    file_out_name = f"{prop_cd} {stay_id} {date_logic} date ({period_compare_start_yyyymmdd}-{period_compare_end_yyyymmdd}).xlsx"
    file_out_path = os.path.join(dir_comparison_stay_specific_out, file_out_name)
    cfx.delete_file_if_exists(file_out_path)
    cfx.create_excel_workbook(file_out_path)

    cfx.append_df_to_existing_excel_workbook(df_version, file_out_path, 'comparison versions')

    dict_sql_replacement['prop_cd_variable'] = prop_cd
    dict_sql_replacement['stay_id_variable'] = stay_id

    with open(file_in_sql_stay_specific_checkout_comparison_path, 'r') as file_r:
        sql = file_r.read()

    sql = cfx.replace_sql_from_dict(sql, dict_sql_replacement)
    cursor.execute(sql)
    df = cursor.fetch_dataframe()
    cfx.append_df_to_existing_excel_workbook(df, file_out_path, f"{prop_cd} {stay_id}")

    #clean excel file
    sheet_format_dict[f"{prop_cd} {stay_id}"] = excel_format
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


