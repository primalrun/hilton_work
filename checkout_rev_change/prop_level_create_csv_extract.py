import config
import common_functions as cfx
import time
import datetime

time_start = time.perf_counter()
script_start_time = datetime.datetime.now()
print(f"Script started at {script_start_time.strftime('%Y-%m-%d %H:%M:%S')}")

charge_category = config.charge_category
version_extract = config.version_extract

sql_file_in = config.file_sql_in_prop_level_path
file_out_extract = config.file_extract_out_prop_level_path
cfx.delete_file_if_exists(file_out_extract)

# redshift connection
rs_host, rs_database, rs_user, rs_pword = config.rs_cred
rs_conn = cfx.connect_rs(rs_host, rs_database, rs_user, rs_pword)
cursor = rs_conn.cursor()

with open(sql_file_in, 'r') as file_r:
    sql = file_r.read()

dict_sql_replacement = config.dict_sql_prop_level_replacement[charge_category][version_extract]
sql = cfx.replace_sql_from_dict(sql, dict_sql_replacement)

cursor.execute(sql)
df = cursor.fetch_dataframe()
numeric_columns = [f'chkout_{charge_category}_usd_amt']
column_decimal_places = [2]
df = cfx.convert_df_columns_to_numeric(df, numeric_columns, column_decimal_places)

df.to_csv(file_out_extract, index=False, header=True)
cursor.close()
rs_conn.close()

time_end = time.perf_counter()
elapsed_time = time_end - time_start

print('success')
print(f'total execution time: {elapsed_time:.4f} seconds')
print(f'total execution time: {float(elapsed_time/60):.4f} minutes')
