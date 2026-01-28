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
version_compare_from = config.version_compare_from
version_compare_to = config.version_compare_to
file_in_comparison_to = config.file_in_prop_level_comparison_to_path
file_in_comparison_from = config.file_in_prop_level_comparison_from_path
file_out_comparison = config.file_comparison_prop_total_path
comparison_columns = config.comparison_columns
file_out_test = r'C:\TEMP\test.xlsx'
df_version = config.df_version

cfx.delete_file_if_exists(file_out_comparison)
cfx.create_excel_workbook(file_out_comparison)

sheet_format_dict = {}
# {sheet: [{format: [column number]}, row_start, row_end]}

cfx.append_df_to_existing_excel_workbook(df_version, file_out_comparison, 'comparison versions')

df_compare_to = pd.read_csv(file_in_comparison_to)
df_compare_from = pd.read_csv(file_in_comparison_from)

# total
room_nights_compare_to_total = df_compare_to['room_nights'].sum()
room_nights_compare_from_total = df_compare_from['room_nights'].sum()
rev_compare_to_total = df_compare_to[f'chkout_{charge_category}_usd_amt'].sum()
rev_compare_from_total = df_compare_from[f'chkout_{charge_category}_usd_amt'].sum()

measure_columns = [col for col in comparison_columns if version_compare_to in col or version_compare_from in col]

data_value = [
    room_nights_compare_to_total
    ,room_nights_compare_from_total
    ,rev_compare_to_total
    ,rev_compare_from_total
]

data_dict = dict(zip(measure_columns, data_value))

df_total = pd.DataFrame(data=data_dict, index=[0])
df_total = cfx.add_variance_columns(df_total, measure_columns, charge_category, version_compare_from, version_compare_to)
df_total = cfx.sort_df(df_total, comparison_columns)
cfx.append_df_to_existing_excel_workbook(df_total, file_out_comparison, 'total')
sheet_format_dict['total'] = [{'#,##0': [1, 2, 3, 5, 6, 7], '0.0%': [4, 8]}, 2, None]

# prop_cd
df_prop_cd = df_compare_to.merge(
    df_compare_from, how='outer', on=['prop_cd', 'country_cd', 'country_desc', 'op_area_level2_desc'], suffixes=(f' {version_compare_to}', f' {version_compare_from}'))
df_prop_cd = cfx.rename_df_columns(df_prop_cd, comparison_columns, version_compare_to, version_compare_from, charge_category)
df_prop_cd[['op_area_level2_desc', 'country_cd', 'country_desc']] = df_prop_cd[['op_area_level2_desc', 'country_cd', 'country_desc']].fillna('Unknown')
df_prop_cd = cfx.add_variance_columns(df_prop_cd, measure_columns, charge_category, version_compare_from, version_compare_to)
df_prop_cd = cfx.sort_df(df_prop_cd, comparison_columns)
cfx.append_df_to_existing_excel_workbook(df_prop_cd, file_out_comparison, 'prop_cd')
sheet_format_dict['prop_cd'] = [{'#,##0': [5, 6, 7, 9, 10, 11], '0.0%': [8, 12]}, 2, None]

# country
df_country = df_prop_cd.groupby(['op_area_level2_desc', 'country_cd', 'country_desc']) [measure_columns].sum().reset_index()
df_country = cfx.add_variance_columns(df_country, measure_columns, charge_category, version_compare_from, version_compare_to)
df_country = cfx.sort_df(df_country, comparison_columns)
cfx.append_df_to_existing_excel_workbook(df_country, file_out_comparison, 'country')
sheet_format_dict['country'] = [{'#,##0': [4, 5, 6, 8, 9, 10], '0.0%': [7, 11]}, 2, None]

# op_area_level2_desc
df_op_area_level2_desc = df_country.groupby(['op_area_level2_desc']) [measure_columns].sum().reset_index()
df_op_area_level2_desc = cfx.add_variance_columns(df_op_area_level2_desc, measure_columns, charge_category, version_compare_from, version_compare_to)
df_op_area_level2_desc = cfx.sort_df(df_op_area_level2_desc, comparison_columns)
cfx.append_df_to_existing_excel_workbook(df_op_area_level2_desc, file_out_comparison, 'op_area_level2_desc')
sheet_format_dict['op_area_level2_desc'] = [{'#,##0': [2, 3, 4, 6, 7, 8], '0.0%': [5, 9]}, 2, None]

#clean excel file
wb = openpyxl.load_workbook(filename=file_out_comparison)
cfx.format_excel_data(wb, sheet_format_dict, file_out_comparison)
cfx.clean_excel_file(wb, file_out_comparison)
sheet_order = ['comparison versions', 'total', 'op_area_level2_desc', 'country', 'prop_cd']
cfx.sort_excel_sheets_from_list(wb, sheet_order, file_out_comparison)
wb.close()

time_end = time.perf_counter()
elapsed_time = time_end - time_start

print('success')
print(f'total execution time: {elapsed_time:.4f} seconds')
print(f'total execution time: {float(elapsed_time/60):.4f} minutes')
