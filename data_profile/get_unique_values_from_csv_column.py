import polars as pl
from pathlib import Path

subject_search = 'TestReservation'
dir_file = r'C:\Users\jwalker221\OneDrive - Hilton\Jira\Fornova\data_profile\file_in'
field_name = 'DIRECT OTA PRICE'
values_list = []


files_to_process = [file for file in Path(dir_file).iterdir() if subject_search in file.name]
for f in files_to_process:
    df_lf = pl.scan_csv(f, encoding='utf8')
    field_df = df_lf.select(
        pl.col(field_name).unique().alias(field_name)
    ).collect(engine='streaming')
    field_values = field_df[field_name].to_list()
    values_list.append(field_values)

values_list = [elem for row in values_list for elem in row]
unique_values = list(set(values_list))
print(unique_values)
