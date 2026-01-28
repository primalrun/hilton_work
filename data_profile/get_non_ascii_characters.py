import polars as pl
import polars.selectors as cs
import common_functions as cfx
import sys
import pandas as pd

file_in = r'C:\Users\jwalker221\OneDrive - Hilton\Jira\Fornova\data_profile\file_in\TestReservation_May-31st-2025_chain_Hilton Corporate_correction_file.csv'
file_out_temp = r'c:\temp\1.xlsx'

print(f'file_in: {file_in}')

# df_in = pl.scan_csv(file_in)
df_in = pl.scan_csv(file_in, encoding = 'utf8')

df_test = df_in.filter(
pl.col('POS').str.len_bytes() == 3
).select(
    pl.col('POS').unique()
).collect(engine='streaming')

str_utf8 = df_test.item(0, 'POS')
print(f'string utf8: {str_utf8}')

str_bytes = str_utf8.encode('utf-8')
print(f'string bytes: {str_bytes}')

len_bytes = len(str_bytes)
print(f'len bytes: {len_bytes}')

is_ascii = str_utf8.isascii()
print(f'is_ascii: {is_ascii}')

print('\n')

for char in str_utf8:
    print(f'char: {char}')
    encoded_char = char.encode('utf-8')
    print(f'encoded_char: {encoded_char}')
    byte_length = len(encoded_char)
    print(f'byte_length: {byte_length}')


# df_pd = pd.read_csv(file_in)
#
# def is_ascii_check(value):
#     if isinstance(value, str):
#         return value.isascii()
#     else:
#         return False
#
#
# df_pd['POS is_ascii'] = df_pd['POS'].apply(is_ascii_check)
# pos_df = df_pd[['POS', 'POS is_ascii']]
# pos_df = pos_df[~pos_df['POS is_ascii']]
#
# print(pos_df.head())

# cfx.write_pl_df_to_excel_temp(pos_df, file_out_temp)
# cfx.write_df_to_excel_temp(pos_df, file_out_temp, '1')
