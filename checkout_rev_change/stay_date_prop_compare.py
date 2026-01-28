from pathlib import Path
import pandas as pd
import os
import sys
from functools import reduce
import common_functions as cfx
import openpyxl
import time

dir_extract_file = r'C:\Users\jwalker221\OneDrive - Hilton\Jira\Checkout Revenue Changes\room\stay_date_comparison\extract'
dir_result = r'C:\Users\jwalker221\OneDrive - Hilton\Jira\Checkout Revenue Changes\room\stay_date_comparison\result'
extract_search = 'room rev stay date 20250608-20250614'
sheet_name = 'stay date 20250608-20250614'
file_prop_hierarchy = os.path.join(dir_extract_file, 'prop_hierarchy.xlsx')
file_result = os.path.join(dir_result, f'{extract_search} prop comparison.xlsx')
file_temp = r'c:\temp\1.xlsx'

def rename_df_columns_except_prop(df, suffix)->pd.DataFrame:
    new_col = {
        col: f'{col}_{suffix}' for col in df.columns if col != 'prop_cd'
    }
    return df.rename(columns=new_col)


def df_add_rev_var_pct_columns(df)->pd.DataFrame:
    df['room_rev_usd_var_pct_MM_vs_OA'] = df.apply(
        lambda x: cfx.calc_variance_pct_between_from_and_to(
            x['room_rev_usd_OA'], x['room_rev_usd_MM']), axis=1)
    df['room_rev_usd_var_pct_EDPv2_vs_OA'] = df.apply(
        lambda x: cfx.calc_variance_pct_between_from_and_to(
            x['room_rev_usd_OA'], x['room_rev_usd_EDPv2']), axis=1)
    df['room_rev_usd_var_pct_EDPv4_vs_OA'] = df.apply(
        lambda x: cfx.calc_variance_pct_between_from_and_to(
            x['room_rev_usd_OA'], x['room_rev_usd_EDPv4']), axis=1)
    df['room_rev_usd_var_pct_EDPv2_vs_MM'] = df.apply(
        lambda x: cfx.calc_variance_pct_between_from_and_to(
            x['room_rev_usd_MM'], x['room_rev_usd_EDPv2']), axis=1)
    df['room_rev_usd_var_pct_EDPv4_vs_MM'] = df.apply(
        lambda x: cfx.calc_variance_pct_between_from_and_to(
            x['room_rev_usd_MM'], x['room_rev_usd_EDPv4']), axis=1)
    df['room_rev_usd_var_pct_EDPv4_vs_EDPv2'] = df.apply(
        lambda x: cfx.calc_variance_pct_between_from_and_to(
            x['room_rev_usd_EDPv2'], x['room_rev_usd_EDPv4']), axis=1)
    return df


def df_sort_columns(df, column_sort_list)->pd.DataFrame:
    column_new = [col for col in col_sort if col in df.columns]
    return df[column_new]


col_sort = [
    'op_area_level2_desc'
    ,'country_cd'
    ,'country_desc'
    ,'prop_cd'
    ,'room_nights_OA'
    ,'room_nights_MM'
    ,'room_nights_EDPv2'
    ,'room_nights_EDPv4'
    , 'room_rev_usd_OA'
    , 'room_rev_usd_MM'
    , 'room_rev_usd_EDPv2'
    , 'room_rev_usd_EDPv4'
    , 'room_rev_usd_var_pct_MM_vs_OA'
    , 'room_rev_usd_var_pct_EDPv2_vs_OA'
    , 'room_rev_usd_var_pct_EDPv4_vs_OA'
    , 'room_rev_usd_var_pct_EDPv2_vs_MM'
    , 'room_rev_usd_var_pct_EDPv4_vs_MM'
    , 'room_rev_usd_var_pct_EDPv4_vs_EDPv2'
]

col_measure = [
    'room_nights_OA'
    ,'room_nights_MM'
    ,'room_nights_EDPv2'
    ,'room_nights_EDPv4'
    , 'room_rev_usd_OA'
    , 'room_rev_usd_MM'
    , 'room_rev_usd_EDPv2'
    , 'room_rev_usd_EDPv4'
]


dist_sort = [
'< -100%'
,'equals -100%'
,'between -100% and -90%'
,'between -90% and -80%'
,'between -80% and -70%'
,'between -70% and -60%'
,'between -60% and -50%'
,'between -50% and -40%'
,'between -40% and -30%'
,'between -30% and -20%'
,'between -20% and -10%'
,'between -10% and -8%'
,'between -8% and -6%'
,'between -6% and -4%'
,'between -4% and -2%'
,'between -2% and 0%'
,'between 0% and 2%'
,'between 2% and 4%'
,'between 4% and 6%'
,'between 6% and 8%'
,'between 8% and 10%'
,'between 10% and 20%'
,'between 20% and 30%'
,'between 30% and 40%'
,'between 40% and 50%'
,'between 50% and 60%'
,'between 60% and 70%'
,'between 70% and 80%'
,'between 90% and 100%'
,'equals 100%'
,'> 100%'
]


files_rev_extract = [os.path.join(dir_extract_file, p.name) for p in Path(dir_extract_file).iterdir() if extract_search in p.name]

prop_hierarchy_df = pd.read_excel(io=file_prop_hierarchy)
file_rev_oa = [file for file in files_rev_extract if 'OA' in file][0]
file_rev_marketmix = [file for file in files_rev_extract if 'MarketMix' in file][0]
file_rev_edpv2 = [file for file in files_rev_extract if 'EDP v2' in file][0]
file_rev_edpv4 = [file for file in files_rev_extract if 'EDP v4' in file][0]

oa_df = pd.read_excel(io=file_rev_oa)
marketmix_df = pd.read_excel(io=file_rev_marketmix)
edpv2_df = pd.read_excel(io=file_rev_edpv2)
edpv4_df = pd.read_excel(io=file_rev_edpv4)

oa_df = rename_df_columns_except_prop(oa_df, 'OA')
marketmix_df = rename_df_columns_except_prop(marketmix_df, 'MM')
edpv2_df = rename_df_columns_except_prop(edpv2_df, 'EDPv2')
edpv4_df = rename_df_columns_except_prop(edpv4_df, 'EDPv4')

df_list = [oa_df, marketmix_df, edpv2_df, edpv4_df]
merged_df = reduce(lambda left, right: pd.merge(left, right, on='prop_cd', how='outer'), df_list)
merged_df = merged_df.merge(prop_hierarchy_df, on='prop_cd', how='left')
merged_df['room_nights_OA'] = merged_df['room_nights_OA'].fillna(0)
merged_df['room_nights_MM'] = merged_df['room_nights_MM'].fillna(0)
merged_df['room_nights_EDPv2'] = merged_df['room_nights_EDPv2'].fillna(0)
merged_df['room_nights_EDPv4'] = merged_df['room_nights_EDPv4'].fillna(0)
merged_df['room_rev_usd_OA'] = merged_df['room_rev_usd_OA'].fillna(0)
merged_df['room_rev_usd_MM'] = merged_df['room_rev_usd_MM'].fillna(0)
merged_df['room_rev_usd_EDPv2'] = merged_df['room_rev_usd_EDPv2'].fillna(0)
merged_df['room_rev_usd_EDPv4'] = merged_df['room_rev_usd_EDPv4'].fillna(0)

merged_df = df_add_rev_var_pct_columns(merged_df)
merged_df = df_sort_columns(merged_df, col_sort)
merged_df['room_rev_usd_var_pct_MM_vs_OA_distribution'] = merged_df.apply(
    lambda x: cfx.get_rev_var_pct_distribution(x['room_rev_usd_var_pct_MM_vs_OA']), axis=1)
merged_df['room_rev_usd_var_pct_EDPv2_vs_OA_distribution'] = merged_df.apply(
    lambda x: cfx.get_rev_var_pct_distribution(x['room_rev_usd_var_pct_EDPv2_vs_OA']), axis=1)
merged_df['room_rev_usd_var_pct_EDPv4_vs_OA_distribution'] = merged_df.apply(
    lambda x: cfx.get_rev_var_pct_distribution(x['room_rev_usd_var_pct_EDPv4_vs_OA']), axis=1)
merged_df['room_rev_usd_var_pct_EDPv2_vs_MM_distribution'] = merged_df.apply(
    lambda x: cfx.get_rev_var_pct_distribution(x['room_rev_usd_var_pct_EDPv2_vs_MM']), axis=1)
merged_df['room_rev_usd_var_pct_EDPv4_vs_MM_distribution'] = merged_df.apply(
    lambda x: cfx.get_rev_var_pct_distribution(x['room_rev_usd_var_pct_EDPv4_vs_MM']), axis=1)
merged_df['room_rev_usd_var_pct_EDPv4_vs_EDPv2_distribution'] = merged_df.apply(
    lambda x: cfx.get_rev_var_pct_distribution(x['room_rev_usd_var_pct_EDPv4_vs_EDPv2']), axis=1)

sheet_format_dict = {}
sheet_format_dict['prop_cd'] = [{'#,##0': [5, 6, 7, 8, 9, 10, 11, 12], '0.0%': [13, 14, 15, 16, 17, 18]}, 2, None]

cfx.delete_file_if_exists(file_result)
cfx.create_excel_workbook(file_result)
cfx.append_df_to_existing_excel_workbook(merged_df, file_result, 'prop_cd')

mm_vs_oa_dist_df = merged_df.groupby(['room_rev_usd_var_pct_MM_vs_OA_distribution']).agg(
    distribution_count = ('room_nights_OA', 'count')
).reset_index()
mm_vs_oa_dist_df['room_rev_usd_var_pct_MM_vs_OA_distribution'] = pd.Categorical(
    mm_vs_oa_dist_df['room_rev_usd_var_pct_MM_vs_OA_distribution'], categories=dist_sort
)
mm_vs_oa_dist_df = mm_vs_oa_dist_df.sort_values(by='room_rev_usd_var_pct_MM_vs_OA_distribution').copy()
mm_vs_oa_dist_df['distribution_count_%_total'] = mm_vs_oa_dist_df.apply(lambda x: x['distribution_count'] / len(merged_df), axis=1)
sheet_format_dict['MM_vs_OA_var_pct_dist'] = [{'#,##0': [2], '0.0%': [3]}, 2, None]
cfx.append_df_to_existing_excel_workbook(mm_vs_oa_dist_df, file_result, 'MM_vs_OA_var_pct_dist')

edpv2_vs_oa_dist_df = merged_df.groupby(['room_rev_usd_var_pct_EDPv2_vs_OA_distribution']).agg(
    distribution_count = ('room_nights_OA', 'count')
).reset_index()
edpv2_vs_oa_dist_df['room_rev_usd_var_pct_EDPv2_vs_OA_distribution'] = pd.Categorical(
    edpv2_vs_oa_dist_df['room_rev_usd_var_pct_EDPv2_vs_OA_distribution'], categories=dist_sort
)
edpv2_vs_oa_dist_df = edpv2_vs_oa_dist_df.sort_values(by='room_rev_usd_var_pct_EDPv2_vs_OA_distribution').copy()
edpv2_vs_oa_dist_df['distribution_count_%_total'] = edpv2_vs_oa_dist_df.apply(lambda x: x['distribution_count'] / len(merged_df), axis=1)
sheet_format_dict['EDPv2_vs_OA_var_pct_dist'] = [{'#,##0': [2], '0.0%': [3]}, 2, None]
cfx.append_df_to_existing_excel_workbook(edpv2_vs_oa_dist_df, file_result, 'EDPv2_vs_OA_var_pct_dist')

edpv4_vs_oa_dist_df = merged_df.groupby(['room_rev_usd_var_pct_EDPv4_vs_OA_distribution']).agg(
    distribution_count = ('room_nights_OA', 'count')
).reset_index()
edpv4_vs_oa_dist_df['room_rev_usd_var_pct_EDPv4_vs_OA_distribution'] = pd.Categorical(
    edpv4_vs_oa_dist_df['room_rev_usd_var_pct_EDPv4_vs_OA_distribution'], categories=dist_sort
)
edpv4_vs_oa_dist_df = edpv4_vs_oa_dist_df.sort_values(by='room_rev_usd_var_pct_EDPv4_vs_OA_distribution').copy()
edpv4_vs_oa_dist_df['distribution_count_%_total'] = edpv4_vs_oa_dist_df.apply(lambda x: x['distribution_count'] / len(merged_df), axis=1)
sheet_format_dict['EDPv4_vs_OA_var_pct_dist'] = [{'#,##0': [2], '0.0%': [3]}, 2, None]
cfx.append_df_to_existing_excel_workbook(edpv4_vs_oa_dist_df, file_result, 'EDPv4_vs_OA_var_pct_dist')

edpv2_vs_mm_dist_df = merged_df.groupby(['room_rev_usd_var_pct_EDPv2_vs_MM_distribution']).agg(
    distribution_count = ('room_nights_OA', 'count')
).reset_index()
edpv2_vs_mm_dist_df['room_rev_usd_var_pct_EDPv2_vs_MM_distribution'] = pd.Categorical(
    edpv2_vs_mm_dist_df['room_rev_usd_var_pct_EDPv2_vs_MM_distribution'], categories=dist_sort
)
edpv2_vs_mm_dist_df = edpv2_vs_mm_dist_df.sort_values(by='room_rev_usd_var_pct_EDPv2_vs_MM_distribution').copy()
edpv2_vs_mm_dist_df['distribution_count_%_total'] = edpv2_vs_mm_dist_df.apply(lambda x: x['distribution_count'] / len(merged_df), axis=1)
sheet_format_dict['EDPv2_vs_MM_var_pct_dist'] = [{'#,##0': [2], '0.0%': [3]}, 2, None]
cfx.append_df_to_existing_excel_workbook(edpv2_vs_mm_dist_df, file_result, 'EDPv2_vs_MM_var_pct_dist')

edpv4_vs_mm_dist_df = merged_df.groupby(['room_rev_usd_var_pct_EDPv4_vs_MM_distribution']).agg(
    distribution_count = ('room_nights_OA', 'count')
).reset_index()
edpv4_vs_mm_dist_df['room_rev_usd_var_pct_EDPv4_vs_MM_distribution'] = pd.Categorical(
    edpv4_vs_mm_dist_df['room_rev_usd_var_pct_EDPv4_vs_MM_distribution'], categories=dist_sort
)
edpv4_vs_mm_dist_df = edpv4_vs_mm_dist_df.sort_values(by='room_rev_usd_var_pct_EDPv4_vs_MM_distribution').copy()
edpv4_vs_mm_dist_df['distribution_count_%_total'] = edpv4_vs_mm_dist_df.apply(lambda x: x['distribution_count'] / len(merged_df), axis=1)
sheet_format_dict['EDPv4_vs_MM_var_pct_dist'] = [{'#,##0': [2], '0.0%': [3]}, 2, None]
cfx.append_df_to_existing_excel_workbook(edpv4_vs_mm_dist_df, file_result, 'EDPv4_vs_MM_var_pct_dist')

edpv4_vs_edpv2_dist_df = merged_df.groupby(['room_rev_usd_var_pct_EDPv4_vs_EDPv2_distribution']).agg(
    distribution_count = ('room_nights_OA', 'count')
).reset_index()
edpv4_vs_edpv2_dist_df['room_rev_usd_var_pct_EDPv4_vs_EDPv2_distribution'] = pd.Categorical(
    edpv4_vs_edpv2_dist_df['room_rev_usd_var_pct_EDPv4_vs_EDPv2_distribution'], categories=dist_sort
)
edpv4_vs_edpv2_dist_df = edpv4_vs_edpv2_dist_df.sort_values(by='room_rev_usd_var_pct_EDPv4_vs_EDPv2_distribution').copy()
edpv4_vs_edpv2_dist_df['distribution_count_%_total'] = edpv4_vs_edpv2_dist_df.apply(lambda x: x['distribution_count'] / len(merged_df), axis=1)
sheet_format_dict['EDPv4_vs_EDPv2_var_pct_dist'] = [{'#,##0': [2], '0.0%': [3]}, 2, None]
cfx.append_df_to_existing_excel_workbook(edpv4_vs_edpv2_dist_df, file_result, 'EDPv4_vs_EDPv2_var_pct_dist')

# country
country_df = merged_df.groupby(['op_area_level2_desc', 'country_cd', 'country_desc']) [col_measure].sum().reset_index()
country_df = df_add_rev_var_pct_columns(country_df)
country_df = df_sort_columns(country_df, col_sort)
sheet_format_dict['country'] = [{'#,##0': [4, 5, 6, 7, 8, 9, 10, 11], '0.0%': [12, 13, 14, 15, 16, 17]}, 2, None]
cfx.append_df_to_existing_excel_workbook(country_df, file_result, 'country')

# op_area_level2_desc
op_area_level2_desc_df = country_df.groupby(['op_area_level2_desc']) [col_measure].sum().reset_index()
op_area_level2_desc_df = df_add_rev_var_pct_columns(op_area_level2_desc_df)
op_area_level2_desc_df = df_sort_columns(op_area_level2_desc_df, col_sort)
sheet_format_dict['op_area_level2_desc'] = [{'#,##0': [2, 3, 4, 5, 6, 7, 8, 9], '0.0%': [10, 11, 12, 13, 14, 15]}, 2, None]
cfx.append_df_to_existing_excel_workbook(op_area_level2_desc_df, file_result, 'op_area_level2_desc')





# clean workbook
wb = openpyxl.load_workbook(filename=file_result)
cfx.format_excel_data(wb, sheet_format_dict, file_result)
time.sleep(4)
cfx.clean_excel_file(wb, file_result)
time.sleep(4)
wb.close()

# cfx.write_df_to_excel(merged_df, file_temp, '1')
os.startfile(file_result)
