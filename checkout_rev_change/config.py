import os
import common_functions as cfx
import pandas as pd
import configparser

charge_category = 'room'
date_logic = 'arrival' # arrival, stay
period_load_yyyymm = 202506
version_extract = 'EDP v4'

period_compare_start_yyyymmdd =  '20250601'
period_compare_end_yyyymmdd =  '20250630'
version_compare_from = 'EDP v2'
version_compare_to = 'EDP v4'

# file naming convention
# (prop) level arrival date EDP v1
# <level> <date logic> <version>
# (prop) level arrival date (20250601-20250630) EDP v1.csv
# <level> <date logic> <date range> <version>

file_config = r'C:\Users\jwalker221\OneDrive - Hilton\Documents\cred.ini'
config = configparser.ConfigParser()
config.read(file_config)
dw_prod_config = config['dw_prod']
host_prod = dw_prod_config['host']
dbname_prod = dw_prod_config['dbname']
user_prod = dw_prod_config['user']
password_prod = dw_prod_config['password']

rs_cred = [
    host_prod
    ,dbname_prod
    ,user_prod
    ,password_prod
]

date_start, date_end, date_start_str, date_end_str, date_start_yyyymmdd, date_end_yyyymmdd = cfx.convert_period_yyyymm_int_to_start_date_and_end_date(period_load_yyyymm)

dir_main = r'C:\Users\jwalker221\OneDrive - Hilton\Jira\Checkout Revenue Changes'
dir_sql_path_folders = [dir_main, charge_category, 'SQL', 'dynamic']
dir_sql_in = os.path.join(*dir_sql_path_folders)
file_sql_in_prop_level_name = f'(prop) level {date_logic} date {version_extract}.sql'
file_sql_in_prop_level_path = os.path.join(dir_sql_in, file_sql_in_prop_level_name)
file_in_sql_prop_specific_checkout_comparison_name = 'prop specific checkout comparison.sql'
file_in_sql_prop_specific_checkout_comparison_path = os.path.join(dir_sql_in, file_in_sql_prop_specific_checkout_comparison_name)
file_in_sql_stay_specific_checkout_comparison_name = 'stay specific checkout comparison.sql'
file_in_sql_stay_specific_checkout_comparison_path = os.path.join(dir_sql_in, file_in_sql_stay_specific_checkout_comparison_name)

dir_extract_path_folders = [dir_main, charge_category, 'extract']
dir_extract_out = os.path.join(*dir_extract_path_folders)
file_extract_out_prop_level_name = f'(prop) level {date_logic} date ({date_start_yyyymmdd}-{date_end_yyyymmdd}) {version_extract}.csv'
file_extract_out_prop_level_path = os.path.join(dir_extract_out, file_extract_out_prop_level_name)

file_in_prop_level_comparison_from_name = f"(prop) level {date_logic} date ({period_compare_start_yyyymmdd}-{period_compare_end_yyyymmdd}) {version_compare_from}.csv"
file_in_prop_level_comparison_to_name = f"(prop) level {date_logic} date ({period_compare_start_yyyymmdd}-{period_compare_end_yyyymmdd}) {version_compare_to}.csv"
file_in_prop_level_comparison_from_path = os.path.join(dir_extract_out, file_in_prop_level_comparison_from_name)
file_in_prop_level_comparison_to_path = os.path.join(dir_extract_out, file_in_prop_level_comparison_to_name)

dir_comparison_path_folders = [dir_main, charge_category, 'comparison']
dir_comparison_out = os.path.join(*dir_comparison_path_folders)
dir_comparison_prop_total_out = os.path.join(dir_comparison_out, 'prop_total')
dir_comparison_prop_specific_out = os.path.join(dir_comparison_out, 'prop_specific')
dir_comparison_stay_specific_out = os.path.join(dir_comparison_out, 'stay_specific')
file_comparison_prop_total_name = f"(prop) level {date_logic} date ({date_start_yyyymmdd}-{date_end_yyyymmdd}) {version_compare_to} vs. {version_compare_from}.xlsx"
file_comparison_prop_total_path = os.path.join(dir_comparison_prop_total_out, file_comparison_prop_total_name)
file_in_prop_specific_name = 'prop_cd to compare.xlsx'
file_in_prop_specific_path = os.path.join(dir_comparison_prop_specific_out, file_in_prop_specific_name)
file_in_stay_specific_name = 'stay_id to compare.xlsx'
file_in_stay_specific_path = os.path.join(dir_comparison_stay_specific_out, file_in_stay_specific_name)

comparison_columns = [
    'op_area_level2_desc'
    ,'country_cd'
    ,'country_desc'
    ,'prop_cd'
    ,f"room nights {version_compare_to}"
    ,f"room nights {version_compare_from}"
    ,'room nights var amt'
    ,'room nights var pct'
    ,f"{charge_category} rev {version_compare_to}"
    ,f"{charge_category} rev {version_compare_from}"
    ,f"{charge_category} rev var amt"
    ,f"{charge_category} rev var pct"
]


dict_filter_logic = {
    'misc': {
        'EDP v1': '(current): charge_category = misc'
        , 'EDP v2': '(new): charge_category = misc, entry_type = (CHARGE, ADJUST, ALLOWE), posting_type = P, charge_routed_ind <> true'
        , 'EDP v3': '(new): charge_category = misc, entry_type = (CHARGE, ADJUST, ALLOWE), posting_type = P'
        , 'EDP v4': '(new): GEP checkout revenue logic'
    }
    ,'food': {
        'EDP v1': '(current): charge_category = food'
        ,
        'EDP v2': '(new): charge_category = food, entry_type = (CHARGE, ADJUST, ALLOWE), posting_type = P, charge_routed_ind <> true'
        , 'EDP v3': '(new): charge_category = food, entry_type = (CHARGE, ADJUST, ALLOWE), posting_type = P'
        , 'EDP v4': '(new): GEP checkout revenue logic'
    }
    , 'tax': {
        'EDP v1': '(current): charge_category = tax'
        ,
        'EDP v2': '(new): charge_category = tax, entry_type = (CHARGE, ADJUST, ALLOWE), posting_type = P, charge_routed_ind <> true'
        , 'EDP v3': '(new): charge_category = tax, entry_type = (CHARGE, ADJUST, ALLOWE), posting_type = P'
        , 'EDP v4': '(new): GEP checkout revenue logic'
    }
    , 'beverage': {
        'EDP v1': '(current): charge_category = beverage'
        ,
        'EDP v2': '(new): charge_category = beverage, entry_type = (CHARGE, ADJUST, ALLOWE), posting_type = P, charge_routed_ind <> true'
        , 'EDP v3': '(new): charge_category = beverage, entry_type = (CHARGE, ADJUST, ALLOWE), posting_type = P'
        , 'EDP v4': '(new): GEP checkout revenue logic'
    }
   , 'package': {
        'EDP v1': '(current): charge_category = package'
        ,
        'EDP v2': '(new): charge_category = package, entry_type = (CHARGE, ADJUST, ALLOWE), posting_type = P, charge_routed_ind <> true'
        , 'EDP v3': '(new): charge_category = package, entry_type = (CHARGE, ADJUST, ALLOWE), posting_type = P'
        , 'EDP v4': '(new): GEP checkout revenue logic'
    }
    , 'room': {
        'EDP v1': '(current): charge_category = room'
        ,
        'EDP v2': '(new): charge_category = room, entry_type = (CHARGE, ADJUST, ALLOWE), posting_type = P, charge_routed_ind <> true'
        , 'EDP v3': '(new): charge_category = room, entry_type = (CHARGE, ADJUST, ALLOWE), posting_type = P'
        , 'EDP v4': '(new): GEP checkout revenue logic'
    }
}

dict_sql_prop_level_replacement = {
    'misc': {
        'EDP v1': {
             'date_start_variable':  date_start_str
            , 'date_end_variable': date_end_str
        }
        ,'EDP v2': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v3': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v4': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
    }
    ,'food': {
        'EDP v1': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v2': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v3': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v4': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
    }
    , 'tax': {
        'EDP v1': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v2': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v3': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v4': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
    }
    , 'beverage': {
        'EDP v1': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v2': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v3': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v4': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
    }
    , 'package': {
        'EDP v1': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v2': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v3': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v4': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
    }
    , 'room': {
        'EDP v1': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v2': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v3': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
        , 'EDP v4': {
            'date_start_variable': date_start_str
            , 'date_end_variable': date_end_str
        }
    }
}

dict_sql_prop_specific_replacement = {
    'misc': {'date_start_variable':  date_start_str
            , 'date_end_variable': date_end_str
            , 'prop_cd_variable': ''
        }
    ,'food': {'date_start_variable': date_start_str
        , 'date_end_variable': date_end_str
        , 'prop_cd_variable': ''
             }
    , 'tax': {'date_start_variable': date_start_str
        , 'date_end_variable': date_end_str
        , 'prop_cd_variable': ''
               }
    , 'beverage': {'date_start_variable': date_start_str
        , 'date_end_variable': date_end_str
        , 'prop_cd_variable': ''
              }
   , 'package': {'date_start_variable': date_start_str
        , 'date_end_variable': date_end_str
        , 'prop_cd_variable': ''
              }
    , 'room': {'date_start_variable': date_start_str
        , 'date_end_variable': date_end_str
        , 'prop_cd_variable': ''
                  }
}

dict_excel_format_prop_specific = {
    'misc': [{'#,##0': [3, 4, 5, 6, 7, 9, 11, 13], '0.0%': [8, 10, 12, 14]}, 2, None]
    ,'food': [{'#,##0': [3, 4, 5, 6, 7, 9, 11, 13], '0.0%': [8, 10, 12, 14]}, 2, None]
    ,'tax': [{'#,##0': [3, 4, 5, 6, 7, 9, 11, 13], '0.0%': [8, 10, 12, 14]}, 2, None]
    , 'beverage': [{'#,##0': [3, 4, 5, 6, 7, 9, 11, 13], '0.0%': [8, 10, 12, 14]}, 2, None]
    , 'package': [{'#,##0': [3, 4, 5, 6, 7, 9, 11, 13], '0.0%': [8, 10, 12, 14]}, 2, None]
    , 'room': [{'#,##0': [3, 4, 5, 6, 7, 9, 11, 13], '0.0%': [8, 10, 12, 14]}, 2, None]
}

dict_sql_stay_specific_replacement = {
    'misc': {'date_start_variable':  date_start_str
            , 'date_end_variable': date_end_str
            , 'prop_cd_variable': ''
            , 'stay_id_variable': ''
        }
    ,'food': {'date_start_variable': date_start_str
        , 'date_end_variable': date_end_str
        , 'prop_cd_variable': ''
        , 'stay_id_variable': ''
             }
    , 'tax': {'date_start_variable': date_start_str
        , 'date_end_variable': date_end_str
        , 'prop_cd_variable': ''
        , 'stay_id_variable': ''
               }
    , 'beverage': {'date_start_variable': date_start_str
        , 'date_end_variable': date_end_str
        , 'prop_cd_variable': ''
        , 'stay_id_variable': ''
              }
    , 'package': {'date_start_variable': date_start_str
        , 'date_end_variable': date_end_str
        , 'prop_cd_variable': ''
        , 'stay_id_variable': ''
              }
    , 'room': {'date_start_variable': date_start_str
        , 'date_end_variable': date_end_str
        , 'prop_cd_variable': ''
        , 'stay_id_variable': ''
                  }
}


dict_excel_format_stay_specific = {
    'misc': [{'#,##0.00': [20, 21]}, 2, None]
    ,'food': [{'#,##0.00': [20, 21]}, 2, None]
    ,'tax': [{'#,##0.00': [20, 21]}, 2, None]
    , 'beverage': [{'#,##0.00': [20, 21]}, 2, None]
    , 'package': [{'#,##0.00': [20, 21]}, 2, None]
    , 'room': [{'#,##0.00': [20, 21]}, 2, None]
}


version_filter_logic = dict_filter_logic[charge_category]
dict_version = {}
version_names = list(version_filter_logic.keys())
filter_logic = list(version_filter_logic.values())
dict_version['version'] = version_names
dict_version['filter logic'] = filter_logic
df_version = pd.DataFrame(data=dict_version)
