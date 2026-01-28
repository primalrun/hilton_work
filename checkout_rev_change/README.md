these python scripts were designed to compare checkout revenue logic results at different hierarchy levels from prop_cd to total (all hotels)
can be used to compare checkout revenue logic results for all charge_category values including (room, food, beverage, misc, package, phone, shop, tax)

shared scripts
  **config.py**
    used to pick type of revenue (charge_category), for example, 'room' for room revenue
    can set evaluation date range to consider
  **common_functions.py**
    modular functions used in different scripts

**prop_level_create_csv_extract.py**
  query redshift
  for each prop_cd (property), get room_nights and checkout revenue for specific charge_category set in config.py file
  output is csv file
  can be run for each checkout revenue logic

**comparison_prop_total_excel_extract.py**
  config_file.py
    set charge_category
    set version_compare_from
    set version_compare_to

  script will produce excel file
  comparing room nights and checkout revenue between comparison logic versions
  comparison levels
    prop_cd (hotel level)
    country
    op_area_level2_desc (global region)
    total (all hotels)
    
**comparison_prop_specific_excel_extract.py**
  can do deeper analysis for specific prop_cd values, multiple values allowed
  populate excel file prop_cd to compare.xlsx with specific properties you want to evaluate all reservation comparisons
  script will loop through all specific properties in prop_cd to compare.xlsx file
  will return excel file with reservation level checkout revenue amounts and variances for every logic available

**comparison_stay_specific_excel_extract.py**
  can do deeper analysis for specific reservations for specific property, multiple values allowed
  populate excel file stay_id to compare.xlsx with specific property reservations
  script will loop through all specific property reservations in stay_id to compare.xlsx file
  will return excel file with all relevant checkout transaction details amounts along with which revenue logic each transaction would include or exclude


  
