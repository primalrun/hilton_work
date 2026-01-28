**config.py**
set variables to be used in other scripts

**common_functions.py**
modular design functions to be used in multiple scripts

**fornova_data_dict_create_extract.py**
take data dictionary excel file as input
clean and normalize data types and reporting inclusion/exclusion flags

**data_profile_create_extract.py**
can process 33 GB csv file without error by using csv scanning and lazy frame collection
reads source file, csv or excel
converts source file from excel to csv if needed
reads source data into polars lazyframe
collects data profile calculations and writes results to excel file 
data type, field name
row count, blank string count, null count, distinct value count
min character length, max character length, max octet length, avg character length
value with min character length, value with max character length, value with max octet length, value with avg character length
min value, max value
count of values with leading whitespace, count of values with trailing whitespace

**data_profile_combine_profile_results.py**
combines results for the same data subject
for example, Compliance is a data subject and has 4 files that were processed for data profiling
this process will compare results of all 4 data profiles and pick the relevant value for each file
for example, a count would be added together
another example, a max varchar field would be selected

**data_profile_create_final_result.py**
will create final data profile results
should be run for each data subject
gets profiling results from data subject totals excel file
adds data types for source, target, and target with a 20% buffer
adds columns for data dictionary attributes
this final file can be used to build DDL, create source to target, and populate input template for data catalog
