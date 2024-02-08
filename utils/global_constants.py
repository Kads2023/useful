from datetime import datetime

date_str = datetime.now().strftime("%Y%m%d")
date_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
run_date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
run_id = datetime.now().strftime("%Y%m%d%H%M%S")

default_run_date = datetime.now().strftime("%Y-%m-%d")

config_file_dir = "{home_dir}/validation_framework/" \
                  "{tenant}_validate/conf/"

config_file_location = "{home_dir}/validation_framework/" \
                       "{tenant}_validate/conf/config.json"

config_file_name = "config.json"

keys_file_location = "{home_dir}/validation_framework/" \
                     "{tenant}_validate/conf/keys.json"

skip_job_file = "SKIP_JOBS_FILE"
user_dir = "/{tenant}_validate"

base_path = "{home_dir}/validation_framework"
common_base_path = "{base_path}/common_validate"
xp_base_path = "{base_path}/{tenant}_validate"

default_timezone = "PST8PDT"
default_timestamp = "19700101000000"
default_run_id_format = "%Y%m%d%H%M%S"
timestamp_standard_format = "%Y-%m-%dT%H:%M:%S.%fZ"

default_comparison_type = ""
default_where_clause_to_be_used_for_counts = "False"
default_convert_column_names_to_lower_case = "False"

display_credentials = "False"
run_summary_only = "False"

prinmary_key_concatenator = ", '|', "

common_section = "COMMON_CONFIG"
base_path_key_name = "BASE_PATH"
common_base_path_key_name = "COMMON_BASE_PATH"
xp_base_path_key_name = "XP_BASE_PATH"
keys_dir_key_name = "KEYS_FILE"
log_file_path_key_name = "LOG_PATH"
log_mode = "LOG_MODE"
log_file_pattern_key_name = "LOG_PATTERN"
log_handlers_key_name = "LOG_HANDLERS"
km_end_point = "KM_ENDPOINT"
sub_validate_type_key_name = "SUB_VALIDATE_TYPE"
sample_data_summary_results_key_name = "SAMPLE_DATA_SUMMARY_RESULTS"
sample_data_row_missing_results_key_name = "SAMPLE_DATA_ROW_MISSING_RESULTS"
sample_data_column_mismatch_results_key_name = "SAMPLE_DATA_COLUMN_MISMATCH_RESULTS"
count_summary_results_key_name = "COUNT_SUMMARY_RESULTS"
count_details_results_key_name = "COUNT_DETAILS_RESULTS"
default_error_if_partition_column_has_null = "True"

default_partition_keys_array_values = "TIMESTAMP('1970-01-01')"

default_sample_record_counts = 100
default_limit_sample = "100000"
default_allow_datatype_mapping = "False"

daily_schedule = 1
weekly_schedule = 7
