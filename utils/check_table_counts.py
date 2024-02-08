




import os
import sys

import common_functions as utils
import gcp_utils

from google.cloud import bigquery

env = os.getenv('ENVIRONMENT')
home_dir = os.getenv('HOME')


def execute_bq_query(bq_client, query, project):
    query_job_config = bigquery.QueryJobConfig()
    query_job = bq_client.query(query, project=project, job_config=query_job_config)
    print("Started job {}".format(query_job.job_id))
    job_result = query_job.result()
    results = 0
    for each in job_result:
        results = each
    return results


xp = ""

prop_home = f""
km_credentials = f""
gcs_bucket_name = f""
gcs_project = ""
bq_dataset = f"{xp}_raw_tables"
bq_project = ""


def _list_tables():
    tables = []
    tables_list = bq_client.list_tables(dataset=f'{bq_project}.{bq_dataset}')
    for table in tables_list:
        tables.append(table.reference.table_id)
    return tables

list_of_tables = _list_tables()

keymaker_response = utils.get_keymaker_api_response('https://keymakerapi.g.paypalinc.com:21358/kmsapi/v1/keyobject/all'
                                                    ,f'{home_dir}/{prop_home}/common/dependency-files/identity.txt')

gcs_credential_json = utils.get_keymaker_key(keymaker_response, km_credentials)
bq_client = gcp_utils.get_bq_client(gcs_credential_json)

keys_dir = home_dir + '/' + prop_home + '/common/dependency-files/keys.json'
keys = utils.get_keys(keys_dir)


snapshot_tables_with_count_zero = []
raw_tables_with_count_zero = []
raw_table_not_found = []
raw_only_table = []
table_counts = {}
primary_key_not_available = []


print(f"list_of_tables {len(list_of_tables)}")
for table in list_of_tables:
    table_details = {}
    table_keys = keys[f"{table}_raw"]
    primary_key = str(table_keys.get("PRIMARY_KEYS", "")).strip()
    primary_keys_list = primary_key.split(",")
    if len(primary_keys_list) == 0:
        primary_key_not_available.append(table)
    elif len(primary_keys_list) == 1:
        primary_key_null_check = f"{primary_keys_list[0]} is NULL"
    else:
        primary_key_null_check = ""
        for each in primary_keys_list:
            if primary_key_null_check:
                primary_key_null_check += f" and {each} is NULL"
            else:
                primary_key_null_check += f"{each} is NULL"
    raw_total_records_count = 0
    raw_history_counts = 0
    raw_distinct_pk_counts = 0
    raw_pk_with_null_counts = 0
    try:
        raw_table_name = f'{bq_project}.{bq_dataset}.{table}_raw'
        print(f"getting count for {raw_table_name}")
        raw_total_records_count = bq_client.get_table(table=raw_table_name).num_rows
        print(f"{table}'s raw_total_records_count --> {raw_total_records_count}")
        if raw_total_records_count == 0:
            raw_tables_with_count_zero.append(raw_table_name)
            raw_history_counts = 0
            raw_distinct_pk_counts = 0
            raw_pk_with_null_counts = 0
        else:
            raw_history_counts_query = f"select count(*) from {raw_table_name} where created_local_time = TIMESTAMP('2022-01-01 00:00:00')"
            raw_history_counts = execute_bq_query(bq_client, raw_history_counts_query, gcs_project)
            print(f"{table}'s raw_history_counts --> {raw_history_counts}")
            if primary_key:
                raw_distinct_pk_counts = execute_bq_query(bq_client, f"select count(*) from (select distinct {primary_key} from {raw_table_name})", gcs_project)
                print(f"{table}'s raw_distinct_pk_counts --> {raw_distinct_pk_counts}")
                raw_pk_with_null_counts = execute_bq_query(bq_client, f"select count(*) from {raw_table_name} where {primary_key_null_check}", gcs_project)
                print(f"{table}'s raw_pk_with_null_counts --> {raw_pk_with_null_counts}")
    except:
        raw_table_not_found.append(table)
    table_details["RAW_TOTAL_COUNT"] = str(raw_total_records_count)
    table_details["RAW_HISTORY_COUNT"] = str(raw_history_counts)
    table_details["RAW_DISTINCT_PRIMARY_KEY_COUNT"] = str(raw_distinct_pk_counts)
    table_details["RAW_PRIMARY_KEY_NULL_COUNT"] = str(raw_pk_with_null_counts)
    try:
        snapshot_table_name = f'{bq_project}.{bq_dataset}.{table}'
        print(f"getting count for {snapshot_table_name}")
        snapshot_total_records_count = bq_client.get_table(table=snapshot_table_name).num_rows
        print(f"{table}'s snapshot_total_records_count --> {snapshot_total_records_count}")
        table_details["SNAPSHOT_TOTAL_COUNT"] = str(snapshot_total_records_count)
        if snapshot_total_records_count == 0:
            snapshot_tables_with_count_zero.append(snapshot_table_name)
            snapshot_distinct_pk_counts = 0
            snapshot_pk_with_null_counts = 0
        else:
            snapshot_distinct_pk_counts = 0
            snapshot_pk_with_null_counts = 0
            if primary_key:
                snapshot_distinct_pk_counts = execute_bq_query(bq_client, f"select count(*) from (select distinct {primary_key} from {snapshot_table_name})", gcs_project)
                print(f"{table}'s snapshot_distinct_pk_counts --> {snapshot_distinct_pk_counts}")
                snapshot_pk_with_null_counts = execute_bq_query(bq_client, f"select count(*) from {snapshot_table_name} where {primary_key_null_check}", gcs_project)
                print(f"{table}'s snapshot_pk_with_null_counts --> {snapshot_pk_with_null_counts}")
        table_details["SNAPSHOT_DISTINCT_PRIMARY_KEY_COUNT"] = str(snapshot_distinct_pk_counts)
        table_details["SNAPSHOT_PRIMARY_KEY_NULL_COUNT"] = str(snapshot_pk_with_null_counts)
        table_details["TABLE_TYPE"] = "RAW_AND_SNAPSHOT"
    except:
        raw_only_table.append(table)
        table_details["SNAPSHOT_TOTAL_COUNT"] = str(0)
        table_details["SNAPSHOT_DISTINCT_PRIMARY_KEY_COUNT"] = str(0)
        table_details["SNAPSHOT_PRIMARY_KEY_NULL_COUNT"] = str(0)
        table_details["TABLE_TYPE"] = "RAW_ONLY"
    table_counts[table] = table_details
    print(f"len(table_counts) --> {len(table_counts)}, len(table_details) --> {len(table_details)}, table_details --> {table_details}")


print(f"len(primary_key_not_available) --> {len(primary_key_not_available)}, "
      f"primary_key_not_available --> {primary_key_not_available}, "
      f"len(raw_tables_with_count_zero) --> {len(raw_tables_with_count_zero)}, "
      f"raw_tables_with_count_zero --> {raw_tables_with_count_zero}, "
      f"len(snapshot_tables_with_count_zero) --> {len(snapshot_tables_with_count_zero)}, "
      f"snapshot_tables_with_count_zero --> {snapshot_tables_with_count_zero}, "
      f"len(raw_only_table) --> {len(raw_only_table)}, "
      f"raw_only_table --> {raw_only_table}, "
      f"len(table_counts) --> {len(table_counts)}, "
      f"table_counts --> {table_counts}, ")


for table in list_of_tables:
    table_name = f'{bq_project}.{bq_dataset}.{table}'
    print(f"table going to delete {table_name}")
    bq_client.delete_table(table=table_name)
