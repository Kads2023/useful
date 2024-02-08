import json
import datetime
import pytz
import base64
import requests
import subprocess
from subprocess import PIPE
import pgpy

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import global_constants

import re
import os
import socket
import struct
import pickle
import cx_Oracle

default_merge_query_type = "ARRAY_AGG"
merge_query_type_options = ["ARRAY_AGG", "ROW_NUM"]


def parse_ora(tns_entry):
    tns = ''
    tnsnames_path = os.environ.get('TNS_ADMIN') + "/tnsnames.ora"
    tnsnames_fp = open(tnsnames_path, "r")
    for line in tnsnames_fp:
        tns = tns + line
    tnsnames_fp.close()

    cleaned_tns = re.sub(r'#[^\n]*\n', '\n', tns)  # Remove comments
    cleaned_tns = re.sub(r'( *\n *)+', '\n', cleaned_tns.strip())  # Remove empty lines
    cleaned_tns = cleaned_tns.replace(' ', '')  # Remove spaces
    tns_lines = cleaned_tns.splitlines()

    # Find start index of passed tns_entry
    index = 0
    while index < len(tns_lines):
        if tns_lines[index].startswith(tns_entry):
            break
        else:
            index += 1

    is_key = True
    is_value = False
    parentheses_count = 0
    connection_string = ''

    # Iterate character by character up-to end of tns entry
    while index < len(tns_lines):
        for character in tns_lines[index]:
            if is_key:
                if character == '(':
                    is_key = False
                    is_value = True
                    parentheses_count += 1
                    connection_string += character
            else:
                if character == '(':
                    parentheses_count += 1
                elif character == ')':
                    parentheses_count -= 1
                connection_string += character
            if is_value and parentheses_count == 0:
                break
        if is_value and parentheses_count == 0:
            break
        else:
            index += 1

    # Remove Failover block (FAILOVER_MODE=...()()) if exists
    index_of_failover = connection_string.find('(FAILOVER_MODE=')
    if index_of_failover >= 0:
        temp_connection_string = connection_string
        connection_string = temp_connection_string[0:index_of_failover]
        parentheses_count = 1
        index = index_of_failover + 1
        while index <= len(temp_connection_string) and parentheses_count > 0:
            if temp_connection_string[index] == '(':
                parentheses_count += 1
            elif temp_connection_string[index] == ')':
                parentheses_count -= 1
            index += 1
        connection_string += temp_connection_string[index:]

    return connection_string


def send_data(conn, data):
    serialized_data = pickle.dumps(data)
    serialized_data_length = len(serialized_data)
    conn.sendall(struct.pack('>I', serialized_data_length))
    conn.sendall(serialized_data)


def get_data_length(conn):
    data_size = struct.unpack('>I', conn.recv(4))[0]
    return data_size


def receive_data(conn):
    data_length = get_data_length(conn)
    data = conn.recv(data_length)
    data = pickle.loads(data)
    return data


def execute_connection_pool_query(etlafm_connection_dict, query, query_params_dict):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(
        (etlafm_connection_dict['connection_pool_socket_host'],
         etlafm_connection_dict['connection_pool_socket_port']))
    data = {
        "query": query,
        "query_params": query_params_dict
    }
    send_data(s, data)
    s.shutdown(socket.SHUT_WR)
    data = receive_data(s)
    s.close()

    return data


def get_config(config_dir):
    # TODO: Read from GCS
    with open('{}'.format(config_dir)) as config:
        config = json.load(config)
    config.close()
    return config


def get_keys(keys_dir, key_name=None):
    if keys_dir:
        with open(keys_dir) as keys:
            keys = json.load(keys)
            return keys[key_name] if key_name else keys


def epoch_to_utc_date(epoch_time_ms):
    print(f"In epoch_to_utc_date of CommonOperations, epoch_time_ms --> {epoch_time_ms}")
    timestamp = (float(epoch_time_ms) / 1000.0)
    run_id = datetime.datetime.fromtimestamp(timestamp, tz=pytz.timezone('UTC')).strftime('%Y%m%d%H%M%S')
    return run_id


def date_to_epoch(run_id):
    print(f"In date_to_epoch of CommonOperations, run_id --> {run_id}")
    epoch_time_ms = int(
        pytz.timezone('PST8PDT').localize(datetime.datetime.strptime(run_id, '%Y%m%d%H%M%S'),
                                          is_dst=True).astimezone(
            tz=pytz.timezone('PST8PDT')).strftime('%s')) * 1000
    return str(epoch_time_ms)


def check_table(passed_bq_client, passed_bq_table_name):
    try:
        passed_bq_client.get_table(passed_bq_table_name)
        print("Table {} already exists.".format(passed_bq_table_name))
        return True
    except NotFound:
        print("Table {} is not found.".format(passed_bq_table_name))
        return False


def get_bq_job_labels(target_table, label, dataset, end_run_id, passed_run_no):
    name = ""
    short_table_name = [word[:2] for word in target_table.split('_')]
    short_table_name = ''.join(short_table_name).replace("_", "").lower()
    short_table_name_new = [word[:1] for word in target_table.split('_')]
    short_table_name_new = ''.join(short_table_name_new).replace("_", "").lower()
    now_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    ret_val = {
        "job-instance-id": "%s-%s-%s-%s-%s" % (name, label[0], short_table_name_new,
                                               end_run_id[2:], passed_run_no),
        "table_name": "%s-%s" % (name, target_table.lower()),
        "property": "%s" % name,
        "task": "%s-%s" % (name, label),
        "dataset": "%s-%s" % (name, dataset),
        "pp-edp-resource-name": "%s-%s-%s" % (name, dataset, short_table_name),
        "pp-edp-custom-billing-tag": "%s-%s-%s-%s" % (name, label, short_table_name, end_run_id[2:]),
        "pp-edp-job-run-id": "%s-%s-%s-%s-%s" % (name, label[0], short_table_name_new,
                                                 end_run_id[2:], passed_run_no),
        "ingestion-time": "%s-%s" % (name, now_time)
    }
    print(f"labels_now --> {ret_val}")
    return ret_val


def execute_bq_query_table_creation(passed_bq_client, passed_query, passed_gcs_project,
                                    passed_target_table, passed_table_config,
                                    execute_queries=False, passed_labels=None):
    partition_key_name = "PARTITION_FIELD"
    clustering_key_name = "CLUSTERING_FIELDS"
    partition_type_name = "PARTITION_TYPE"

    query_job_config = bigquery.QueryJobConfig(
        destination=passed_target_table
    )
    if partition_key_name in passed_table_config:
        partition_type = passed_table_config[partition_type_name]
        if partition_type != '':
            time_partitioning_opts = bigquery.TimePartitioning()
            time_partitioning_opts.type_ = partition_type
            time_partitioning_opts.field = passed_table_config[partition_key_name]
            query_job_config.time_partitioning = time_partitioning_opts

    if clustering_key_name in passed_table_config:
        clustering_key = passed_table_config[clustering_key_name]
        if clustering_key != '':
            query_job_config.clustering_fields = clustering_key
    print(f"query_job_config --> {query_job_config}, "
          f"passed_query --> {passed_query}, "
          f"execute_queries --> {execute_queries}, "
          f"before executing query --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")
    if execute_queries:
        if passed_labels:
            query_job_config.labels = passed_labels
        query_job = passed_bq_client.query(passed_query,
                                           project=passed_gcs_project,
                                           job_config=query_job_config)
        print("Started job {}".format(query_job.job_id))
        job_result = query_job.result()
    else:
        job_result = None
    print(f"job_result --> {job_result}"
          f"after executing query --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")
    return job_result


def get_table_counts(passed_records_count_query, passed_bq_client,
                     passed_gcs_project, passed_labels=None):
    print(f"records_count_query --> {passed_records_count_query}, "
          f"before getting table count --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")
    query_job_config = bigquery.QueryJobConfig()
    if passed_labels:
        query_job_config.labels = passed_labels
    records_count_job = passed_bq_client.query(passed_records_count_query,
                                               project=passed_gcs_project,
                                               job_config=query_job_config)
    records_count_rows = records_count_job.result()
    records_count = 0
    for records_count_row in records_count_rows:
        records_count = int(records_count_row['records_count'])
    print(f"records_count --> {records_count}, "
          f"after getting table count --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")
    return records_count


def bq_table_add_columns(bq_client, bq_table_name, columns_to_add):
    bq_table = bq_client.get_table(bq_table_name)
    original_schema = bq_table.schema
    updated_schema = original_schema[:]

    for column_name, data_type in columns_to_add.items():
        updated_schema.append(bigquery.SchemaField(column_name, data_type))

    bq_table.schema = updated_schema
    bq_table = bq_client.update_table(bq_table, ["schema"])  # Update schema of existing table

    if len(bq_table.schema) == len(original_schema) + len(columns_to_add) == len(updated_schema):
        print("Table schema has been updated successfully")
    else:
        print("Table schema failed to update")
        raise RuntimeError("Failed to update table schema")


def get_bq_table_schema(passed_bq_client, passed_bq_project, passed_bq_dataset,
                        passed_table_name, passed_gcs_project, passed_labels=None):
    bq_schema = {}
    query = f"SELECT * FROM " \
            f"{passed_bq_project}.{passed_bq_dataset}.INFORMATION_SCHEMA.COLUMNS " \
            f"WHERE TABLE_NAME='{passed_table_name}'"
    print("Query to get column names : {} ".format(query))
    query_job_config = bigquery.QueryJobConfig()
    if passed_labels:
        query_job_config.labels = passed_labels
    query_job = passed_bq_client.query(query,
                                       project=passed_gcs_project,
                                       job_config=query_job_config)
    rows = query_job.result()
    for row in rows:
        bq_schema[row['column_name'].lower()] = row['data_type']
    return bq_schema


def execute_bq_query(passed_bq_client, passed_query, passed_gcs_project, passed_labels=None,
                     with_params=False, passed_start_time=None, passed_end_time=None,
                     partition_type=None, partition_columns=None, clustering_columns=None,
                     load_type=None, gcs_table_id=None, execute_queries=False):
    print(f"inside execute_bq_query, "
          f"passed_gcs_project --> {passed_gcs_project}, "
          f"with_params --> {with_params}, "
          f"passed_start_time --> {passed_start_time}, "
          f"passed_end_time --> {passed_end_time}, "
          f"partition_type --> {partition_type}, "
          f"partition_columns --> {partition_columns}, "
          f"clustering_columns --> {clustering_columns}, "
          f"load_type --> {load_type}, "
          f"gcs_table_id --> {gcs_table_id}, "
          f"execute_queries --> {execute_queries}")
    query_job_config = bigquery.QueryJobConfig()
    if with_params:
        query_job_config.query_parameters = [
                bigquery.ScalarQueryParameter("start_run_id", "STRING", passed_start_time),
                bigquery.ScalarQueryParameter("end_run_id", "STRING", passed_end_time),
            ]
    if partition_type:
        time_partitioning_opts = bigquery.TimePartitioning()
        time_partitioning_opts.type_ = partition_type
        time_partitioning_opts.field = partition_columns
        query_job_config.time_partitioning = time_partitioning_opts
    if clustering_columns:
        if isinstance(clustering_columns, list):
            query_job_config.clustering_fields = clustering_columns
        else:
            query_job_config.clustering_fields = list(map(str.strip, clustering_columns.split(",")))
    if load_type == "FULL_LOAD":
        query_job_config.destination = gcs_table_id
        query_job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    print(f"query_job_config --> {query_job_config}, "
          f"passed_query --> {passed_query}, "
          f"execute_queries --> {execute_queries}, "
          f"before executing query --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")
    if execute_queries:
        if passed_labels:
            query_job_config.labels = passed_labels
        query_job = passed_bq_client.query(passed_query,
                                           project=passed_gcs_project,
                                           job_config=query_job_config,
                                           timeout=86400.00)
        print("Started job {}".format(query_job.job_id))
        job_result = query_job.result()
    else:
        job_result = None
    print(f"job_result --> {job_result}"
          f"after executing query --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")
    return job_result


def execute_cursor_query(orc_db_details, query, query_params_dict):
    print("****EXECUTING CURSOR QUERY***********")
    connection = cx_Oracle.connect(
        '%s@%s' % (orc_db_details.get('credentials'), orc_db_details.get('connection_string')))
    cursor = connection.cursor()
    cursor.execute(query, query_params_dict)
    result_set = cursor.fetchall()
    return result_set


def set_db_details(passed_username, passed_password, passed_connection_string):
    now_orc_db_details = {'credentials': passed_username + "/" + passed_password,
                          'connection_string': parse_ora(passed_connection_string)}
    return now_orc_db_details


def execute_etlafm_query(orc_db_details, etlafm_connection_dict, query, **query_params_dict):
    try:
        if etlafm_connection_dict.get("connection_pooling_enabled", False):
            print("**********USING POOL***************")
            return execute_connection_pool_query(etlafm_connection_dict, query, query_params_dict)
        else:
            print("*******USING CURSOR CONNECTION********")
            return execute_cursor_query(orc_db_details, query, query_params_dict)
    except Exception as e:
        print(f"Exception caused --> {e}")
        print("*******USING CURSOR CONNECTION********")
        return execute_cursor_query(orc_db_details, query, query_params_dict)


def epoch_to_date(epoch_time_ms, passed_timezone=None):
    print(f"In epoch_to_date, "
          f"epoch_time_ms --> {epoch_time_ms}, "
          f"passed_timezone --> {passed_timezone}")
    audit_column_timezone = global_constants.default_timezone
    if passed_timezone:
        audit_column_timezone = passed_timezone
    timestamp = (float(epoch_time_ms) / 1000.0)
    run_id = datetime.datetime.fromtimestamp(
        timestamp,
        tz=pytz.timezone(audit_column_timezone)).strftime(
        global_constants.default_run_id_format)
    print(f"In epoch_to_date, "
          f"epoch_time_ms --> {epoch_time_ms}, "
          f"passed_timezone --> {passed_timezone}, "
          f"audit_column_timezone --> {audit_column_timezone}, "
          f"run_id --> {run_id}")
    return run_id


def get_keymaker_api_response(keymaker_url, encoded_appcontext_path, cac_ert_path=None):
    try:
        # Handle certificate and SSL verification
        if not cac_ert_path:
            cac_ert_path = False

        # Handle keymaker token
        try:
            with open(encoded_appcontext_path) as f:
                token = f.read()
                print("Appcontext file read successfully.")
                token_decoded = base64.b64decode(token)
        except IOError:
            error_msg = f"In get_keymaker_api_response of KeyMakerApiProxy, " \
                        f"can't find file {encoded_appcontext_path}"
            print(error_msg)
            raise ValueError(error_msg)

        # strip newline from token string
        token_decoded = token_decoded.strip()
        url = f"{keymaker_url}?version_hint=last_enabled"
        headers = {"Content-Type": "application/json", "X-KM-APP-CONTEXT": token_decoded}
        response = None
        try:
            session = requests.Session()
            session.trust_env = False
            response = session.get(url, headers=headers, verify=cac_ert_path)
            print(f"Key maker response status = {response.status_code}")
            if response.ok:
                return response.json()
            else:
                response.raise_for_status()
        except Exception as e:
            error_msg = f"In get_keymaker_api_response of KeyMakerApiProxy, " \
                        f"Unable to read KM response - " \
                        f"Status code :{response.status_code}, " \
                        f"got the exception : {e}"
            print(error_msg)
            raise ValueError(error_msg)

    except Exception as ex:
        error_msg = f"In get_keymaker_api_response of KeyMakerApiProxy, " \
                    f"Failed to fetch response from KM {ex}"
        print(error_msg)
        raise ValueError(error_msg)


def get_keymaker_key(keymaker_response, keymaker_keyname):
    try:
        for key in keymaker_response["nonkeys"]:
            if key["nonkey"]["name"] == keymaker_keyname and key["nonkey"]["state"] == "enabled":
                print("In get_keymaker_key of KeyMakerApiProxy "
                      "credential file read successfully from key maker.")
                if keymaker_keyname.find('svc') != -1:
                    key_val = base64.b64decode(key["nonkey"]["encoded_key_data"])
                    try:
                        ret_val = json.loads(key_val)
                    except TypeError as te:
                        ret_val = json.dumps(key_val)
                        print("In get_keymaker_key of KeyMakerApiProxy "
                              "error while reading the key_val, "
                              f"te --> {te}")
                    return ret_val
                else:
                    return base64.b64decode(key["nonkey"]["encoded_key_data"]).decode("utf-8")
        error_msg = f"In get_keymaker_key of KeyMakerApiProxy, " \
                    f"keymaker_keyname --> {keymaker_keyname}"
        print(error_msg)
        raise ValueError(error_msg)

    except Exception as e:
        error_msg = f"In get_keymaker_key of KeyMakerApiProxy, , " \
                    f"keymaker_keyname --> {keymaker_keyname}, " \
                    f"Key not found ...{e}"
        print(error_msg)
        raise ValueError(error_msg)


def add_cast_details(column, columns_to_cast):
    print(f"In add_cast_details, column --> {column}, "
          f"columns_to_cast --> {columns_to_cast}")
    if column in columns_to_cast.keys():
        ret_val = f"CAST(SOURCE.{column} AS {columns_to_cast[column]})"
    else:
        ret_val = f"SOURCE.{column}"
    print(f"In add_cast_details, ret_val --> {ret_val}")
    return ret_val


def condition_for_merge(passed_primary_keys, passed_table_keys,
                        columns_to_cast, passed_error_if_partition_column_has_null):
    print(f"In condition_for_merge")
    keys_list = list(map(str.strip, passed_primary_keys.split(',')))
    conditions_list = []
    for key in keys_list:
        conditions_list.append(
            add_cast_details(key, columns_to_cast) + " = TARGET." + key
        )
    partition_field_key_name = "PARTITION_FIELD"
    partition_type_key_name = "PARTITION_TYPE"
    if passed_table_keys is not None and \
            partition_field_key_name in passed_table_keys["DATA"] and \
            partition_type_key_name in passed_table_keys["DATA"]:
        partition_field = passed_table_keys["DATA"][partition_field_key_name]
        partition_type = passed_table_keys["DATA"][partition_type_key_name]
        if partition_field != '':
            if passed_error_if_partition_column_has_null == "False":
                conditions_list.append(f"(TIMESTAMP_TRUNC(TIMESTAMP(TARGET.{partition_field}),"
                                       f"{partition_type}) in "
                                       f"UNNEST(partition_keys_array) "
                                       f"OR TARGET.{partition_field} IS NULL)")
            else:
                conditions_list.append(f"TIMESTAMP_TRUNC(TIMESTAMP(TARGET.{partition_field}),"
                                       f"{partition_type}) in "
                                       f"UNNEST(partition_keys_array)")

    ret_val = " AND ".join(conditions_list)
    print(f"In condition_for_merge, ret_val --> {ret_val}")
    return ret_val


def update_stmt_on_merge(passed_config, column_list, columns_to_cast, passed_target_table_keys):
    print(f"In update_stmt_on_merge")
    column_update_list = []
    columns_not_be_updated = ''
    common_data_config = passed_config[global_constants.common_section]["DATA"]
    if common_data_config is not None and "DO_NOT_UPDATE_COLUMNS" in common_data_config:
        columns_not_be_updated = list(map(str.strip, str(common_data_config["DO_NOT_UPDATE_COLUMNS"]).split(",")))
        print(f"In update_stmt_on_merge, "
              f"columns_not_be_updated --> {columns_not_be_updated}")
    columns_not_be_updated_if_null = passed_target_table_keys.get("DO_NOT_UPDATE_IF_NULL_COLUMNS", [])
    for column in column_list:
        if column in columns_not_be_updated:
            print(f"In update_stmt_on_merge, "
                  f"column --> {column} in "
                  f"columns_not_be_updated --> {columns_not_be_updated}, "
                  f"hence not updating the same")
        else:
            if column in columns_not_be_updated_if_null:
                print(f"In update_stmt_on_merge, "
                      f"column --> {column} in "
                      f"columns_not_be_updated_if_null --> "
                      f"{columns_not_be_updated_if_null}, "
                      f"hence not having COALESCE")
                column_update_list.append(
                    "TARGET." + str(column) + " = COALESCE(" +
                    add_cast_details(column, columns_to_cast) +
                    ", TARGET." + str(column) + ")"
                )
            else:
                column_update_list.append(
                    "TARGET." + str(column) + " = " + add_cast_details(column, columns_to_cast)
                    # column1 = SOURCE.column1, column2 = SOURCE.column2...
                )
    ret_val = "UPDATE SET {}".format(', '.join(column_update_list))
    print(f"In update_stmt_on_merge, ret_val --> {ret_val}")
    return ret_val


def insert_stmt_on_merge(passed_config, column_list, columns_to_cast):
    print(f"In insert_stmt_on_merge")
    column_insert_list = []
    fin_column_list = []
    escape_keywords = ''
    common_data_config = passed_config[global_constants.common_section]["DATA"]
    if common_data_config is not None and "ESCAPE_KEYWORDS" in common_data_config:
        escape_keywords = common_data_config["ESCAPE_KEYWORDS"]
        print(f"In insert_stmt_on_merge, escape_keywords --> {escape_keywords}")
        # if column in escape_keywords, then add the escape character
    for column in column_list:
        fin_column = str(column)
        if fin_column.lower() in escape_keywords:
            fin_column = "`" + str(column) + "`"
            print(f"In insert_stmt_on_merge, fin_column --> {fin_column}")
        fin_column_list.append(fin_column)
        column_insert_list.append(
            add_cast_details(column, columns_to_cast)  # SOURCE.column1, SOURCE.column2...
        )
    ret_val = "INSERT (%s) VALUES (%s)" % (', '.join(fin_column_list), ', '.join(column_insert_list))
    print(f"In insert_stmt_on_merge, ret_val --> {ret_val}")
    return ret_val


def prepare_raw_source_array_agg_query(passed_bq_source_table_name,
                                       passed_target_table_keys,
                                       passed_audit_column,
                                       passed_extraction_start_datetime,
                                       passed_extraction_end_datetime,
                                       passed_inner_where_clause,
                                       passed_whole_table_run=False):
    print(f"In prepare_raw_source_array_agg_query of LoadUtils, "
          f"passed_bq_source_table_name --> {passed_bq_source_table_name}, "
          f"passed_target_table_keys --> {passed_target_table_keys}, "
          f"passed_audit_column --> {passed_audit_column}, "
          f"passed_extraction_start_datetime --> "
          f"{passed_extraction_start_datetime}, "
          f"passed_extraction_end_datetime --> {passed_extraction_end_datetime}, "
          f"passed_inner_where_clause --> {passed_inner_where_clause}")

    if passed_whole_table_run:
        raw_source_query = f"SELECT * FROM {passed_bq_source_table_name} " \
                           f"WHERE {passed_inner_where_clause} "
    else:
        raw_source_query = f"SELECT * FROM {passed_bq_source_table_name} " \
                           f"WHERE {passed_inner_where_clause} {passed_audit_column} " \
                           f"BETWEEN {passed_extraction_start_datetime} AND {passed_extraction_end_datetime} "
    print(f"In prepare_raw_source_array_agg_query of LoadUtils, "
          f"raw_source_query --> {raw_source_query}")

    ordering_columns = ','.join(
        [ordering_column.strip() + ' DESC' for ordering_column
         in passed_target_table_keys["ORDERING_COLUMNS"].split(';')])
    primary_keys = passed_target_table_keys["PRIMARY_KEYS"]
    print(f"In prepare_raw_source_array_agg_query of LoadUtils, "
          f"ordering_columns --> {ordering_columns}, "
          f"primary_keys --> {primary_keys}")

    prepared_raw_dedup_source_query = (f"SELECT LATEST.* "
                                       f"FROM ( "
                                       f"SELECT ARRAY_AGG( "
                                       f"TBL "
                                       f"ORDER BY {ordering_columns} "
                                       f"LIMIT 1 "
                                       f")[OFFSET(0)] LATEST "
                                       f"FROM ({raw_source_query}) TBL "
                                       f"GROUP BY "
                                       f"{primary_keys})")

    print(f"In prepare_raw_source_array_agg_query of LoadUtils, "
          f"raw_dedup_source_query --> {prepared_raw_dedup_source_query}")
    return prepared_raw_dedup_source_query


def prepare_raw_source_row_num_query(passed_bq_source_table_name,
                                     passed_target_table_keys,
                                     passed_audit_column,
                                     passed_extraction_start_datetime,
                                     passed_extraction_end_datetime,
                                     passed_inner_where_clause,
                                     passed_whole_table_run=False):
    print(f"In prepare_raw_source_row_num_query of LoadUtils, "
          f"passed_bq_source_table_name --> {passed_bq_source_table_name}, "
          f"passed_target_table_keys --> {passed_target_table_keys}, "
          f"passed_audit_column --> {passed_audit_column}, "
          f"passed_extraction_start_datetime --> "
          f"{passed_extraction_start_datetime}, "
          f"passed_extraction_end_datetime --> {passed_extraction_end_datetime}, "
          f"passed_inner_where_clause --> {passed_inner_where_clause}")

    if passed_whole_table_run:
        raw_source_query = f"SELECT * FROM {passed_bq_source_table_name} " \
                           f"WHERE {passed_inner_where_clause} "
    else:
        raw_source_query = f"SELECT * FROM {passed_bq_source_table_name} " \
                           f"WHERE {passed_inner_where_clause} {passed_audit_column} " \
                           f"BETWEEN {passed_extraction_start_datetime} AND {passed_extraction_end_datetime} "
    print(f"In prepare_raw_source_row_num_query of LoadUtils, "
          f"raw_source_query --> {raw_source_query}")

    ordering_columns = ','.join(
        [ordering_column.strip() + ' DESC' for ordering_column
         in passed_target_table_keys["ORDERING_COLUMNS"].split(';')])
    primary_keys = passed_target_table_keys["PRIMARY_KEYS"]
    print(f"In prepare_raw_source_row_num_query of LoadUtils, "
          f"ordering_columns --> {ordering_columns}, "
          f"primary_keys --> {primary_keys}")
    prepared_raw_dedup_source_query = ("SELECT LATEST.* "
                                       "FROM ( "
                                       "SELECT EXT.*,ROW_NUMBER() "
                                       "OVER (PARTITION BY {primary_keys} "
                                       "ORDER BY {ordering_columns} ) RNK "
                                       "FROM ({raw_source_query}) EXT "
                                       ")LATEST "
                                       "WHERE LATEST.RNK=1")

    print(f"In prepare_raw_source_row_num_query of LoadUtils, "
          f"raw_dedup_source_query --> {prepared_raw_dedup_source_query}")
    return prepared_raw_dedup_source_query


def prepare_merge_query(passed_full_bq_source_table_name, passed_full_bq_target_table_name,
                        passed_column_list, start_run_id, end_run_id,
                        target_table_keys, passed_columns_to_cast, passed_config,
                        passed_query_type=default_merge_query_type,
                        common_error_if_partition_column_has_null=global_constants.default_error_if_partition_column_has_null,
                        whole_table_run=False):
    print(f"In prepare_merge_query, passed_query_type --> {passed_query_type}")
    audit_column = passed_config[global_constants.common_section]["DATA"]["AUDIT_COLUMNS"]
    extraction_start_datetime = f"TIMESTAMP(PARSE_TIMESTAMP('%Y%m%d%H%M%S', " \
                                f"'{start_run_id}','PST8PDT'))"
    extraction_end_datetime = f"TIMESTAMP(PARSE_TIMESTAMP('%Y%m%d%H%M%S', " \
                              f"'{end_run_id}','PST8PDT'))"

    table_level_error_if_partition_column_has_null = target_table_keys.get(
        "ERROR_IF_PARTITION_COLUMN_HAS_NULL", ""
    )
    if table_level_error_if_partition_column_has_null:
        error_if_partition_column_has_null = table_level_error_if_partition_column_has_null
    elif common_error_if_partition_column_has_null:
        error_if_partition_column_has_null = common_error_if_partition_column_has_null
    else:
        error_if_partition_column_has_null = global_constants.default_error_if_partition_column_has_null

    inner_where_clause = ""
    if target_table_keys is not None and target_table_keys.get("INNER_WHERE_CLAUSE") is not None:
        where_clause = target_table_keys["INNER_WHERE_CLAUSE"]
        if where_clause != '':
            inner_where_clause = "%s AND" % where_clause
    print(f"In prepare_merge_query, inner_where_clause --> {inner_where_clause}")

    partition_prune_query = ""
    partition_field_key_name = "PARTITION_FIELD"
    partition_type_key_name = "PARTITION_TYPE"
    if target_table_keys is not None and \
            partition_field_key_name in target_table_keys["DATA"] and \
            partition_type_key_name in target_table_keys["DATA"]:
        partition_field = target_table_keys["DATA"][partition_field_key_name]
        partition_type = target_table_keys["DATA"][partition_type_key_name]
        if partition_field != '':
            if error_if_partition_column_has_null == "False":
                partition_prune_query = "DECLARE partition_keys_array DEFAULT ARRAY(select " \
                                        f"distinct(TIMESTAMP_TRUNC(" \
                                        f"TIMESTAMP(" \
                                        f"COALESCE(" \
                                        f"TIMESTAMP({partition_field}), " \
                                        f"{global_constants.default_partition_keys_array_values}" \
                                        f")" \
                                        f"),{partition_type})) " \
                                        f"from {passed_full_bq_source_table_name} " \
                                        f"where {inner_where_clause} {audit_column} " \
                                        f"BETWEEN {extraction_start_datetime} AND {extraction_end_datetime}); \n"
            else:
                partition_prune_query = "DECLARE partition_keys_array DEFAULT ARRAY(select " \
                                        f"distinct(TIMESTAMP_TRUNC(" \
                                        f"TIMESTAMP(" \
                                        f"{partition_field}" \
                                        f"),{partition_type})) " \
                                        f"from {passed_full_bq_source_table_name} " \
                                        f"where {inner_where_clause} {audit_column} " \
                                        f"BETWEEN {extraction_start_datetime} AND {extraction_end_datetime}); \n"
    print(f"In prepare_merge_query, partition_prune_query --> {partition_prune_query}")

    primary_keys = target_table_keys["PRIMARY_KEYS"]
    print(f"In prepare_merge_query, "
          f"primary_keys --> {primary_keys}")
    if passed_query_type == "ARRAY_AGG":
        raw_dedup_source_query = prepare_raw_source_array_agg_query(passed_full_bq_source_table_name,
                                                                    target_table_keys,
                                                                    audit_column,
                                                                    extraction_start_datetime,
                                                                    extraction_end_datetime,
                                                                    inner_where_clause,
                                                                    whole_table_run)
    elif passed_query_type == "ROW_NUM":
        raw_dedup_source_query = prepare_raw_source_row_num_query(passed_full_bq_source_table_name,
                                                                  target_table_keys,
                                                                  audit_column,
                                                                  extraction_start_datetime,
                                                                  extraction_end_datetime,
                                                                  inner_where_clause,
                                                                  whole_table_run)
    else:
        raw_dedup_source_query = prepare_raw_source_array_agg_query(passed_full_bq_source_table_name,
                                                                    target_table_keys,
                                                                    audit_column,
                                                                    extraction_start_datetime,
                                                                    extraction_end_datetime,
                                                                    inner_where_clause,
                                                                    whole_table_run)
    print(f"In prepare_merge_query, raw_dedup_source_query --> {raw_dedup_source_query}")

    matched_clause = ""
    if target_table_keys is not None and target_table_keys.get("MATCHED_CLAUSE") is not None:
        matched_column = target_table_keys["MATCHED_CLAUSE"]
        if matched_column != '':
            matched_clause = f"AND TARGET.{matched_column} < SOURCE.{matched_column} "
    if not matched_clause:
        if target_table_keys is not None and target_table_keys.get("ORDERING_COLUMNS") is not None:
            matched_column = str(target_table_keys["ORDERING_COLUMNS"].split(';')[0]).strip()
            matched_clause = f"AND (SOURCE.{matched_column} IS NOT NULL) " \
                             f"AND TARGET.{matched_column} <= SOURCE.{matched_column} "
    print(f"In prepare_merge_query, matched_clause --> {matched_clause}")
    conditions_on_merge = condition_for_merge(primary_keys,
                                              target_table_keys,
                                              passed_columns_to_cast,
                                              error_if_partition_column_has_null)
    update_set = update_stmt_on_merge(passed_config, passed_column_list,
                                      passed_columns_to_cast, target_table_keys)
    insert_set = insert_stmt_on_merge(passed_config, passed_column_list, passed_columns_to_cast)

    merge_query = (f"MERGE INTO {passed_full_bq_target_table_name} AS TARGET "
                   f" USING ({raw_dedup_source_query}) AS SOURCE"
                   f" ON ({conditions_on_merge})"
                   f" WHEN MATCHED {matched_clause} THEN {update_set}"
                   f" WHEN NOT MATCHED THEN {insert_set}")
    print(f"In prepare_merge_query, merge_query --> {merge_query}")
    merge_query = partition_prune_query + merge_query
    return merge_query


def exec_command(command):
    """
    Execute the command and return the exit status.
    """
    print('Running system command: {0}\n'.format(''.join(command)))
    pobj = subprocess.Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
    stdo, stde = pobj.communicate()
    exit_code = pobj.returncode
    if stdo or stde:
        print(f"stdo --> {stdo}, stde --> {stde}")
    return exit_code, stdo, stde


def execute_bq_query_and_get_results(query, passed_bq_client,
                                     passed_gcs_project, column_name,
                                     passed_labels=None):
    print("********** In execute_bq_query_and_get_results of BQEndpoint ********** ")

    query = query
    print(f"In execute_bq_query_and_get_results of BQEndpoint, "
          f"query --> {query}, "
          f"passed_labels --> {passed_labels}, "
          f"passed_gcs_project --> {passed_gcs_project}")
    query_job_config = bigquery.QueryJobConfig()
    gcs_project = passed_gcs_project
    if passed_labels:
        query_job_config.labels = passed_labels
    query_job = passed_bq_client.query(
        query,
        project=gcs_project,
        job_config=query_job_config)
    print(f"In execute_bq_query_and_get_results of BQEndpoint, "
          f"Started job, "
          f"query_job.job_id --> {query_job.job_id}")
    query_job_rows = query_job.result()
    print(f"In execute_bq_query_and_get_results of BQEndpoint, "
          f"completed job, "
          f"query_job.job_id --> {query_job.job_id}, "
          f"job_result --> {query_job_rows}")
    query_job_result = str(0)
    for query_job_row in query_job_rows:
        query_job_result = str(query_job_row[column_name])
    print(f"In execute_bq_query_and_get_results of BQEndpoint, "
          f"{column_name} --> {query_job_result}")
    return query_job_result


def upload_string_to_gcs(passed_string, passed_gcs_bucket, passed_gcs_uri):
    print("********** In upload_string_to_gcs of GCSEndpoint ********** ")
    print(f"In upload_string_to_gcs of GCSEndpoint, "
          f"passed_string --> {passed_string}")
    gcs_bucket = passed_gcs_bucket
    gcs_uri = passed_gcs_uri
    blob = gcs_bucket.blob(gcs_uri)
    blob.upload_from_string(passed_string)


def get_pgp_key_details(keymaker_response, keymaker_keyname):
    """ Get the decryption keys from KM """
    for key in keymaker_response["pgpkeypairs"]:
        if key["pgpkeypair"]["name"] == keymaker_keyname:
            private_key = base64.b64decode(key["pgpkeypair"]["secret_keyring"]["encoded_keyring"])
            passphrase = base64.b64decode(key["pgpkeypair"]["secret_keyring"]["encoded_passphrase"])
            return private_key, passphrase


def encrypt_file(private_key, passphrase, source_file_paths, dest_file_paths):
    source_path_list = source_file_paths if isinstance(source_file_paths, list) else [source_file_paths]
    dest_path_list = dest_file_paths if isinstance(dest_file_paths, list) else [dest_file_paths]
    key, _ = pgpy.PGPKey.from_blob(private_key)
    for source_path, dest_path in zip(source_path_list, dest_path_list):
        print(f"PGP Encryption started for {source_path}")
        with open(source_path, "rb") as file:
            file_data = file.read()
            with key.unlock(passphrase.decode("utf-8", 'ignore')):
                message = pgpy.PGPMessage.new(file_data)
                enc_message = key.pubkey.encrypt(message)
                with open(dest_path, "w") as encrypted_file:
                    encrypted_file.write(str(enc_message))


def decrypt_file(private_key, passphrase, encrypted_file_paths, dest_file_paths, passed_logger=None, read_type='w'):
    source_path_list = encrypted_file_paths if isinstance(encrypted_file_paths, list) else [encrypted_file_paths]
    dest_path_list = dest_file_paths if isinstance(dest_file_paths, list) else [dest_file_paths]

    key, _ = pgpy.PGPKey.from_blob(private_key)
    for source_path, dest_path in zip(source_path_list, dest_path_list):
        print(f"PGP Decryption started for {source_path}")
        enc_content = pgpy.PGPMessage.from_file(rf"{source_path}")
        with open(dest_path, read_type) as decrypted_file:
            with key.unlock(passphrase.decode("utf-8", 'ignore')):
                dec_content = key.decrypt(enc_content).message
                if read_type == 'w':
                    dec_content = str(dec_content.decode("utf-8", 'ignore'))
                decrypted_file.write(dec_content)

