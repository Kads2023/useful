import json
import os
import zlib

from google.cloud import storage, bigquery
from google.cloud.bigquery import ExternalSourceFormat
from google.cloud.exceptions import NotFound


def get_gcs_client(credentials):
    return storage.Client.from_service_account_info(credentials)


def get_bq_client(credentials):
    return bigquery.Client.from_service_account_info(credentials)


def download_blob_to_gcs(gcs_bucket, gcs_timeout_seconds, source_file_path, destination_file_path):
    blobs = gcs_bucket.list_blobs(prefix=source_file_path)
    for blob in blobs:
        file_name = blob.name.split("/")[-1]
        destination_file = destination_file_path + "/" + file_name
        blob.download_to_filename(destination_file, timeout=gcs_timeout_seconds)


def upload_blob_to_gcs(gcs_bucket, gcs_timeout_seconds, gcs_retries, source_file_name, destination_blob_name):
    blob = gcs_bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name, timeout=gcs_timeout_seconds, num_retries=gcs_retries)


def download_gcs_files(gcs_bucket, list_path, nas_staging_dir):
    blobs = gcs_bucket.list_blobs(prefix=list_path)
    print(list_path, blobs)
    for blob in blobs:
        if os.path.basename(blob.name) != '':
            destination_uri = '{}/{}'.format(nas_staging_dir, os.path.basename(blob.name))
            print(f"Downloading GCS file to NAS --> {destination_uri}")
            blob.download_to_filename(destination_uri)


def list_gcs_sub_folders(gcs_bucket, list_path):
    blobs = gcs_bucket.list_blobs(prefix=list_path)
    print(list_path, blobs)
    schema_folders = set()
    for blob in blobs:
        sub_folder_name = os.path.dirname(blob.name)
        schema_folders.add(sub_folder_name)

    return schema_folders


def get_bq_schema_from_json(gcs_schema_json):
    """
    To form the external table, STRING datatype is used for all the columns.
    Then type casting will be performed for all the columns based on the actual datatype during _raw table creation.
    """
    return [bigquery.SchemaField(i["name"], "STRING") for i in gcs_schema_json["schema"]]


def get_bq_parquet_configs(gcs_staging_uri_prefix, bq_parquet_common_config, bq_parquet_table_config=None):
    print('Partition source_uri_prefix %s' % gcs_staging_uri_prefix)

    use_clustering_for_raw = ""
    if bq_parquet_table_config:
        use_clustering_for_raw = bq_parquet_table_config.get('USE_CLUSTERING_FOR_RAW', None)

    # Hive partition config
    hive_partitioning_opts = bigquery.external_config.HivePartitioningOptions()
    hive_partitioning_opts.mode = "CUSTOM"
    hive_partitioning_opts.source_uri_prefix = gcs_staging_uri_prefix
    # Table Partition
    time_partitioning_opts = bigquery.TimePartitioning()
    time_partitioning_opts.type_ = bq_parquet_common_config.get('PARTITION_TYPE', None)
    time_partitioning_opts.field = bq_parquet_common_config.get('PARTITION_FIELD', None)
    # Load job config
    job_config_opts = bigquery.LoadJobConfig()
    job_config_opts.source_format = bigquery.SourceFormat.PARQUET
    job_config_opts.time_partitioning = time_partitioning_opts
    job_config_opts.hive_partitioning = hive_partitioning_opts
    if use_clustering_for_raw == "True":
        job_config_opts.clustering_fields = bq_parquet_table_config.get('CLUSTERING_FIELDS', None)
    else:
        job_config_opts.clustering_fields = bq_parquet_common_config.get('CLUSTERING_FIELDS', None)
    job_config_opts.schema_update_options = ["ALLOW_FIELD_ADDITION"]
    return job_config_opts


def load_bq_from_gcs_parquet(bq_client, gcs_dir, table_name, project, job_config):
    load_job = bq_client.load_table_from_uri(gcs_dir, table_name, project=project, job_config=job_config)
    return load_job.result()


def get_bq_csv_configs(gcs_staging_uri_prefix, gcs_dir, bq_schema, table_name,
                       gcs_table_id, bq_csv_common_config, bq_csv_table_config=None):
    print('Partition source_uri_prefix %s' % gcs_staging_uri_prefix)

    use_clustering_for_raw = ""
    if bq_csv_table_config:
        use_clustering_for_raw = bq_csv_table_config.get('USE_CLUSTERING_FOR_RAW', None)

    # Hive partition config
    hive_partitioning_opts = bigquery.external_config.HivePartitioningOptions()
    hive_partitioning_opts.mode = "CUSTOM"
    hive_partitioning_opts.source_uri_prefix = gcs_staging_uri_prefix

    # Configure the external data source
    external_config = bigquery.ExternalConfig(ExternalSourceFormat.CSV)
    external_config.hive_partitioning = hive_partitioning_opts
    external_config.source_uris = [
        gcs_dir
    ]
    external_config.schema = bq_schema
    external_config.options.skip_leading_rows = bq_csv_common_config["HEADER_ROWS"]
    external_config.options.field_delimiter = bq_csv_common_config["FILE_DELIMITER"]
    external_config.options.allow_quoted_newlines = False
    external_config.options.quote_character = ''
    external_config.options.allow_jagged_rows = False

    # Table Partition
    time_partitioning_opts = bigquery.TimePartitioning()
    time_partitioning_opts.type_ = bq_csv_common_config['PARTITION_TYPE']  # bigquery.TimePartitioningType.DAY
    time_partitioning_opts.field = bq_csv_common_config['PARTITION_FIELD']

    job_config_external = bigquery.QueryJobConfig(table_definitions={table_name + "_external": external_config})
    job_config_external.destination = gcs_table_id
    job_config_external.time_partitioning = time_partitioning_opts
    if use_clustering_for_raw == "True":
        job_config_external.clustering_fields = bq_csv_table_config['CLUSTERING_FIELDS']
    else:
        job_config_external.clustering_fields = bq_csv_common_config['CLUSTERING_FIELDS']
    job_config_external.schema_update_options = ["ALLOW_FIELD_ADDITION"]
    job_config_external.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    return job_config_external


def load_bq_from_gcs_csv(bq_client, table_name, project, job_config_external,
                         gcs_schema_json, bq_csv_table_config, source_timezone):
    # `last_ingestion_utc_time` column details are not available in the actual schema.
    # Because, this column is added to the _external table while loading the hive partitioned data from GCS.
    gcs_schema_json['schema'].append({'name': 'last_ingestion_utc_time', 'dataType': 'TIMESTAMP'})
    escape_keywords = ''
    if bq_csv_table_config is not None and "ESCAPE_KEYWORDS" in bq_csv_table_config:
        escape_keywords = bq_csv_table_config["ESCAPE_KEYWORDS"]
        print(f"escape_keywords --> {escape_keywords}")
    audit_columns = ''
    if bq_csv_table_config is not None and "AUDIT_COLUMNS" in bq_csv_table_config:
        audit_columns = bq_csv_table_config["AUDIT_COLUMNS"]
        print(f"audit_columns --> {audit_columns}")
    # Since data for some STRING columns are available as 'NULL',
    # we get inconsistent data like None and NULL in the table.
    # So to handle this, 'NULL' check is added during the type casting.
    load_sql = 'SELECT'
    for column_schema in gcs_schema_json['schema']:
        fin_column_name = str(column_schema['name'])
        if fin_column_name.lower() in escape_keywords:
            fin_column_name = "`" + str(column_schema['name']) + "`"
        if " " in fin_column_name:
            fin_column_name = str(column_schema['name']).replace(" ", "_")
        cast_column = f"{fin_column_name}"
        if column_schema['dataType'] == "STRING" or "BLOB" in column_schema['dataType']:
            cast_column = f"IF({fin_column_name}='NULL', Null, {fin_column_name})"
        load_sql += f" SAFE_CAST({cast_column} AS {column_schema['dataType']}) AS {fin_column_name},"
    load_sql += f"{audit_columns}, '{source_timezone}' as source_timezone,"
    load_sql = f" FROM {table_name}_external".join(load_sql.rsplit(',', 1))
    print(load_sql)

    query_job = bq_client.query(load_sql, project=project, job_config=job_config_external)
    print("Started job {}".format(query_job.job_id))
    return query_job.result()


def get_bq_json_external_configs(gcs_staging_uri_prefix, gcs_dir, table_name, gcs_table_id,
                                 bq_schema, bq_json_common_config, bq_json_table_config=None):
    print('Partition source_uri_prefix %s' % gcs_staging_uri_prefix)

    use_clustering_for_raw = ""
    if bq_json_table_config:
        use_clustering_for_raw = bq_json_table_config.get('USE_CLUSTERING_FOR_RAW', None)

    # Hive partition config
    hive_partitioning_opts = bigquery.external_config.HivePartitioningOptions()
    hive_partitioning_opts.mode = "CUSTOM"
    hive_partitioning_opts.source_uri_prefix = gcs_staging_uri_prefix

    # Table Partition
    time_partitioning_opts = bigquery.TimePartitioning()
    time_partitioning_opts.type_ = bq_json_common_config.get('PARTITION_TYPE', None)
    time_partitioning_opts.field = bq_json_common_config.get('PARTITION_FIELD', None)

    # Configure the external data source
    external_config = bigquery.ExternalConfig(bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
    external_config.schema = bq_schema
    external_config.hive_partitioning = hive_partitioning_opts
    external_config.source_uris = [gcs_dir]

    job_config_external = bigquery.QueryJobConfig(table_definitions={table_name + "_external": external_config})
    job_config_external.destination = gcs_table_id
    job_config_external.time_partitioning = time_partitioning_opts
    if use_clustering_for_raw == "True":
        job_config_external.clustering_fields = bq_json_table_config['CLUSTERING_FIELDS']
    else:
        job_config_external.clustering_fields = bq_json_common_config['CLUSTERING_FIELDS']
    job_config_external.schema_update_options = ["ALLOW_FIELD_ADDITION"]
    job_config_external.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    return job_config_external


def load_bq_from_gcs_external_json(bq_client, table_name, project, job_config_external,
                                   gcs_schema_json, bq_json_table_config, source_timezone):
    load_sql = 'SELECT '
    for column_schema in gcs_schema_json['schema']:
        if column_schema['dataType'] != "STRING":
            load_sql += f" SAFE_CAST({column_schema['name']} AS {column_schema['dataType']}) " \
                        f"AS {column_schema['name']},"
        else:
            load_sql += f" {column_schema['name']},"
    load_sql += f" {bq_json_table_config['AUDIT_COLUMNS']}, '{source_timezone}' as source_timezone" \
                f", last_ingestion_utc_time FROM {table_name}_external"
    print(load_sql)

    query_job = bq_client.query(load_sql, project=project, job_config=job_config_external)
    print("Started job {}".format(query_job.job_id))
    return query_job.result()


# SNAPSHOT load utils

def get_bq_table_schema(bq_client, dataset_name, table_name, project, passed_label=None):
    bq_schema = {}
    query = "SELECT * FROM " + dataset_name + ".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + table_name + "'"
    print("Query to get column names : {} ".format(query))
    query_job_config = bigquery.QueryJobConfig()
    if passed_label:
        query_job_config.labels = passed_label
    query_job = bq_client.query(query, project=project, job_config=query_job_config)
    rows = query_job.result()
    for row in rows:
        bq_schema[row['column_name'].lower()] = row['data_type']
    return bq_schema


def execute_bq_query(bq_client, query, project, labels,
                     should_publish_stats=True, local_params=None, load_type=None, gcs_table_id=None):
    query_job_config = bigquery.QueryJobConfig()
    query_job_config.labels = labels
    if load_type == "FULL_LOAD":
        query_job_config.destination = gcs_table_id
        query_job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    query_job = bq_client.query(query, project=project, job_config=query_job_config)
    print("Started job {}".format(query_job.job_id))
    job_result = query_job.result()
    if should_publish_stats:
        publish_stats_for_query(bq_client, query, project, query_job, local_params)
    return job_result


def execute_bq_query_with_params(bq_client, query, project, start_run_id, end_run_id, passed_label=None):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_run_id", "STRING", start_run_id),
            bigquery.ScalarQueryParameter("end_run_id", "STRING", end_run_id),
        ]
    )
    if passed_label:
        job_config.labels = passed_label
    query_job = bq_client.query(query, job_config=job_config, project=project)
    print("Started job {}".format(query_job.job_id))
    return query_job.result()


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


def get_bq_job_labels(edlxp_name, label, target_table, dataset, end_run_id, last_ingestion_utc_time,
                      km_app_name, km_credential_json, run_no=None):
    short_table_name = [word[:2] for word in target_table.split('_')]
    short_table_name = ''.join(short_table_name).replace("_", "").lower()
    short_table_name_new = [word[:1] for word in target_table.split('_')]
    short_table_name_new = ''.join(short_table_name_new).replace("_", "").lower()
    if not run_no:
        run_no = 1
    ret_val = {
        "job-instance-id": "%s-%s-%s-%s-%s" % (edlxp_name.lower(), label[0].lower(),
                                               short_table_name_new, end_run_id[2:], run_no),
        "table_name": "%s-%s" % (edlxp_name.lower(), target_table.lower()),
        "property": "%s" % edlxp_name.lower(),
        "task": "%s-%s" % (edlxp_name.lower(), label.lower()),
        "dataset": "%s-%s" % (edlxp_name.lower(), dataset.lower()),
        "pp-edp-resource-name": "%s-%s-%s" % (edlxp_name.lower(), dataset.lower(), short_table_name),
        "pp-edp-custom-billing-tag": "%s-%s-%s-%s" % (edlxp_name.lower(), label.lower(),
                                                      short_table_name, end_run_id[2:]),
        "pp-edp-job-run-id": "%s-%s-%s-%s-%s" % (edlxp_name.lower(), label[0].lower(),
                                                 short_table_name_new, end_run_id[2:], run_no),
        "ingestion-time": "%s-%s" % (edlxp_name.lower(), last_ingestion_utc_time.strftime('%Y%m%d%H%M%S')),
        "pp-edp-platform": "non-opiniated",
        "pp-edp-business-unit": "edp-big-data-platform-engineering",
        "pp-edp-org-unit": "enterprise-data-solutions",
        "pp-edp-app-id": km_app_name,
        "pp-edp-app-name": km_app_name,
        "pp-edp-gcp-uid": km_credential_json
    }
    print(f"inside get_bq_job_labels, ret_val --> {ret_val}")
    return ret_val


def get_job_level_stats(query, query_stats_json, stat_params, job_id):
    # Get on job level
    print(f"inside get_job_level_stats, stat_params --> {stat_params}")
    job_destination_db_name = 'NO_TARGET_SPECIFIED'
    job_start_time, job_end_time, num_dml_affected_rows = query_stats_json['startTime'], query_stats_json.get(
        'endTime'), query_stats_json['query'].get('numDmlAffectedRows', "0")
    job_time_taken = (job_end_time - job_start_time) / 1000
    if "destination_table" in stat_params:
        job_destination_db_name = (stat_params['destination_table']['datasetId'] + ":" +
                                   stat_params['destination_table']['tableId'])

    query_checksum = zlib.crc32(query.encode('utf-8'))
    query = query[0:4000]  # Length of query column in table is 4000

    job_stats = {
        'end_time_ms': date_to_epoch(stat_params['end_time_ms']),
        'seq_num': stat_params['seq_num'],
        'job_id': job_id,
        'job_name': stat_params['job_name'],
        'query_type': 'JOB',
        'checksum': query_checksum,
        'query': query,
        'db_name': job_destination_db_name,
        'init_time': job_start_time,
        'end_time': job_end_time,
        'record_count': num_dml_affected_rows,
        'time_taken': job_time_taken
    }
    print(f"inside get_job_level_stats, job_stats --> {job_stats}")
    return job_stats


def get_stage_level_stats(query_stats_json, stat_params, job_id):
    if 'query' not in query_stats_json or 'queryPlan' not in query_stats_json['query']:
        return None
    print(f"inside get_stage_level_stats, stat_params --> {stat_params}")
    stage_json = query_stats_json['query']['queryPlan']
    stats_rows = []
    for stage in stage_json:
        stage_label = stage['name']
        stage_start_time = stage['startMs']
        stage_end_time = stage['endMs']
        stage_time_taken = (int(stage_end_time) - int(stage_start_time)) / 1000
        stage_records_read = stage['recordsRead']
        if "Input" in stage_label:
            stage_query = '__::__'.join(
                [', '.join(elem['substeps']) for elem in stage['steps'] if elem['kind'] == "READ"])
        else:
            stage_query = '__::__ '.join(
                [elem['kind'] + ":" + (', '.join(elem['substeps'])) for elem in stage['steps']])

        query_checksum = zlib.crc32(stage_query.encode('utf-8'))
        stage_query_split = stage_query.lower().split()
        stage_db_name = ""
        if "Input" in stage_label and 'from' in stage_query_split:
            db_name_index = stage_query_split.index('from') + 1
            if db_name_index < len(stage_query_split):
                stage_db_name = stage_query_split[db_name_index]
        else:
            stage_db_name = 'Input stage : ' + ','.join(stage['inputStages'])

        job_stats = {
            'end_time_ms': date_to_epoch(stat_params['end_time_ms']),
            'seq_num': stat_params['seq_num'],
            'job_id': job_id,
            'job_name': stat_params['job_name'],
            'query_type': 'STAGE',
            'checksum': query_checksum,
            'query': stage_query,
            'db_name': stage_db_name,
            'init_time': stage_start_time,
            'end_time': stage_end_time,
            'record_count': stage_records_read,
            'time_taken': stage_time_taken
        }
        print(f"inside get_stage_level_stats, job_stats --> {job_stats}")
        stats_rows.append(job_stats)
    return stats_rows


def publish_stats_for_query(bq_client, query, project, query_job, stat_params):
    stats_rows = []
    parent_job_id = query_job.job_id
    query_job_properties = query_job._properties
    parent_query_stats_json = json.loads(json.dumps(query_job_properties['statistics']))

    # Get the destination table name from target table (if present)
    if 'configuration' in query_job_properties and \
            'query' in query_job_properties['configuration'] and \
            'destinationTable' in query_job_properties['configuration']['query']:
        stat_params['destination_table'] = json.loads(
            json.dumps(query_job_properties['configuration']['query']['destinationTable']))

    parent_stats_row = get_stats_from_json(query, parent_query_stats_json, stat_params, parent_job_id)
    if parent_stats_row is not None:
        stats_rows.extend(parent_stats_row)

    if 'destination_table' in stat_params:
        del stat_params['destination_table']

    child_job_list = bq_client.list_jobs(parent_job=parent_job_id, project=project)

    for child_job in child_job_list:
        child_job_properties = child_job._properties
        child_stats_json = json.loads(json.dumps(child_job_properties['statistics']))
        child_job_id = parent_job_id + ":" + child_job.job_id
        child_query = child_stats_json['scriptStatistics']['stackFrames'][0]['text']

        # Get the destination table name from target table (if present)
        if 'configuration' in child_job_properties and 'query' in \
                child_job_properties['configuration'] and 'destinationTable' \
                in child_job_properties['configuration']['query']:
            stat_params['destination_table'] = json.loads(
                json.dumps(child_job_properties['configuration']['query']['destinationTable']))

        child_stats_row = get_stats_from_json(child_query, child_stats_json, stat_params, child_job_id)
        if child_stats_row is not None:
            stats_rows.extend(child_stats_row)

        if 'destination_table' in stat_params:
            del stat_params['destination_table']

    # insert_into_stats_table(stats_rows)


def get_stats_from_json(query, query_stats_json, stat_params, job_id):
    stats_rows = []
    job_level_stats = get_job_level_stats(query, query_stats_json, stat_params, job_id)
    if job_level_stats is not None:
        stats_rows.append(job_level_stats)

    stage_level_stats = get_stage_level_stats(query_stats_json, stat_params, job_id)
    if stage_level_stats is not None:
        stats_rows.extend(stage_level_stats)

    return stats_rows


def check_table(bq_client, bq_table_name):
    try:
        bq_client.get_table(bq_table_name)
        print("Table {} already exists.".format(bq_table_name))
        return True
    except NotFound:
        print("Table {} is not found.".format(bq_table_name))
        return False


def execute_bq_query_table_creation(bq_client, query, project, target_table, table_config, passed_label=None):
    partition_key_name = "PARTITION_FIELD"
    clustering_key_name = "CLUSTERING_FIELDS"
    partition_type_name = "PARTITION_TYPE"

    query_job_config = bigquery.QueryJobConfig(
        destination=target_table
    )
    if partition_key_name in table_config:
        partition_type = table_config[partition_type_name]
        if partition_type != '':
            time_partitioning_opts = bigquery.TimePartitioning()
            time_partitioning_opts.type_ = partition_type
            time_partitioning_opts.field = table_config[partition_key_name]
            query_job_config.time_partitioning = time_partitioning_opts

    if clustering_key_name in table_config:
        clustering_key = table_config[clustering_key_name]
        if clustering_key != '':
            query_job_config.clustering_fields = clustering_key
    if passed_label:
        query_job_config.labels = passed_label
    query_job = bq_client.query(query, project=project, job_config=query_job_config)
    print("Started job {}".format(query_job.job_id))
    job_result = query_job.result()
    return job_result
