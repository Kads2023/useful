
import os
import sys

import common_functions as utils
import gcp_utils

from google.cloud import bigquery

env = os.getenv('ENVIRONMENT')
home_dir = os.getenv('HOME')

def execute_bq_query_with_allow_field_addition(bq_client, query, project, gcs_table_id):
    query_job_config = bigquery.QueryJobConfig()
    query_job_config.destination = gcs_table_id
    query_job_config.schema_update_options = ["ALLOW_FIELD_ADDITION"]
    query_job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    query_job = bq_client.query(query, project=project, job_config=query_job_config)
    print("Started job {}".format(query_job.job_id))
    job_result = query_job.result()
    return job_result



def execute_bq_query(bq_client, query, project, passed_label=None):
    query_job_config = bigquery.QueryJobConfig()
    if passed_label:
        query_job_config.labels = passed_label
    query_job = bq_client.query(query, project=project, job_config=query_job_config)
    print("Started job {}".format(query_job.job_id))
    job_result = query_job.result()
    return job_result



def execute_bq_query_with_params(bq_client, query, project, start_run_id, end_run_id):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_run_id", "STRING", start_run_id),
            bigquery.ScalarQueryParameter("end_run_id", "STRING", end_run_id),
        ]
    )
    query_job = bq_client.query(query, job_config=job_config, project=project)
    job_result = query_job.result()
    sourced_records = int(job_result.total_rows)


xp = ""

prop_home = f""
km_credentials = f""
gcs_bucket_name = f""
gcs_project = ""
bq_dataset = f"{xp}_raw_tables"
bq_project = ""




keymaker_response = utils.get_keymaker_api_response('https://keymakerapi.g.paypalinc.com:21358/kmsapi/v1/keyobject/all'
                                                    ,f'{home_dir}/{prop_home}/common/dependency-files/identity.txt')



gcs_credential_json = utils.get_keymaker_key(keymaker_response, km_credentials)
bq_client = gcp_utils.get_bq_client(gcs_credential_json)




query = f"""
CREATE TABLE {bq_project}.{bq_dataset}.delta_trial (
ID STRING,
ATTRIBUTES STRUCT<phone_number_2 STRING,occupation_description STRING>)
"""

query = """
SELECT
CAST("4" AS STRING) AS ID,
STRUCT(
CAST("766857" AS STRING) AS phone_number_2,
CAST("ghjk" AS STRING) AS occupation_description,
CAST("hgkfghj" AS STRING) AS occupation_description_new,
CAST("5678fgd" AS STRING) AS occupation_description_new_1
) AS ATTRIBUTES
"""

execute_bq_query_with_allow_field_addition(bq_client, query, gcs_project, f'{bq_project}.{bq_dataset}.delta_trial')

