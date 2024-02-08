from pyspark.sql import SparkSession
import os
import json
import base64
import requests
import sys
import datetime
import argparse

os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""

try:
    from pyspark.sql import functions as F
except ImportError:
    pass  # Let non-Spark people at least enjoy the loveliness of the pandas datacompy functionality


def get_keymaker_api_response(token, keymaker_url, cacert_path=None):
    try:

        # Handle certificate and SSL verification
        if not cacert_path:
            cacert_path = False

        # Handle keymaker token
        try:
            token_decoded = base64.b64decode(token)
            # with open(encoded_appcontext_path) as f:
            #     token = f.read()
            #     print("Appcontext file read successfully.")
            #     token_decoded = base64.b64decode(token)
        except IOError:
            pass
            # raise ValueError("can't find file {}".format(encoded_appcontext_path))

        # strip newline from token string
        token_decoded = token_decoded.strip()
        # url = "{}?version_hint=last_enabled".format(keymaker_url)
        url = keymaker_url
        headers = {"Content-Type": "application/json", "X-KM-APP-CONTEXT": token_decoded}
        try:
            session = requests.Session()
            session.trust_env = False
            response = session.get(url, headers=headers, verify=cacert_path)
            print("Key maker response status = {}".format(response.status_code))
            if response.ok:
                return response.json()
            else:
                response.raise_for_status()
        except Exception as e:
            print("Unable to read KM response - Status code :{}".format(response.status_code))
            raise e

    except Exception as ex:
        print("Failed to fetch response from KM {}".format(ex))
        raise RuntimeError("Failed to fetch response from KM " + str(ex))


def get_keymaker_key(passed_keymaker_response, keymaker_keyname):
    try:
        for key in passed_keymaker_response["nonkeys"]:
            if key["nonkey"]["name"] == keymaker_keyname and key["nonkey"]["state"] == "enabled":
                print("KeyMakerApiProxy credential file read successfully from key maker.")
                if keymaker_keyname.find('svc') != -1:
                    return json.loads(base64.b64decode(key["nonkey"]["encoded_key_data"]))
                else:
                    return base64.b64decode(key["nonkey"]["encoded_key_data"]).decode("utf-8")
        raise ValueError(keymaker_keyname)

    except Exception as e:
        print("Key not found ...{}".format(e))
        raise RuntimeError("Key not found :: {}".format(e))


def main():
    if len(sys.argv) == 0:
        print("Invalid Number of Arguments passed")
        print("Usage " + sys.argv[
            0] + "<file_path_with_name")

    print(f"inside main --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")

    parser = argparse.ArgumentParser()
    parser.add_argument("--source_query", help="source_query", required=True, default="")
    parser.add_argument("--full_bq_target_table", help="full_bq_target_table", required=True, default="")
    parser.add_argument("--sf_db", help="sf_db", required=True, default="")
    parser.add_argument("--sf_schema", help="sf_schema", required=True, default="")
    parser.add_argument("--sf_warehouse", help="sf_warehouse", required=True, default="")
    parser.add_argument("--check_data", help="check_data", required=False, default="False")
    parser.add_argument("--append_mode", help="append_mode", required=False, default="False")

    spark = SparkSession.builder.appName('sf_read').getOrCreate()

    gcs_bucket_name = ''

    spark.conf.set("viewsEnabled", "true")
    spark.conf.set("temporaryGcsBucket", gcs_bucket_name)
    spark.conf.set("materializationProject", "")
    spark.conf.set("materializationDataset", "")

    in_args = parser.parse_args()
    source_query = str(in_args.source_query).strip()
    full_bq_target_table = str(in_args.full_bq_target_table).strip()
    sf_db = str(in_args.sf_db).strip()
    sf_schema = str(in_args.sf_schema).strip()
    sf_warehouse = str(in_args.sf_warehouse).strip()
    check_data = eval(str(in_args.check_data).strip().capitalize())
    in_append_mode = eval(str(in_args.append_mode).strip().capitalize())

    if in_append_mode:
        append_mode = "append"
    else:
        append_mode = "overwrite"

    print(f"source_query --> {source_query}, "
          f"full_bq_target_table --> {full_bq_target_table}, "
          f"sf_db --> {sf_db}, "
          f"sf_schema --> {sf_schema}, "
          f"sf_warehouse --> {sf_warehouse}, "
          f"check_data --> {check_data}, "
          f"in_append_mode --> {in_append_mode}, "
          f"append_mode --> {append_mode}")

    token = ""
    keymaker_url = ""
    km_credentials = ""

    keymaker_response = get_keymaker_api_response(token, keymaker_url)
    gcs_credential_json = get_keymaker_key(keymaker_response, km_credentials)

    if check_data:
        print(f"before reading from snowflake --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")
        df_3 = (spark.read.format("net.snowflake.spark.snowflake")
                .option("sfURL", "")
                .option("sfAccount", "")
                .option("sfUser", "")
                .option("sfPassword", "")
                .option("sfRole", "")
                .option("sfDatabase", sf_db)
                .option("sfSchema", sf_schema)
                .option("sfWarehouse", sf_warehouse)
                .option("use_proxy", "true")
                .option("proxy_host", "")
                .option("proxy_port", "80")
                .option("proxy_protocol", "https")
                .option("query", source_query)
                .option("autopushdown", "off")
                .load().persist())
        print(f"after reading from snowflake --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")
        df_3.cache()
        print(f"before getting df_3_count --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")
        df_3_count = df_3.count()
        print(f"after getting df_3_count --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")
        print(f"df_3 --> {df_3_count}")
        print(f"before df_3_show --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")
        df_3.show(truncate=False)
        print(f"after df_3_show --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")

        print(f"before writing the dataframe --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")
        df_3.write.format("com.google.cloud.spark.bigquery").\
            mode(append_mode)\
            .options(table=full_bq_target_table,
                     credential=gcs_credential_json,
                     project="",
                     parentProjectId="").save()
        print(f"After writing the dataframe --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")
    else:
        print(f"before reading from snowflake --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")
        spark.read.format("net.snowflake.spark.snowflake").\
            option("sfURL", "").\
            option("sfAccount", "").\
            option("sfUser", "").\
            option("sfPassword", "").\
            option("sfRole", "").\
            option("sfDatabase", sf_db).\
            option("sfSchema", sf_schema).\
            option("sfWarehouse", sf_warehouse).\
            option("use_proxy", "true").\
            option("proxy_host", "").\
            option("proxy_port", "80").\
            option("proxy_protocol", "https").\
            option("query", source_query).\
            option("autopushdown", "off").\
            load().write.format("com.google.cloud.spark.bigquery").\
            mode(append_mode).\
            options(table=full_bq_target_table,
                    credential=gcs_credential_json,
                    project="",
                    parentProjectId="").\
            save()
        print(f"After writing the dataframe --> {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")


if __name__ == '__main__':
    print("================================================================================")
    print("********** Inside main method ********** ")
    main()

# try:
#     df_1 = spark.read.format("net.snowflake.spark.snowflake")\
#         .options(sfOptions)\
#         .option("query", query)\
#         .load()
#     df_1.show(truncate=False)
# except Exception as e:
#     print(f"Exception while trying to connect to SF using net.snowflake.spark.snowflake, "
#           f"e --> {e}")

# try:
#     df_3 = spark.read.format("net.snowflake.spark.snowflake")\
#         .option("sfURL", "")\
#         .option("sfAccount", "")\
#         .option("sfUser", "")\
#         .option("sfPassword", "")\
#         .option("sfRole", "")\
#         .option("sfDatabase", "")\
#         .option("sfSchema", "")\
#         .option("sfWarehouse", "")\
#         .option("query", query)\
#         .option("autopushdown", "off")\
#         .load()
#     df_3.show(truncate=False)
# except Exception as e:
#     print(f"Exception while trying to connect to SF using net.snowflake.spark.snowflake, "
#           f"with individual options, "
#           f"e --> {e}")
#
# try:
#     from net.snowflake.spark.snowflake.Utils import SNOWFLAKE_SOURCE_NAME
#     df_2 = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
#         .options(sfOptions) \
#         .option("query", query) \
#         .load()
#     df_2.show(truncate=False)
# except Exception as e:
#     print(f"Exception while trying to connect to SF using SNOWFLAKE_SOURCE_NAME, "
#           f"e --> {e}")
