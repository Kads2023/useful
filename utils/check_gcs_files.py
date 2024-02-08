
import common_functions as utils
import gcp_utils
import os
import datetime
import base64
import pgpy
import requests


env = os.getenv('ENVIRONMENT')
home_dir = os.getenv('HOME')

prop_home = ''


keymaker_response = utils.get_keymaker_api_response('https://keymakerapi.g.paypalinc.com:21358/kmsapi/v1/keyobject/all'
                                                    ,f'{home_dir}/{prop_home}/common/dependency-files/identity.txt')

gcs_credential_json = utils.get_keymaker_key(keymaker_response, '')

gcs_client = gcp_utils.get_gcs_client(gcs_credential_json)


gcs_bucket_name = ''

gcs_bucket = gcs_client.bucket(gcs_bucket_name)


for blob in gcs_bucket.list_blobs(prefix='/year=2024/month=02/day=06/actual/'):
    file_name = blob.name.split("/")[-1]
    kms_key = blob.kms_key_name
    print(f"file_name --> {file_name}, kms_key --> {kms_key}")


for blob in gcs_bucket.list_blobs(prefix='/year=2024/month=02/day=06/encrypted/'):
    file_name = blob.name.split("/")[-1]
    kms_key = blob.kms_key_name
    print(f"file_name --> {file_name}, kms_key --> {kms_key}")


nas_location = "/data/"
target_table = "table_name"


for blob in gcs_bucket.list_blobs(prefix='/year=2024/month=02/day=06/encrypted/'):
    file_name = blob.name.split("/")[-1]
    print(f"file_name --> {file_name}")
    nas_file_location = f"{nas_location}/{target_table}/"
    actual_file_name = f"{nas_file_location}/encrypted/{file_name}"
    blob.download_to_filename(actual_file_name)


def get_decrypt_key_details(keymaker_response, keymaker_keyname):
    """ Get the decryption keys from KM """
    for key in keymaker_response["pgpkeypairs"]:
        if key["pgpkeypair"]["name"] == keymaker_keyname:
            private_key = base64.b64decode(key["pgpkeypair"]["secret_keyring"]["encoded_keyring"])
            passphrase = base64.b64decode(key["pgpkeypair"]["secret_keyring"]["encoded_passphrase"])
            return private_key, passphrase


private_key, passphrase = get_decrypt_key_details(keymaker_response, "")
key, _ = pgpy.PGPKey.from_blob(private_key)


source_path_list = [f"{nas_location}/{target_table}/actual/{target_table}_20240206_000000000012.csv"]
dest_path_list = [f"{nas_location}/{target_table}/encrypted/{target_table}_20240206_000000000012.csv.gpg"]

for source_path, dest_path in zip(source_path_list, dest_path_list):
    print(f"PGP Encryption started for {source_path}")
    with open(source_path, "rb") as file:
        file_data = file.read()
        print(f"read file_data")
        with key.unlock(passphrase.decode("utf-8", 'ignore')):
            print("inside key.unlock")
            message = pgpy.PGPMessage.new(file_data)
            print("after message")
            enc_message = key.pubkey.encrypt(message)
            print("after enc_message")
            with open(dest_path, "w") as encrypted_file:
                enc_content = str(enc_message)
                encrypted_file.write(enc_content)


for source_path, dest_path in zip(dest_path_list, source_path_list):
    print(f"PGP Decryption started for {source_path}")
    enc_content = pgpy.PGPMessage.from_file(f"{source_path}")
    with open(dest_path, "w") as decrypted_file:
        with key.unlock(passphrase.decode("utf-8", 'ignore')):
            dec_content = key.decrypt(enc_content).message
            dec_content = str(dec_content.decode("utf-8", 'ignore'))
            decrypted_file.write(dec_content)

import pandas as pd
data = pd.read_csv(f"{nas_location}/{target_table}/actual/{target_table}_20240206_000000000012.csv", nrows=100, error_bad_lines=False)
print(data)

for blob in gcs_bucket.list_blobs \
        (prefix=f'/year=2024/month=02/day=06/actual/{target_table}_20240206_000000000012.csv'):
    file_name = blob.name.split("/")[-1]
    print(f"file_name --> {file_name}")
    nas_file_location = f"{nas_location}/{target_table}/"
    actual_file_name = f"{nas_file_location}/actual/{target_table}_20240206_000000000012_new.csv"
    blob.download_to_filename(actual_file_name)




