import os
import sys
import json
import time
import datetime
import subprocess

home_dir = os.getenv('HOME')


def get_files_to_purge(purge_json, file_type, directory):
    files_to_purge = []
    print('Scanning directory %s for file format %s that have exceeded retention period of %s days' % (
    directory, file_type, purge_json[file_type]['retention_days']))
    for file_name in os.listdir(directory):
        file_name = os.path.join(directory, file_name)
        if file_name.endswith(file_type) and not "connection_pool" in file_name:
            if os.stat(file_name).st_mtime < time.time() - purge_json[file_type]['retention_days'] * 86400:
                files_to_purge.append(file_name)
    return files_to_purge


def get_recursive_files_to_purge(purge_json, file_type, directory):
    files_to_purge = []
    print('Scanning directory %s recursively for file format %s that have exceeded retention period of %s days' % (
    directory, file_type, purge_json[file_type]['retention_days']))
    for file_name in os.listdir(directory):
        file_name = os.path.join(directory, file_name)
        if file_name.endswith(file_type) and not "connection_pool" in file_name:
            if os.stat(file_name).st_mtime < time.time() - purge_json[file_type]['retention_days'] * 86400:
                files_to_purge.append(file_name)
        elif os.path.isdir(file_name):
            files_to_purge.extend(get_recursive_files_to_purge(purge_json, file_type, file_name))
    return files_to_purge


def get_recursive_dir_to_purge(purge_json, file_type, directory):
    dir_to_purge = []
    print('Scanning directory %s recursively for empty directories' % (directory))
    for file_name in os.listdir(directory):
        file_name = os.path.join(directory, file_name)
        if os.path.isdir(file_name):
            if not os.listdir(file_name):
                print("Empty directory %s" %(file_name))
                if os.stat(file_name).st_mtime < time.time() - purge_json[file_type]['retention_days'] * 86400:
                    dir_to_purge.append(file_name)
            else:
                dir_to_purge.extend(get_recursive_dir_to_purge(purge_json, file_type, file_name))
    return dir_to_purge


with open('/data/purge_files.json') as f:
    purge_json = json.load(f)

f.close()
print(purge_json)
files_to_purge = []


for file_type in purge_json.keys():
    if 'directories' in purge_json[file_type].keys():
        directories = purge_json[file_type]['directories']
        for directory in directories:
            files_to_purge.extend(get_files_to_purge(purge_json, file_type, directory))
    if 'recursive_directories' in purge_json[file_type].keys():
        recursive_directories = purge_json[file_type]['recursive_directories']
        for recursive_directory in recursive_directories:
            files_to_purge.extend(get_recursive_files_to_purge(purge_json, file_type, recursive_directory))


files_to_purge = list(set(files_to_purge))


dir_to_purge = []
for file_type in purge_json.keys():
    if 'recursive_directories' in purge_json[file_type].keys():
        recursive_directories = purge_json[file_type]['recursive_directories']
        for recursive_directory in recursive_directories:
            dir_to_purge.extend(get_recursive_dir_to_purge(purge_json, file_type,recursive_directory))

dir_to_purge = list(set(dir_to_purge))
