#! python3
import logging
import boto3
import json
import os
import sys
import threading
import pprint
import datetime


open('..\\debug.log','w').close()
logging.basicConfig(level=logging.DEBUG, filename='..\\debug.log', format='%(asctime)s - %(levelname)s - %(message)s')
logging.disable(logging.DEBUG)

""" 
DONE: login to S3. This is done via %userprofile%\.aws\credentials and config files. 
Also default output set to json format and default region is Ireland.
"""

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()
    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)\n" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


# DONE: read files from specified directory, dealing with folder exclusions
# DONE: included file exclusion by name and by extention
def s3_upload_directory_tree(directory, excl, s3_files):
    count_processed_files = 0
    count_copied_files = 0
    all_local_files_processed = []
    if os.path.exists(directory):
        try:
            for folder_name, subfolders, filenames in os.walk(directory):
                # DONE: excluding some pre-defined folders
                subfolders[:] = [item.lower() for item in subfolders]
                subfolders[:] = [item for item in subfolders if item not in excl['dirs']]
                filenames[:] = [item for item in filenames if item not in excl['files']]
                filenames[:] = [item for item in filenames if not item.endswith(tuple(excl['file_extensions']))]
                for file in filenames:
                    file_path = (folder_name + '\\' + file)
                    file_key = ('root' + folder_name + '\\' + file).replace(directory, '').replace('\\', '/')
                    all_local_files_processed.append(file_key)
                    count_processed_files += 1
                    # Compare if new files and copy if new right away
                    if file_key not in s3_files.keys():
                        response = s3.upload_file(file_path,
                                                  'backup-to-cloud',
                                                  file_key,
                                                  Callback=ProgressPercentage(file_path)
                                                  )
                        count_copied_files += 1
                    # Compare is local copy of file size is different from the one on S3. Copy if yes.
                    elif os.path.getsize(file_path) != s3_files[file_key]['Size']:
                        response = s3.upload_file(file_path,
                                                 'backup-to-cloud',
                                                 file_key,
                                                 Callback=ProgressPercentage(file_path)
                                                 )
                        count_copied_files += 1
                    # Compare is local copy of file is newer than the one ons S3. Ask if overwrite in S3.
                    elif datetime.datetime.utcfromtimestamp(
                            os.path.getmtime(file_path)) > s3_files[file_key]['LastModified'].replace(tzinfo=None):
                        upload_modified = None
                        while upload_modified not in ['Yes', 'yes', 'No', 'no', 'y', 'n', '']:
                            upload_modified = input(
                                '\nFile \"%s\" has modified date later than on S3. Do you want to overwrite it on S3? \
    (Yes / No): ' % file_path)
                        if upload_modified in ['Yes', 'yes', 'y']:
                            response = s3.upload_file(file_path,
                                                      'backup-to-cloud',
                                                      file_key,
                                                      Callback=ProgressPercentage(file_path)
                                                      )
                            count_copied_files += 1
                        else:
                            print('File not modified on S3.\n')
            log_file.write(datetime.datetime.now().strftime(
                '%Y/%m/%d %H:%M:%S') + ' - INFO - All files copied to S3 successfully.\n')
        except Exception as err:
            print('Failure while walking through directory tree: \"%s\"' % directory)
            log_file.write(datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S') + ' - ERROR - failure while walking \
through directory tree and copying files to S3. Check error_log.txt log file. Error message: %s\n')
            error_log = open('..\\error_log.txt', 'w')
            error_log.write('Working directory:\n')
            error_log.write('\t%s' % directory)
            error_log.write('\nLast touched file name:\n')
            error_log.write('\t%s' % file)
            error_log.write('\nResponse from S3:\n')
            error_log.write('\t%s' % pprint.pformat(response))
            sys.exit(err)
    else:
        print('Directory \"%s\" does not exist' % directory)
        log_file.write(datetime.datetime.now().strftime(
            '%Y/%m/%d %H:%M:%S') + ' - INFO - Specified local directory \"%s \"not found.\n' % directory)
        sys.exit()
    print('Total files processed: %s' % count_processed_files)
    print('Total files copied: %s' % count_copied_files)
    return all_local_files_processed


def s3_delete_files(local_files, on_s3_files):
    files_on_s3_but_not_on_local_disk = [item for item in on_s3_files if item not in local_files]
    files_on_local_disk_but_not_on_s3 = [item for item in local_files if item not in on_s3_files]
    if files_on_s3_but_not_on_local_disk:
        log_file.write(datetime.datetime.now().strftime(
            '%Y/%m/%d %H:%M:%S') + ' - INFO - found files on S3 that are no longer present on disk.\n')
        delete_from_s3 = None
        while delete_from_s3 not in ['Yes', 'yes', 'No', 'no', 'y', 'n', '']:
            delete_from_s3 = input(
                '\nBelow files are on S3, but no longer exists on local disk: \n\"%s\"\nDo you want to delete it \
from S3? (Yes / No): ' % pprint.pformat(files_on_s3_but_not_on_local_disk))
        if delete_from_s3 in ['Yes', 'yes', 'y']:
            delete_dic = []
            for item in files_on_s3_but_not_on_local_disk:
                delete_dic.append({'Key':item})
            try:
                s3.delete_objects(Bucket='backup-to-cloud', Delete={'Objects':delete_dic})
                print('All deleted from S3.')
                log_file.write(datetime.datetime.now().strftime(
                    '%Y/%m/%d %H:%M:%S') + ' - INFO - All files successfully deleted from S3 bucket.\n')
            except Exception as err:
                print('Failure while deleting files from S3 bucket.')
                log_file.write(datetime.datetime.now().strftime(
                    '%Y/%m/%d %H:%M:%S') + ' - ERROR - failure while deleting files from S3 bucket. Error message: %s\n'
                               % err)
                sys.exit(err)
        else:
            print('Files not deleted from S3.\n')
    if files_on_local_disk_but_not_on_s3:
        print('By the way, for some reason there are files on disk, but not on S3:\n\"%s\"'
              % pprint.pformat(files_on_local_disk_but_not_on_s3))


def s3_list_files():
    try:
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket='backup-to-cloud', PaginationConfig={'PageSize': 1000})
        object_list = []
        for page in page_iterator:
            try:
                object_list += [item for item in page['Contents']]
            except:
                print('S3 bucket empty.')
                log_file.write(datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S') + ' - INFO - S3 bucket empty.\n')
            # convert list to dictionary where dictionary keys = s3 file keys. This makes easier to compare objects later
        object_dictionary = {}
        for item in object_list:
            object_dictionary.update({item['Key']:item})
            # print(pprint.pformat(object_dictionary))
        log_file.write(datetime.datetime.now().strftime(
            '%Y/%m/%d %H:%M:%S') + ' - INFO - S3 bucket file list retrieved successfully.\n')
        return object_dictionary
    except Exception as err:
        print('Failure while retrieving files list from S3 bucket.')
        log_file.write(datetime.datetime.now().strftime(
            '%Y/%m/%d %H:%M:%S') + ' - ERROR - failure while retrieving files list from S3 bucket. Error message: %s\n'
                       % err)
        sys.exit(err)


# Load and read exclusions file in JSON format, where exclusion dirs, files and file_extentions specified
def load_exclusions():
    try:
        exclusions_file = open('..\\exclusions.txt', 'r')
        exclude_dic = json.loads(exclusions_file.read())
        exclusions_file.close()
        exclude_dic['dirs'] = [item.lower() for item in exclude_dic['dirs']]
        log_file.write(datetime.datetime.now().strftime(
                '%Y/%m/%d %H:%M:%S') + ' - INFO - Exclusions loaded successfully from file \'exclusions.txt\'.\n')
        return exclude_dic
    except Exception as err:
        print('Failed to load exclusions from file \'exclusions.txt\'')
        log_file.write(datetime.datetime.now().strftime(
            '%Y/%m/%d %H:%M:%S') + ' - ERROR - failure while loadind eclusions from file \"%s\". Error message: %s\n'
                       % ('exclusions.txt', err))
        sys.exit(err)

"""MAIN PROGRAM STARTS HERE"""
# Log files
global log_file
open('..\\log_file.txt','w').close()
log_file = open('..\\log_file.txt','a',encoding='utf-8')
# Create S3 client. In other words call service resource
global s3
s3 = boto3.client('s3')
# Load exclusions
exclusions = load_exclusions()
# Get list of files on S3 bucket before upload
files_on_s3 = s3_list_files()
# Send directory to function that reads directory tree and copy file to S3
backup_directory_file = open('..\\backup_dir.txt', 'r')
files_on_disk = s3_upload_directory_tree(backup_directory_file.readline(), exclusions, files_on_s3)
backup_directory_file.close()
# Get list of files on S3 bucket after upload
files_on_s3 = s3_list_files()
# Send local and s3 file lists to s3 delete function and provide means to delete files on s3
s3_delete_files(files_on_disk, list(files_on_s3.keys()))
print('\nAll tasks completed successfully! :):):)')
os.system('pause')
log_file.write(datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S') + ' - INFO - All tasks completed successfully!\n')
log_file.close()