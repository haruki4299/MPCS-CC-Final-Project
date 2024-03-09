# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Haruki Yoshida <yoshida@uchicago.edu>'

import sys
import time
import driver
import boto3
import json
from botocore.exceptions import ClientError
import logging
import os

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))


# Connect to s3 Client
try:
    s3 = boto3.client('s3', region_name='us-east-1')
except Exception as e:
    print("Error has occured accessing the s3 client:", str(e))
    sys.exit(1)

# Connect to the dynamoDB client
try:
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table('yoshidah_annotations')
except Exception as e:
    print("Error has occured accessing the dynamoDB client:", str(e))
    sys.exit(1)
    
# Connect to the sns client
try:
    sns = boto3.client('sns')
    # https://stackoverflow.com/questions/36721014/aws-sns-how-to-get-topic-arn-by-topic-name
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/create_topic.html
    # This should just get the topic_arn since we already create the sns topic
    results_topic_arn_info = sns.create_topic(
        Name=config['gas']['SNSResultsTopicName']
    )
    results_topic_arn = results_topic_arn_info['TopicArn']
except Exception as e:
    print("Error has occured accessing the sns topic:", str(e))
    sys.exit(1)
    
try: 
    glacier_archive_topic_arn_info = sns.create_topic(
        Name=config['gas']['SNSGlacierArchiveTopicName']
    )
    glacier_archive_topic_arn = glacier_archive_topic_arn_info['TopicArn']
except Exception as e:
    print("Error has occured accessing the sns topic:", str(e))
    sys.exit(1)
    
results_bucket = config['gas']['ResultsBucket']

"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
      print(f"Approximate runtime: {self.secs:.2f} seconds")

# Delete all files within a directory
def delete_all_files_in_directory(directory_path):
    try:
        files = os.listdir(directory_path)
        for file in files:
            file_path = os.path.join(directory_path, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
    except OSError:
        print("Error occured while deleting files")

if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        if len(sys.argv) > 6:
            with Timer():
                driver.run(sys.argv[1], 'vcf')
                
            # Add code to save results and log files to S3 results bucket
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
            
            # Read input arguments
            file_name_without_extension = sys.argv[2]
            job_id_directory = sys.argv[3]
            user_id = sys.argv[4]
            job_id = sys.argv[5]
            name = sys.argv[6]
            email = sys.argv[7]
            role = sys.argv[8]
            
            
            # Prepare result file processing
            annotated_file = f'{file_name_without_extension}.annot.vcf'
            log_file = f'{file_name_without_extension}.vcf.count.log'
            annotated_file_path = job_id_directory + annotated_file
            log_file_path = job_id_directory + log_file
            annotated_file_object_name = f"{config['gas']['OwnerName']}/{user_id}/{job_id}/{annotated_file}"
            log_file_object_name = f"{config['gas']['OwnerName']}/{user_id}/{job_id}/{log_file}"
            
            # 1. Upload the results file
            try:
                response = s3.upload_file(annotated_file_path, results_bucket, annotated_file_object_name)
            except ClientError as e:
                print("Failed to upload annotated result file")
                logging.error(e)
                
            # 2. Upload the log file
            try:
                response = s3.upload_file(log_file_path, results_bucket, log_file_object_name)
            except ClientError as e:
                print("Failed to upload annotated result file")
                logging.error(e)
                
            # 3. Clean up (delete) local job files
            # https://www.tutorialspoint.com/How-to-delete-all-files-in-a-directory-with-Python
            delete_all_files_in_directory(job_id_directory)
                
            timestamp = int(time.time())
            
            try:
                print("Update table item")
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
                table.update_item(
                    Key={'job_id': job_id},
                    UpdateExpression='SET job_status = :complete, s3_results_bucket = :s3_results_bucket, s3_key_result_file = :s3_key_result_file, s3_key_log_file = :s3_key_log_file, complete_time = :complete_time',
                    ConditionExpression= 'job_status = :running',
                    ExpressionAttributeValues={":running": "RUNNING",
                                               ":complete": "COMPLETED", 
                                               ":s3_results_bucket":results_bucket, 
                                               ":s3_key_result_file":annotated_file_object_name,
                                               ":s3_key_log_file":log_file_object_name,
                                               ":complete_time":timestamp
                                               }
                )
            except e:
                print(f"Failed to update item from table with job id: {job_id}")
                logging.error(e)
            
            # Publish a notification message to the SNS topics
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
            try:
                print("Sending Message to Results Topic")
                # Send to Results topic
                data = {
                    "job_id": job_id,
                    "name": name,
                    "email": email,
                    "role": role,
                    "status": "completed",
                    "message": f"Job {job_id} has been completed successfully. Annotated results and log files are available in S3."
                }
                sns.publish(
                    TopicArn=results_topic_arn,
                    Message=json.dumps(data)
                )
            except Exception as e:
                print(f"Error publishing notification message to Results SNS topic: {str(e)}")
                logging.error(e)
                
            if role == 'free_user':
                try:
                    print("Sending Message to Glacier Topic")
                    # Send to Glacier Archive topic
                    data = {
                        "job_id": job_id,
                        "complete_time": timestamp,
                    }
                    sns.publish(
                        TopicArn=glacier_archive_topic_arn,
                        Message=json.dumps(data)
                    )
                except Exception as e:
                    print(f"Error publishing notification message to Glacier Archive SNS topic: {str(e)}")
                    logging.error(e)
                    
        else:
            print("Input: filepath, file_name_without_extension, job_id_directory, user_id, user_profile is required.")
        
    else:
        print("A valid .vcf file must be provided as input to this program.")

### EOF
