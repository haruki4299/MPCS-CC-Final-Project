# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import json
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('thaw_config.ini')

# Connect to s3 Client
try:
    s3 = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
except Exception as e:
    print("Error has occured accessing the s3 client:", str(e))
    sys.exit(1)

# Connect to the dynamoDB client
try:
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(config['gas']['DynamoTable'])
except Exception as e:
    print("Error has occured accessing the dynamoDB client:", str(e))
    sys.exit(1)

# Connect to SQS and get the message queue
try:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
    sqs = boto3.client('sqs')
except Exception as e:
    print("Error has occured accessing the sqs queue:", str(e))
    sys.exit(1)
try:   
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/get_queue_url.html
    job_id_queue_url_info = sqs.get_queue_url(
        QueueName=config['gas']['SQSGlacierJobIDQueueName']
    )
    job_id_queue_url = job_id_queue_url_info['QueueUrl']
except Exception as e:
    print("Error has occured accessing the sqs queue url:", str(e))
    sys.exit(1)

# Connect to Glacier
try:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
    glacier = boto3.client('glacier')
except Exception as e:
    print("Error has occured accessing the glacier client:", str(e))
    sys.exit(1)

# Add utility code here
while True:
    # Attempt to read a message from the queue
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html#
    # Use long polling - DO NOT use sleep() to wait between polls
    # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-long-polling
    try:
        message_recieved = sqs.receive_message(
            QueueUrl=job_id_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,
        )
    except Exception as e:
        print(f"Error has occured while recieveing message from {job_id_queue_url}: {e}")
        sys.exit(1) # If cannot read messages critical
        
    try:
        messages = message_recieved['Messages']
    except:
        # Must have been no messages
        continue
    
    for message in messages:
        message_body = message['Body']
        receipt_handle = message['ReceiptHandle']
        message_json = json.loads(message_body)
        data = json.loads(message_json['Message'])
        job_id = data['job_id']
        glacier_job_id = data['glacier_job_id']
        archieve_id = data['archiveId']
        
        # Figure out if job is complete
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/describe_job.html
            job_status = glacier.describe_job(
                vaultName=config['gas']['GlacierVaultName'],
                jobId=glacier_job_id
            )
        except ClientError as e:
            print(f"Failed describe_job() for job in glacier: {glacier_job_id}")
            print(e)
            sys.exit(1)

        if job_status['Completed']:
            # If the job is completed 1. download and put in s3 2. Delete archive in glacier 3. update table and get rid of results_file_archive_id
            # 1. download and put in s3
            try:
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/get_job_output.html
                response = glacier.get_job_output(
                    vaultName=config['gas']['GlacierVaultName'],
                    jobId=glacier_job_id
                )
            except ClientError as e:
                print(f"Failed get_job_output() for job in glacier with id={glacier_job_id}: {e}")
                sys.exit(1)
            
            # Get parameters from the Dynamo table
            try:
                table_response = table.get_item(Key={'job_id': job_id})
            except ClientError as e:
                print(f"Error has occured while retrieving object with id: {job_id} from dynamo db: {e}")
                sys.exit(1)
                
            # Read parameters
            item = table_response.get('Item')
            s3_results_bucket = item['s3_results_bucket']
            input_file_name = item['input_file_name']
            s3_key_result_file = item['s3_key_result_file']
            
            # Create folder to store file
            job_dir = f"{job_id}"
            os.makedirs(job_dir, exist_ok=True)
            
            # Write downloaded data to file
            local_file_path = f"{job_dir}/{input_file_name}"
            with open(local_file_path, 'wb') as file:
                file.write(response['body'].read())
            
            
            # Upload to s3
            try:
                s3.upload_file(local_file_path, s3_results_bucket, s3_key_result_file)
                print("File uploaded successfully to S3!")
            except ClientError as e:
                print(f"Error uploading file to S3: {e}")
                sys.exit(1)
                
            # Delete the Local Files Created
            # Delete file https://sentry.io/answers/delete-a-file-or-folder-in-python/#:~:text=To%20delete%20a%20folder%20that,function%20from%20Python's%20shutil%20module.
            os.remove(local_file_path)
            # Delete directory
            os.removedirs(job_dir)
                
            # 2. Delete archive in glacier 
            try:
                # Delete the restored object from Glacier
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/delete_archive.html
                glacier.delete_archive(
                    vaultName=config['gas']['GlacierVaultName'],
                    archiveId=archieve_id
                )
                print("Restored object deleted from Glacier")
            except Exception as e:
                print(f"Error deleting restored object from Glacier: {e}")
                sys.exit(1)
                
            # 3. update dynamo table and get rid of results_file_archive_id
            try:
                # Update the DynamoDB table
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
                response = table.update_item(
                    Key={'job_id': job_id},
                    UpdateExpression='REMOVE results_file_archive_id',
                    ConditionExpression='attribute_exists(results_file_archive_id)'
                )
                print("DynamoDB table updated")
            except Exception as e:
                print(f"Error updating DynamoDB table: {e}")
                sys.exit(1)
                
            # Delete message from queue
            try:
                sqs.delete_message(
                    QueueUrl=job_id_queue_url,
                    ReceiptHandle=receipt_handle
                )
            except ClientError as e:
                print(f"Error when deleting message from {job_id_queue_url} with Receipt Handle: {receipt_handle}", str(e))
                sys.exit(1) # Critical error if we cannot delete messages. Exit program
            

            print(f"Completed for {job_id}")
### EOF