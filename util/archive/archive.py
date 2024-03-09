# archive.py
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
config.read('archive_config.ini')

# Connect to s3 Client
try:
    s3 = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
except Exception as e:
    print("Error has occured accessing the s3 client:", str(e))
    sys.exit(1)
    
# Connect to Glacier Client
try:
    glacier = boto3.client('glacier')
except Exception as e:
    print("Error has occured accessing the Glacier client:", str(e))
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
    glacier_archive_queue_url_info = sqs.get_queue_url(
        QueueName=config['gas']['SQSGlacierArchiveQueueName']
    )
    glacier_archive_queue_url = glacier_archive_queue_url_info['QueueUrl']
except ClientError as e:
    print("Error has occured accessing the sqs queue url:", str(e))
    sys.exit(1)

# Add utility code here
# Poll the message queue in a loop
while True:
    # Attempt to read a message from the queue
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html#
    # Use long polling - DO NOT use sleep() to wait between polls
    # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-long-polling
    try:
        message_recieved = sqs.receive_message(
            QueueUrl=glacier_archive_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,
        )
    except Exception as e:
        print(f"Error has occured while recieveing message from {glacier_archive_queue_url}", str(e))
        sys.exit(1) # If cannot read messages critical
        
    try:
        messages = message_recieved['Messages']
    except:
        # Must have been no messages
        print("Empty Message Poll")
        continue
    
    current_time = datetime.utcnow()
    
    # Read Each Message in the Queue
    for message in messages:
        message_body = message['Body']
        receipt_handle = message['ReceiptHandle']
        message_json = json.loads(message_body)
        data = json.loads(message_json['Message'])
        job_id = data['job_id']
        complete_time = datetime.utcfromtimestamp(data['complete_time'])
        time_diff = current_time - complete_time
        # If the time has been more than 5 minutes
        if time_diff > timedelta(minutes=5):
            # Archive to glacier
            print("Archive!")
            # First get info about the object from the table
            try:
                table_response = table.get_item(Key={'job_id': job_id})
            except ClientError as e:
                print(f"Error has occured while retrieving object with id: {job_id} from dynamo db", str(e))
                continue # Skip to next message
            
            item = table_response.get('Item')
            s3_key_result_file = item['s3_key_result_file']
            
            # Here is we encounter an error, the first two operations are safe to abort and attempt the next
            # Last one we must exit
            # First get object data from s3
            try:
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
                s3_object = s3.get_object(
                    Bucket=config['gas']['ResultsBucket'],
                    Key=s3_key_result_file
                )
                object_data = s3_object['Body'].read()
            except ClientError as e:
                print(f"Error has occured while retrieving object from s3 results bucket", str(e))
                continue # Skip to Next Message
            
            print("Get s3 Object Complete")
            
            # Upload to Glacier
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/upload_archive.html
            try:
                glacier_response = glacier.upload_archive(
                    vaultName=config['gas']['GlacierVaultName'],
                    body=object_data
                )
            except ClientError as e:
                print(f"Error has occured while uploading object from s3 to Glacier", str(e))
                continue # Skip to Next Message
            
            print("upload to glacier complete")
            
            results_file_archive_id = glacier_response['archiveId']
            
            # Add the archive ID to the dynamo db
            try:
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
                table.update_item(
                    Key={'job_id': job_id},
                    UpdateExpression="SET results_file_archive_id = :archive_id",
                    ExpressionAttributeValues={":archive_id": results_file_archive_id,}
                )
            except ClientError as e:
                print(f"Error has occured while putting archive ID into dynamo db (JOBID = {job_id}, ArchiveID = {results_file_archive_id})", str(e))
                sys.exit(1)
            
            print("table update complete")
            
            # Delete the s3 object from the database
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
            try:
                s3.delete_object(
                    Bucket=config['gas']['ResultsBucket'],
                    Key=s3_key_result_file
                )
            except ClientError as e:
                print(f"Error has occured deleting an archived object from s3", str(e))
                sys.exit(1)
                
            # If we made it to this point we should delete the message off the queue
            try:
                sqs.delete_message(
                    QueueUrl=glacier_archive_queue_url,
                    ReceiptHandle=receipt_handle
                )
            except ClientError as e:
                print(f"Error when deleting message from {glacier_archive_queue_url} with Receipt Handle: {receipt_handle}", str(e))
                sys.exit(1) # Critical error if we cannot delete messages. Exit program

            print("Delete of message complete")
### EOF