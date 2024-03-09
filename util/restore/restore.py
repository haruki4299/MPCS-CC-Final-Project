# restore.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

# https://docs.aws.amazon.com/amazonglacier/latest/dev/downloading-an-archive-two-steps.htmls

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
config.read('restore_config.ini')

# Connect to the dynamoDB client
try:
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(config['gas']['DynamoTable'])
except Exception as e:
    print("Error has occured accessing the dynamoDB client:", str(e))
    sys.exit(1)

# Connect to sns client
try:
    sns = boto3.client('sns')
except Exception as e:
    print(f"Error connecting to the sns client: {e}")
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
    restore_queue_url_info = sqs.get_queue_url(
        QueueName=config['gas']['SQSGlacierRestoreQueueName']
    )
    restore_queue_url = restore_queue_url_info['QueueUrl']
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
# Poll the message queue in a loop
while True:
    # Attempt to read a message from the queue
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html#
    # Use long polling - DO NOT use sleep() to wait between polls
    # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-long-polling
    try:
        message_recieved = sqs.receive_message(
            QueueUrl=restore_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,
        )
    except Exception as e:
        print(f"Error has occured while recieveing message from {restore_queue_url}", str(e))
        sys.exit(1) # If cannot read messages critical
        
    try:
        messages = message_recieved['Messages']
    except:
        # Must have been no messages
        continue
    
    current_time = datetime.utcnow()
        
    for message in messages:
        message_body = message['Body']
        receipt_handle = message['ReceiptHandle']
        message_json = json.loads(message_body)
        data = json.loads(message_json['Message'])
        job_id = data['job_id']
        
        # Figure out if job data is already in glacier (some jobs still may be running and will be in glacier after)
        # Look for the results_file_archive_id
        try:
            table_response = table.get_item(Key={'job_id': job_id})
        except ClientError as e:
            print(f"Error has occured while retrieving object with id: {job_id} from dynamo db", str(e))
            sys.exit(1)
            
        item = table_response.get('Item')
        try:
            # Retrieve the archive ID. If it doesn't exist yet, it will continue to next
            results_file_archive_id = item['results_file_archive_id']
        except:
            print("Not yet")
            continue # The job still is in the process of running or not expired yet, skip this job_id and go to next
        
        print(f"Initiating job for {job_id}")
        
        # Initiate job
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/initiate_job.html
        # try expedite first
        expedite_failed = False
        try:
            response = glacier.initiate_job(
                vaultName=config['gas']['GlacierVaultName'],
                jobParameters={
                    'Type':'archive-retrieval',
                    'ArchiveId':results_file_archive_id,
                    'Tier':'Expedited'
                }
            )
        except ClientError as e:
            print(f"Expedited retrieval not available. Fallback to Standard. Error Message: {e}")
            expedite_failed = True
            
        try:
            if expedite_failed:
                response = glacier.initiate_job(
                    vaultName=config['gas']['GlacierVaultName'],
                    jobParameters={
                        'Type':'archive-retrieval',
                        'ArchiveId':results_file_archive_id,
                        'Tier':'Standard'
                    }
                )
        except ClientError as e:
            print(f"Error: Failed glacier retrieval jobs. {e}")
            sys.exit(1)
            
        glacier_job_id = response['jobId']
        data = {
            'job_id': job_id,
            'archiveId': results_file_archive_id,
            "glacier_job_id": glacier_job_id
        }
        
        # Publish SNS message indicating the glacier restore job was initiated
        try:
            sns.publish(
                TopicArn=config['gas']['SNSGlacierJobIDARN'],
                Message=json.dumps(data)
            )
        except ClientError as e:
            print(f"Failed to publish message to sns topic for glacier job ids")
            sys.exit(1)
        
        # Delete message from queue for job_id
        try:
            sqs.delete_message(
                QueueUrl=restore_queue_url,
                ReceiptHandle=receipt_handle
            )
        except ClientError as e:
            print(f"Error when deleting message from {restore_queue_url} with Receipt Handle: {receipt_handle}", str(e))
            sys.exit(1) # Critical error if we cannot delete messages. Exit program

### EOF