import boto3
from botocore.exceptions import ClientError
import subprocess
import sys
import json
import os

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))


# Define the data directory
job_directory = config['code']['JobDirectory']
# Create the directory and its parent directories if they don't exist
if not os.path.exists(job_directory):
    os.makedirs(job_directory)
    
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
    request_queue_url_info = sqs.get_queue_url(
        QueueName=config['gas']['SQSRequestQueueName']
    )
    request_queue_url = request_queue_url_info['QueueUrl']
except Exception as e:
    print("Error has occured accessing the sqs queue url:", str(e))
    sys.exit(1)

# Poll the message queue in a loop
while True:
    # Attempt to read a message from the queue
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html#
    # Use long polling - DO NOT use sleep() to wait between polls
    # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-long-polling
    try:
        message_recieved = sqs.receive_message(
            QueueUrl=request_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,
        )
    except Exception as e:
        print(f"Error has occured while recieveing message from {request_queue_url}", str(e))
        sys.exit(1) # If cannot read messages critical
        
    try:
        messages = message_recieved['Messages']
    except:
        # Must have been no messages
        continue
    
    # If message read, extract job parameters from the message body as before
    # If message empty will go to next loop
    for message in messages:
        message_body = message['Body']
        receipt_handle = message['ReceiptHandle']
        message_json = json.loads(message_body)
        data = json.loads(message_json['Message'])
        job_id = data['job_id']
        user_id = data['user_id']
        input_file_name = data['input_file_name']
        s3_inputs_bucket = data['s3_inputs_bucket']
        s3_key_input_file = data['s3_key_input_file']
        user_profile = data['user_profile']
        user_name = user_profile['name']
        user_email = user_profile['email']
        user_role = user_profile['role']
    
        print(f"message for job id: {job_id} recieved")
            
        # Use a local directory structure that makes it easy to organize
        # multiple running annotation jobs
            
        # Create user directory to store working files
        user_directory = job_directory + user_id + "/"       
        if not os.path.exists(user_directory):
            os.makedirs(user_directory)      
        # create individual job directories
        job_id_directory = user_directory + job_id + "/"   
        if not os.path.exists(job_id_directory):
            os.makedirs(job_id_directory)
        filepath = job_id_directory + input_file_name
        
        # Get the input file S3 object and copy it to a local file
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-download-file.html
            s3.download_file(s3_inputs_bucket, s3_key_input_file, filepath)
        except Exception as e:
            print(f"Error has occured downloading the inputfile: {input_file_name} job_id: {job_id} from the s3 client:", str(e))
            sys.exit(1) # If cannot read messages critical
            
        print(f"Downloaded file {s3_key_input_file} from {s3_inputs_bucket}")
        
        # Update Table to Running
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
            table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='SET job_status = :running',
                ConditionExpression= 'job_status = :pending',
                ExpressionAttributeValues={":running": "RUNNING", ":pending": "PENDING"}
        )
        except Exception as e:
            print(f"Error has occured updating the table item to running for job_id: {job_id}", str(e))
            sys.exit(1) # If cannot read messages critical
        
        print("Table Updated to RUNNING")
        
        # Launch annotation job as a background process
        file_name_without_extension = input_file_name.split('.')[0]
        try:
            command = ['python', 'run.py', filepath, file_name_without_extension, job_id_directory, user_id, job_id, user_name, user_email, user_role]
            job = subprocess.Popen(command)
        except Exception as e:
            print(f"Error has occured launching the annotation job for job_id: {job_id}", str(e))
            sys.exit(1) # If cannot read messages critical
            
        # Delete the message from the queue, if job was successfully submitted
        if job:
            try:
                sqs.delete_message(
                    QueueUrl=request_queue_url,
                    ReceiptHandle=receipt_handle
                )
            except botocore.exceptions.ClientError as e:
                print(f"Error when deleting message with Receipt Handle: {receipt_handle}", str(e))
                sys.exit(1) # Critical error if we cannot delete messages. Exit program
        
        print("Delete of message complete")