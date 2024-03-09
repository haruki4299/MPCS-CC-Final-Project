# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime, timedelta

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for, jsonify)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]
  

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():
  user_id = session.get('primary_identity')
  profile = get_profile(identity_id=user_id)
  user_name = profile.name
  user_email = profile.email
  user_role = profile.role
  user_profile = {
    'name' : user_name,
    'email' : user_email,
    'role' : user_role
  }

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))

  # Extract the job ID from the S3 key
  s3_path = s3_key.split("/", maxsplit=2)
  object_name = s3_path[2]
  job_id = object_name.split("~")[0]
  filename = object_name.split("~")[1]
  

  # Persist job to database
  # https://www.programiz.com/python-programming/datetime/current-time#google_vignette
  timestamp = int(time.time())
  data = { "job_id": job_id,
      "user_id": user_id,
      "input_file_name": filename,
      "s3_inputs_bucket": bucket_name,
      "s3_key_input_file": s3_key,
      "submit_time": timestamp,
      "job_status": "PENDING"
  }

  # Connect to the dynamoDB client
  try:
      dynamo = boto3.resource('dynamodb')
      table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  except ClientError as e:
    app.logger.error(f"Unable to connect with the dynamoDB client: {e}")
    return abort(500)

  try:
      table.put_item(Item = data)
  except ClientError as e:
    app.logger.error(f"Error writing job item to database: {e}")
    return abort(500)

  # Add extra data to send to sns
  data['user_profile'] = user_profile

  # Send message to request queue
  # Connect to sns client and publish topic
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
  try:
    sns = boto3.client('sns')
    sns.publish(
        TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'],
        Message=json.dumps(data)
    )
  except ClientError as e:
    app.logger.error(f"Error publishing notification message to SNS topic: {e}")
    return abort(500)
  
  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  user_id = session['primary_identity']
  if user_id:
    try:
      # Connect to the dynamoDB client
      dynamo = boto3.resource('dynamodb')
      table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    except ClientError as e:
        app.logger.error(f"Unable to connect with the dynamoDB client: {e}")
        return abort(500)
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html
        response = table.query(
          IndexName='user_id_index',
          KeyConditionExpression='user_id = :user_id',
          ExpressionAttributeValues={
              ':user_id': user_id
          }
        )
        items = response.get('Items', [])
    except Exception as e:
        app.logger.error(f"Error retrieving user jobs from DynamoDB: {e}")
        abort(500)
    annotations = []
    for item in items:
      annotation = {
          'job_id': item['job_id'],
          'submit_time': datetime.utcfromtimestamp(item['submit_time']).strftime('%Y-%m-%d %H:%M:%S') + ' UTC',
          'input_file_name': item['input_file_name'],
          'job_status': item['job_status']
      }
      annotations.append(annotation)
    return render_template('annotations.html', annotations=annotations)
  else:
      abort(403)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  # Create a session client to the S3 service
  try:
    s3 = boto3.client('s3',
      region_name=app.config['AWS_REGION_NAME'],
      config=Config(signature_version='s3v4'))
  except ClientError as e:
      app.logger.error(f"Error Connecting to the s3 client: {e}")
      return abort(500)
  
  user_id = session.get('primary_identity')
  profile = get_profile(identity_id=user_id)
  user_role = profile.role
  is_free_user = True if user_role == "free_user" else False

  # Check if the requested job ID belongs to the authenticated user
  try:
      dynamodb = boto3.resource('dynamodb')
      table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
      response = table.get_item(Key={'job_id': id})
      item = response.get('Item')
      if not item or item.get('user_id') != user_id:
          app.logger.error(f"User: {user_id} attempting to access unauthorized job: {id}")
          abort(403)
  except ClientError as e:
      app.logger.error(f"Error retrieving job details from DynamoDB: {e}")
      return abort(500)
    
  # Get download url for the input file
  try:
    # https://boto3.amazonaws.com/v1/documentation/api/1.11.4/guide/s3-presigned-urls.html
    inputs_download_url = s3.generate_presigned_url('get_object',
                                          Params={'Bucket': app.config['AWS_S3_INPUTS_BUCKET'],
                                                  'Key': item['s3_key_input_file']},
                                          ExpiresIn=300)
  except ClientError as e:
    app.logger.errer(f"Error Creating Presigned URL from s3 client")
    abort(500)

  # Convert epoch times to human-readable form
  submit_time = datetime.utcfromtimestamp(item['submit_time']).strftime('%Y-%m-%d %H:%M:%S') + ' UTC'
  complete_time = (datetime.utcfromtimestamp(item['complete_time']).strftime('%Y-%m-%d %H:%M:%S') + ' UTC') if 'complete_time' in item else None

  annotation = {
    'job_id': id,
    'submit_time': submit_time,
    'input_file_name': item['input_file_name'],
    'input_file_url': inputs_download_url,
    'job_status': item['job_status'],
    'complete_time': complete_time
  }
  
  # For free users, if five minutes have passed, then they cannot download the object
  free_access_expired = False
  if item['job_status'] == "COMPLETED" and is_free_user:
    current_time = datetime.utcnow()
    time_diff = current_time - datetime.utcfromtimestamp(item['complete_time'])
    # Check if the time difference is greater than five minutes
    # https://docs.python.org/3/library/datetime.html
    if time_diff > timedelta(minutes=5):
        free_access_expired = True
  elif item['job_status'] == "COMPLETED" and not is_free_user and 'results_file_archive_id' in item:
    # Restoring must be in process
    annotation['restore_message'] = "Restoration in Progress"
  # Add approriate elements to annotations
  elif item['job_status'] == "COMPLETED":
    # Get the download URL for the completed results file
    # https://boto3.amazonaws.com/v1/documentation/api/1.11.4/guide/s3-presigned-urls.html
    try:
      results_download_url = s3.generate_presigned_url('get_object',
                                           Params={'Bucket': app.config['AWS_S3_RESULTS_BUCKET'],
                                                   'Key': item['s3_key_result_file']},
                                           ExpiresIn=300)
    except ClientError as e:
      app.logger.errer(f"Error Creating Presigned URL from s3 client")
      abort(500)
    annotation['result_file_url'] = results_download_url
    
  # Render the annotation_details template with job details
  return render_template('annotation_details.html', annotation=annotation, free_access_expired=free_access_expired)


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  user_id = session.get('primary_identity')
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))
  
  # Fetch Job Details from the Dynamo DB
  try:
      dynamodb = boto3.resource('dynamodb')
      table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
      response = table.get_item(Key={'job_id': id})
      item = response.get('Item')
      if not item or item.get('user_id') != user_id:
          app.logger.error(f"User: {user_id} attempting to access unauthorized job: {id}")
          abort(403)
  except ClientError as e:
      app.logger.error(f"Error retrieving job details from DynamoDB: {e}")
      return abort(500)
    
  try:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
    response = s3.get_object(
      Bucket=app.config['AWS_S3_RESULTS_BUCKET'],
      Key=item.get('s3_key_log_file')
    )
    # https://stackoverflow.com/questions/43294802/lambda-return-payload-botocore-response-streamingbody-object-prints-but-then-emp
    log_file_contents = response['Body'].read().decode('utf-8')
  except ClientError as e:
    app.logger.error(f"Unable to connect with the dynamoDB client: {e}")
    return abort(500)
  return render_template('view_log.html', job_id=id, log_file_contents=log_file_contents)


"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!
    user_id = session['primary_identity']
    if user_id:
      try:
        # Connect to the dynamoDB client
        dynamo = boto3.resource('dynamodb')
        table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
      except ClientError as e:
          app.logger.error(f"Unable to connect with the dynamoDB client: {e}")
          return abort(500)
        
      try:
          # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html
          response = table.query(
            IndexName='user_id_index',
            KeyConditionExpression='user_id = :user_id',
            ExpressionAttributeValues={
                ':user_id': user_id
            }
          )
          job_items = response.get('Items', [])
      except Exception as e:
          app.logger.error(f"Error retrieving user jobs from DynamoDB: {e}")
          abort(500)
          
      # Connect to sns client
      try:
        sns = boto3.client('sns')
      except Exception as e:
        app.logger.error(f"Error rConnecting to sns client: {e}")
        abort(500)
        
      # For each item, publish a message to unarchive the job
      for job in job_items:
        data = {
          'job_id': job['job_id']
        }
        try:
          sns.publish(
              TopicArn=app.config['AWS_SNS_GLACIER_RESTORE_TOPIC'],
              Message=json.dumps(data)
          )
        except Exception as e:
          app.logger.error(f"Error publishing notification message to SNS topic: {e}")
          return abort(500)
    else:
        abort(403)

    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF
