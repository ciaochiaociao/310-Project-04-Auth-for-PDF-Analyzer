import json
import boto3
import os
import uuid
import base64
import pathlib
import datatier
import auth
import api_utils

from configparser import ConfigParser

def lambda_handler(event, context):
  try:
    print("**STARTING**")
    print("**lambda: proj04_upload**")
    
    #
    # setup AWS based on config file
    #
    config_file = 'config.ini'
    os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file
    
    configur = ConfigParser()
    configur.read(config_file)
    
    #
    # configure for S3 access
    #
    s3_profile = 's3readwrite'
    boto3.setup_default_session(profile_name=s3_profile)
    
    bucketname = configur.get('s3', 'bucket_name')
    
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketname)
    
    #
    # configure for RDS access
    #
    rds_endpoint = configur.get('rds', 'endpoint')
    rds_portnum = int(configur.get('rds', 'port_number'))
    rds_username = configur.get('rds', 'user_name')
    rds_pwd = configur.get('rds', 'user_pwd')
    rds_dbname = configur.get('rds', 'db_name')

    #
    # get the access token from the request headers,
    # then get the user ID from the token
    #
    print("**Accessing request headers to get authenticated user info**")

    if "headers" not in event:
      return api_utils.error(400, "no headers in request")
    
    headers = event["headers"]

    #
    # TODO: YOUR CODE HERE
    #
    
    token = auth.get_token_from_header(headers)
    if token is None:
      return api_utils.error(401, "no bearer token in headers")

    # CHANGE THIS
    try:
      userid = auth.get_user_from_token(token, secret="abc")
    except Exception as e:
      return api_utils.error(401, "invalid access token")
    
    print("userid:", userid)
  
    #
    # the user has sent us two parameters:
    #  1. filename of their file
    #  2. raw file data in base64 encoded string
    #
    # The parameters are coming through web server 
    # (or API Gateway) in the body of the request
    # in JSON format.
    #
    print("**Accessing request body**")
    
    if "body" not in event:
      return api_utils.error(400, "no body in request")
      
    body = json.loads(event["body"]) # parse the json
    
    if "filename" not in body:
      return api_utils.error(400, "no filename in body")
    if "data" not in body:
      return api_utils.error(400, "no data in body")

    filename = body["filename"]
    datastr = body["data"]
    
    print("filename:", filename)
    print("datastr (first 10 chars):", datastr[0:10])

    #
    # open connection to the database
    #
    print("**Opening connection**")
    
    dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)

    #
    # first we need to make sure the userid is valid
    #
    print("**Checking if userid is valid**")
    
    sql = "SELECT * FROM users WHERE userid = %s;"
    
    row = datatier.retrieve_one_row(dbConn, sql, [userid])
    
    if row == ():  # no such user
      print("**No such user, returning...**")
      return api_utils.error(404, "no such user")
    
    print(row)
    
    username = row[1]
    
    #
    # at this point the user exists, so safe to upload to S3
    #
    base64_bytes = datastr.encode()        # string -> base64 bytes
    bytes = base64.b64decode(base64_bytes) # base64 bytes -> raw bytes
    
    #
    # write raw bytes to local filesystem for upload
    #
    print("**Writing local data file**")
    
    local_filename = "/tmp/data.pdf"
    
    outfile = open(local_filename, "wb")
    outfile.write(bytes)
    outfile.close()
    
    #
    # generate unique filename in preparation for the S3 upload
    #
    print("**Uploading local file to S3**")
    
    basename = pathlib.Path(filename).stem
    extension = pathlib.Path(filename).suffix
    
    if extension != ".pdf" : 
      return api_utils.error(400, "expecting filename to have .pdf extension")
    
    bucketkey = "benfordapp/" + username + "/" + basename + "-" + str(uuid.uuid4()) + ".pdf"
    
    print("S3 bucketkey:", bucketkey)
    
    #
    # add a jobs record to the database BEFORE we upload, just in case
    # the compute function is triggered faster than we can update the
    # database
    #
    print("**Adding jobs row to database**")
    
    sql = """
      INSERT INTO jobs(userid, status, originaldatafile, datafilekey, resultsfilekey)
                  VALUES(%s, 'pending', %s, %s, '');
    """
    
    datatier.perform_action(dbConn, sql, [userid, filename, bucketkey])
    
    #
    # grab the jobid that was auto-generated by mysql
    #
    sql = "SELECT LAST_INSERT_ID();"
    
    row = datatier.retrieve_one_row(dbConn, sql)
    
    jobid = row[0]
    
    print("jobid:", jobid)
    
    #
    # finally, upload to S3
    #
    print("**Uploading data file to S3**")

    bucket.upload_file(local_filename, 
                       bucketkey, 
                       ExtraArgs={
                         'ACL': 'public-read',
                         'ContentType': 'application/pdf'
                       })

    #
    # respond in an HTTP-like way, i.e. with a status
    # code and body in JSON format
    #
    print("**DONE, returning jobid**")

    return api_utils.success(200, {'jobid': jobid})
    
  except Exception as err:
    print("**ERROR**")
    print(str(err))

    return api_utils.error(500, str(err))
