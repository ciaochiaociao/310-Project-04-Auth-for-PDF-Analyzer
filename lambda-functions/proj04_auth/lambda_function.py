import json
import os
import datatier
import auth
import api_utils

from configparser import ConfigParser

def lambda_handler(event, context):
  try:
    print("**STARTING**")
    print("**lambda: proj04_auth**")

    #
    # setup AWS based on config file
    #
    config_file = 'config.ini'
    os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file
    
    configur = ConfigParser()
    configur.read(config_file)
    
    #
    # configure for RDS access
    #
    rds_endpoint = configur.get('rds', 'endpoint')
    rds_portnum = int(configur.get('rds', 'port_number'))
    rds_username = configur.get('rds', 'user_name')
    rds_pwd = configur.get('rds', 'user_pwd')
    rds_dbname = configur.get('rds', 'db_name')

    #
    # read the username and password from the event body
    #
    print("**Accessing request body**")

    if "body" not in event:
      return api_utils.error(400, "no body in request")
    
    body = json.loads(event["body"])

    if "username" not in body or "password" not in body:
      return api_utils.error(400, "missing credentials in body")
    
    username = body["username"]
    password = body["password"]

    #
    # open connection to the database
    #
    print("**Opening connection**")
    
    dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)

    #
    # TODO: YOUR CODE HERE
    #

    cols = datatier.retrieve_one_row(dbConn, "SELECT `userid`, `pwdhash` FROM `users` WHERE `username`=%s", (username,))
    if len(cols) == 0:
      return api_utils.error(404, "no such user")
    else:
      userid, pwdhash = cols
      valid = auth.check_password(password, pwdhash)
      if not valid:
        return api_utils.error(401, "password incorrect")
      token = auth.generate_token(userid, 'abc')
    #
    # respond in an HTTP-like way, i.e. with a status
    # code and body in JSON format:
    #
    print("**DONE, returning token**")

    return api_utils.success(200, {'access_token': token})
    
  except Exception as err:
    print("**ERROR**")
    print(str(err))

    return api_utils.error(500, str(err))
