#
# Client-side python app for benford app, which is calling
# a set of lambda functions in AWS through API Gateway.
# The overall purpose of the app is to process a PDF and
# see if the numeric values in the PDF adhere to Benford's
# law.
#
# Authors:
#   Prof. Joe Hummel
#   Northwestern University
#   CS 310, Project 03
#
#   Dilan Nair
#   Northwestern University
#   CS 310, Project 04
#

import requests
import json

import uuid
import pathlib
import logging
import sys
import os
import base64

from configparser import ConfigParser

############################################################
#
# classes
#


class User:

  def __init__(self, row):
    self.userid = row[0]
    self.username = row[1]
    self.pwdhash = row[2]


class Job:

  def __init__(self, row):
    self.jobid = row[0]
    self.userid = row[1]
    self.status = row[2]
    self.originaldatafile = row[3]
    self.datafilekey = row[4]
    self.resultsfilekey = row[5]


############################################################
#
# globals
#

sessions = {}


def load_sessions():
  """
  Loads the previous sessions from the sessions.json file
  """

  global sessions
  if os.path.exists("sessions.json"):
    with open("sessions.json", "r") as f:
      sessions = json.load(f)


def update_session(username, token):
  """
  Updates the session with the given username and token
  """

  global sessions
  sessions[username] = {"token": token, "active": False}

  use_session(username)


def get_active_session():
  """
  Returns the active session
  """

  global sessions
  for username in sessions:
    if sessions[username]["active"]:
      return username, sessions[username]["token"]
  return None, None


def use_session(username):
  """
  Sets the session with the given username to active
  """

  global sessions
  for session in sessions:
    sessions[session]["active"] = False
  sessions[username]["active"] = True
  with open("sessions.json", "w") as f:
    json.dump(sessions, f, indent=2)


def clear_sessions():
  """
  Clears all sessions
  """

  global sessions
  sessions = {}
  with open("sessions.json", "w") as f:
    json.dump(sessions, f, indent=2)


def handle_error(url, res):
  """
  Handles an error from a request
  """

  print("Failed with status code:", res.status_code)
  print("  url:", url)
  print("  message:", res.json()["message"])


############################################################
#
# prompt
#
def prompt():
  """
  Prompts the user and returns the command number

  Parameters
  ----------
  None

  Returns
  -------
  Command number entered by user (0, 1, 2, ...)
  """
  print()
  print(">> Enter a command:")
  print("   0 => quit")
  print("")
  print("   1 => get all users")
  print("   2 => add user")
  print("   3 => log in")
  print("   4 => view and switch sessions")
  print("")
  print("   5 => get jobs for all users")
  print("   6 => upload")
  print("   7 => download")
  print("")
  print("   8 => log out all")
  print("   9 => reset users and jobs")

  cmd = input()

  if cmd == "":
    cmd = -1
  elif not cmd.isnumeric():
    cmd = -1
  else:
    cmd = int(cmd)

  return cmd


############################################################
#
# get_users
#
def get_users(baseurl):
  """
  Prints out all the users in the database

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  #
  # call the web service:
  #
  api = '/users'
  url = baseurl + api

  res = requests.get(url)

  #
  # let's look at what we got back:
  #
  if not res.ok:
    handle_error(url, res)
    return

  #
  # deserialize and extract users:
  #
  body = res.json()

  #
  # let's map each row into a User object:
  #
  users = []
  for row in body:
    user = User(row)
    users.append(user)
  #
  # Now we can think OOP:
  #
  if len(users) == 0:
    print("no users...")
    return

  for user in users:
    print(user.userid)
    print(" ", user.username)
    print(" ", user.pwdhash)

  return


############################################################
#
# add_user
#
def add_user(baseurl):
  """
  Adds a new user to the database

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  #
  # get username and password from user:
  #
  print("Enter username>")
  username = input()

  print("Enter password>")
  password = input()

  #
  # build the data packet:
  #
  data = {"username": username, "password": password}

  #
  # call the web service:
  #
  api = '/users'
  url = baseurl + api

  res = requests.post(url, json=data)

  #
  # let's look at what we got back:
  #
  if not res.ok:
    handle_error(url, res)
    return

  #
  # success, extract userid:
  #
  body = res.json()

  userid = body["userid"]

  print("User added, id =", userid)

  return


############################################################
#
# login
#
def login(baseurl):
  """
  Log in as a user

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  #
  # get username and password from user:
  #
  print("Enter username>")
  username = input()

  print("Enter password>")
  password = input()

  #
  # build the data packet:
  #
  data = {"username": username, "password": password}

  #
  # call the web service:
  #
  api = '/auth'
  url = baseurl + api

  res = requests.post(url, json=data)

  #
  # let's look at what we got back:
  #
  if not res.ok:
    handle_error(url, res)
    return

  #
  # success, extract token:
  #
  body = res.json()

  token = body["access_token"]

  print("New user logged in, username = ", username)

  #
  # update sessions:
  #
  update_session(username, token)

  return


############################################################
#
# switch_user
#
def switch_user(baseurl):
  """
  Switch user

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  print("Current sessions:")
  if len(sessions) == 0:
    print("  none")

  for session in sessions:
    print(" ", session, " => active =", sessions[session]["active"])

  print("Enter username of session or leave blank to skip>")
  username = input()

  if username == "":
    return

  if username not in sessions:
    print("No session with that username...")
    return

  use_session(username)

  print("Switched session, username =", username)

  return


############################################################
#
# get_jobs
#
def get_jobs(baseurl):
  """
  Prints out all the jobs in the database

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  #
  # call the web service:
  #
  api = '/jobs'
  url = baseurl + api

  res = requests.get(url)

  #
  # let's look at what we got back:
  #
  if not res.ok:
    handle_error(url, res)
    return

  #
  # deserialize and extract jobs:
  #
  body = res.json()
  #
  # let's map each row into an Job object:
  #
  jobs = []
  for row in body:
    job = Job(row)
    jobs.append(job)
  #
  # Now we can think OOP:
  #
  if len(jobs) == 0:
    print("no jobs...")
    return

  for job in jobs:
    print(job.jobid)
    print(" ", job.userid)
    print(" ", job.status)
    print(" ", job.originaldatafile)
    print(" ", job.datafilekey)
    print(" ", job.resultsfilekey)

  return


############################################################
#
# upload
#
def upload(baseurl):
  """
  Prompts the user for a local filename, and uploads that
  asset (PDF) to S3 for processing as an authenticated user.

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  username, token = get_active_session()

  if username is None:
    print("No active session...")
    return

  print("Uploading as user:", username)

  print("Enter PDF filename>")
  local_filename = input()

  if not pathlib.Path(local_filename).is_file():
    print("PDF file '", local_filename, "' does not exist...")
    return

  #
  # build the data packet:
  #
  infile = open(local_filename, "rb")
  bytes = infile.read()
  infile.close()

  #
  # now encode the pdf as base64. Note b64encode returns
  # a bytes object, not a string. So then we have to convert
  # (decode) the bytes -> string, and then we can serialize
  # the string as JSON for upload to server:
  #
  data = base64.b64encode(bytes)
  datastr = data.decode()

  data = {"filename": local_filename, "data": datastr}

  #
  # call the web service:
  #
  api = '/upload'
  url = baseurl + api

  res = requests.post(url,
                      json=data,
                      headers={"Authorization": "Bearer " + token})

  #
  # let's look at what we got back:
  #
  if not res.ok:
    handle_error(url, res)
    return

  #
  # success, extract jobid:
  #
  body = res.json()

  jobid = body["jobid"]

  print("PDF uploaded, job id =", jobid)
  return


############################################################
#
# download
#
def download(baseurl):
  """
  Prompts the user for the job id, and downloads
  that asset (PDF).

  Asset must belong to the authenticated user.

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  username, token = get_active_session()

  if username is None:
    print("No active session...")
    return

  print("Downloading as user:", username)

  print("Enter job id>")
  jobid = input()

  #
  # call the web service:
  #
  api = '/download'
  url = baseurl + api + '/' + jobid

  res = requests.get(url, headers={"Authorization": "Bearer " + token})

  #
  # let's look at what we got back:
  #
  if not res.ok:
    handle_error(url, res)
    return

  #
  # deserialize and extract results:
  #
  body = res.json()

  datastr = body["data"]

  base64_bytes = datastr.encode()
  bytes = base64.b64decode(base64_bytes)
  results = bytes.decode()

  print(results)
  return


############################################################
#
# reset_sessions
#
def reset_sessions(baseurl):
  """
  Clears all sessions

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  clear_sessions()

  print("Sessions cleared")

  return


############################################################
#
# reset_everything
#
def reset_everything(baseurl):
  """
  Resets the database back to initial state and clears all sessions.

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  #
  # clear sessions:
  #
  clear_sessions()

  #
  # call the web service:
  #
  api = '/reset'
  url = baseurl + api

  res = requests.delete(url)

  #
  # let's look at what we got back:
  #
  if not res.ok:
    handle_error(url, res)
    return

  #
  # deserialize and print message
  #
  body = res.json()

  msg = body

  print(msg)
  return


############################################################
# main
#
try:
  print('** Welcome to BenfordApp **')
  print()

  # eliminate traceback so we just get error message:
  sys.tracebacklimit = 0

  #
  # what config file should we use for this session?
  #
  config_file = 'benfordapp-client-config.ini'

  print("Config file to use for this session?")
  print("Press ENTER to use default, or")
  print("enter config file name>")
  s = input()

  if s == "":  # use default
    pass  # already set
  else:
    config_file = s

  #
  # does config file exist?
  #
  if not pathlib.Path(config_file).is_file():
    print("**ERROR: config file '", config_file, "' does not exist, exiting")
    sys.exit(0)

  #
  # setup base URL to web service:
  #
  configur = ConfigParser()
  configur.read(config_file)
  baseurl = configur.get('client', 'webservice')

  #
  # make sure baseurl does not end with /, if so remove:
  #
  if len(baseurl) < 16:
    print("**ERROR: baseurl '", baseurl, "' is not nearly long enough...")
    sys.exit(0)

  if baseurl == "https://YOUR_GATEWAY_API.amazonaws.com":
    print(
        "**ERROR: update benfordapp-client-config.ini file with your gateway endpoint"
    )
    sys.exit(0)

  lastchar = baseurl[len(baseurl) - 1]
  if lastchar == "/":
    baseurl = baseurl[:-1]

  #
  # load previous sessions:
  #
  load_sessions()

  #
  # main processing loop:
  #
  cmd = prompt()

  fns = [
      None, get_users, add_user, login, switch_user, get_jobs, upload,
      download, reset_sessions, reset_everything
  ]

  try:
    while cmd != 0:
      if cmd < 0 or cmd >= len(fns):
        print("** Unknown command, try again...")
        cmd = prompt()
        continue
      fn = fns[cmd]
      if fn is None:
        break
      fn(baseurl)
      cmd = prompt()
  except Exception as e:
    logging.error(fn.__name__ + "() failed:")
    logging.error(e)

  #
  # done
  #
  print()
  print('** done **')
  sys.exit(0)

except Exception as e:
  logging.error("**ERROR: main() failed:")
  logging.error(e)
  sys.exit(0)
