#!/usr/bin/env python3
import httplib, urllib
import math
import datetime
import time
import sys
import os
import numpy
import socket
import mysql.connector

def metopmon_insert(mysqlstmt, myvalues):
  myconnection = mysql.connector.connect(host="localhost", user="metopmon", passwd="metop1", database="metopmon")
  mycursor = myconnection.cursor() 
  mycursor.execute(mysqlstmt, myvalues)
  myconnection.commit()
  return  
  
def metopmon_read(mysqlstmt, myvalues):
  myconnection = mysql.connector.connect(host="localhost", user="metopmon", passwd="metop1", database="metopmon")
  mycursor = myconnection.cursor()
  if myvalues != 0:
    mycursor.execute(mysqlstmt, myvalues)
  if myvalues == 0:
    mycursor.execute(mysqlstmt)
  myresult = mycursor.fetchall()
  return(myresult)

def send_info (message):
  #This will send a low priority warning to all SOMs
  mySOMs = metopmon_read("SELECT * FROM pushover_keys WHERE role = 'KOM'", 0)
  
  for i in mySOMs:
    conn = httplib.HTTPSConnection("api.pushover.net:443")
    conn.request("POST", "/1/messages.json",
      urllib.urlencode({
        "token": "ay8kcarsgk9w6uhywetw1wrugcstpy",
        "user": i[2],
        "message": message,
        "priority": -1,
        "sound": "pushover"
      }), { "Content-type": "application/x-www-form-urlencoded" })
    conn.getresponse()


def send_alert (message):
  #This will send a low priority warning to all SOMs
  mySOMs = metopmon_read("SELECT * FROM pushover_keys WHERE role = 'SOM'", 0)
  
  for i in mySOMs:
    conn = httplib.HTTPSConnection("api.pushover.net:443")
    conn.request("POST", "/1/messages.json",
      urllib.urlencode({
        "token": "ay8kcarsgk9w6uhywetw1wrugcstpy",
        "user": i[2],
        "message": message,
        "priority": 0,
        "sound": "pushover"
      }), { "Content-type": "application/x-www-form-urlencoded" })
    conn.getresponse()

def send_warning (message):
  #This will send a low priority warning to all SOMs
  mySOMs = metopmon_read("SELECT * FROM pushover_keys WHERE role = 'KOM'", 0)
  
  for i in mySOMs:
    conn = httplib.HTTPSConnection("api.pushover.net:443")
    conn.request("POST", "/1/messages.json",
      urllib.urlencode({
        "token": "ay8kcarsgk9w6uhywetw1wrugcstpy",
        "user": i[2],
        "message": message,
        "priority": 1,
        "sound": "pushover"
      }), { "Content-type": "application/x-www-form-urlencoded" })
    conn.getresponse()
    
def send_critical (message):
  #This will send a critical warning to all SOMs
  mySOMs = metopmon_read("SELECT * FROM pushover_keys WHERE role = 'KOM'", 0)
  
  for i in mySOMs:
    conn = httplib.HTTPSConnection("api.pushover.net:443")
    conn.request("POST", "/1/messages.json",
      urllib.urlencode({
        "token": "ay8kcarsgk9w6uhywetw1wrugcstpy",
        "user": i[2],
        "message": message,
        "priority": 2,
        "retry": 30,
        "expire": 900,
        "sound": "persistent"
      }), { "Content-type": "application/x-www-form-urlencoded" })
    conn.getresponse()

def process_messages(mymessages, criticality, aocs_pass): 

  scid = mymessages[0][0]
  orbit = mymessages[0][1]
  aos = mymessages[0][2]
  
  message = ""
  count = 0
  carriage_return = ""
  
  if criticality == 1 and aocs_pass == 1:
    message = scid + " " + str(orbit) + " " + aos.strftime("%d-%b-%Y, %H:%M:%S") + " " + " AOCS Pass Without Errors"
    send_info(message)
  elif criticality > 1:
    for i in mymessages:
      #determining whether carriage return is needed
      if count > 0:
        carriage_return = "\n"   
      count = count + 1    
      message = message + carriage_return + i[3] + " criticality " + str(i[5]) + ": " + i[4]
    message = scid + " " + str(orbit) + " " + aos.strftime("%d-%b-%Y, %H:%M:%S") + " " + " Overall Criticality: " + str(criticality) + "\n" + message
  
    if criticality == 2:
      send_alert(message)
    if criticality == 3:
      send_warning(message)      
    elif criticality == 4:
      send_critical(message)
    
  print(message)  
  
  
#Get List of passes to report
mysqlstmt = "SELECT * FROM processed_passes p WHERE NOT EXISTS (SELECT 1 FROM notified_passes n WHERE p.scid = n.scid and p.orbit = n.orbit)"
passes_to_notify = metopmon_read(mysqlstmt, 0)
for i in passes_to_notify:
  scid = i[0]
  orbit = i[1]
  
  #Add this to the list of passes where notifications have been sent
  mysqlstmt = "INSERT INTO notified_passes (scid, orbit) VALUES (%s, %s)"
  myvalues = (scid, orbit)
  metopmon_insert(mysqlstmt, myvalues)

  #Get All the Messages for this Orbit
  mysqlstmt = "Select * FROM events WHERE scid = %s AND orbit = %s"
  myvalues = (scid, orbit)
  mymessages = metopmon_read(mysqlstmt, myvalues)
  
  #determine whether this is an aocs pass
  aocs_pass = 0
  aos = mymessages[0][2].time()
  morning_aocs_start = datetime.time(9, 30, 0)
  morning_aocs_end = datetime.time(11, 10, 0)
  evening_aocs_start = datetime.time(21, 30, 0)
  evening_aocs_end = datetime.time(23, 10, 0)
  if (aos > morning_aocs_start and aos < morning_aocs_end) or (aos > evening_aocs_start and aos < evening_aocs_end):
    aocs_pass = 1
    
  #determine the max criticality
  criticality = 1
  for i in mymessages:
   if i[5] > criticality:
    criticality = i[5]
  
  #send a message if its an aocs pass or
  if aocs_pass == 1 or criticality > 1:
    process_messages(mymessages, criticality, aocs_pass)

  
