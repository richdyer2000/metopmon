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
  mySOMs = metopmon_read("SELECT * FROM pushover_keys WHERE role = 'SOM'", 0)
  
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
  mySOMs = metopmon_read("SELECT * FROM pushover_keys WHERE role = 'SOM'", 0)
  
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
  mySOMs = metopmon_read("SELECT * FROM pushover_keys WHERE role = 'SOM'", 0)
  
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

def process_messages(mymessages, criticality): 

  scid = mymessages[0][0]
  orbit = mymessages[0][1]
  passtype = mymessages[0][2]
  aos = mymessages[0][3]
  
  message = ""
  count = 0
  carriage_return = ""
 

  for mymessage in mymessages:
    #determining whether carriage return is needed
    mysubsystem = mymessage[4]
    mycriticality = mymessage[6]
    mytext = mymessage[5] 
    
    if count > 0: carriage_return = "\n"   
    count = count + 1    
    message = message + carriage_return + mysubsystem + " criticality " + str(mycriticality) + ": " + mytext
  
  message = scid + " " + str(orbit) + " " + passtype + " PASS at " + aos.strftime("%d-%b-%Y, %H:%M:%S") + " " + " Overall Criticality: " + str(criticality) + "\n" + message
  
  if criticality <= 2:
    send_alert(message)
  if criticality == 3:
    send_warning(message)      
  elif criticality == 4:
    send_critical(message)
    

  
  
#Get List of passes to report
mysqlstmt = "SELECT * FROM processed_passes p WHERE NOT EXISTS (SELECT 1 FROM notified_passes n WHERE p.scid = n.scid and p.orbit = n.orbit) AND p.status = 'COMPLETED'"
passes_to_notify = metopmon_read(mysqlstmt, 0)

for i in passes_to_notify:
  scid = i[0]
  orbit = i[1]
  
  complete = 0
  print(str(datetime.datetime.now()) + ": processing " + scid + " pass " + str(orbit) ) 
  
 
  try:
    #Get All the Messages for this Orbit
    mysqlstmt = "Select scid, orbit, passtype, aos, subsystem, message, criticality FROM events WHERE scid = %s AND orbit = %s"
    myvalues = (scid, orbit)
    mymessages = metopmon_read(mysqlstmt, myvalues)
    passtype = mymessages[0][2]  
    #determine the max criticality
    criticality = 1
    for i in mymessages:
      if i[6] > criticality: criticality = i[6]
  
    #send a message if criticality > 1
    if criticality > 1 or passtype == "AOCS":
      process_messages(mymessages, criticality)

    complete = 1

  except:
    print(str(datetime.datetime.now()) + ": " + scid + " pass " + str(orbit) + " failed somewhere") 
    metopmon_insert("DELETE FROM notified_passes WHERE scid = %s AND orbit = %s", (scid, orbit))

  if complete == 1:
    print(str(datetime.datetime.now()) + ": finished processing " + scid + " pass " + str(orbit) ) 
    metopmon_insert("INSERT INTO notified_passes (scid, orbit) VALUES (%s, %s)", (scid, orbit))
