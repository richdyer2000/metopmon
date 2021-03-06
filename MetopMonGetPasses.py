#!/usr/bin/env python3
import math
import datetime
import time
import sys
import os
import numpy
import socket
import mysql.connector

sys.path.append('/home/pi/')

#Get mcf server details
myconnection = mysql.connector.connect(host="localhost", user="metopmon", passwd="metop1", database="metopmon")
mycursor = myconnection.cursor() 


mycursor.execute("SELECT * FROM mcf_server LIMIT 1")
mcfserver = mycursor.fetchall()
mcfservername = mcfserver[0][0]
mcfusername = mcfserver[0][1]
mcfpassword = mcfserver[0][2]

duration_aos = -24
duration_anx = -25
#duration_aos = -192 #comment out unless testing
#duration_anx = -193 #comment out unless testing


#Get AOS/LOS + ANX Times. We go back further for ANX times to make sure we find one for our earlies AOS
myconnection = mysql.connector.connect(host=mcfservername, user=mcfusername, passwd=mcfpassword, database="fdfdb")
mycursor = myconnection.cursor()
mycursor.execute("select scid, orbit, utc as 'aos', date_add(utc, INTERVAL duration/60000 MINUTE) as 'los' from fdfdb.events where name ='STAT_AOS' and target = 'CDA' and extra = 'CONSTANT_0' and scid like 'M0%' and utc > date_add(NOW(), INTERVAL " + str(duration_aos) + " HOUR) ORDER BY utc DESC")
mypasses = mycursor.fetchall()
mycursor.execute("select scid, orbit, utc from fdfdb.events where name ='ANX' and scid like 'M0%' and utc > date_add(NOW(), INTERVAL " + str(duration_anx) + " HOUR) ORDER BY utc DESC")
myanx = mycursor.fetchall()

#Merge into a single list, and calculate the Pass Type
if len(mypasses) > 0 and len(myanx) > 0:
  myconnection = mysql.connector.connect(host="localhost", user="metopmon", passwd="metop1", database="metopmon")
  mycursor = myconnection.cursor() 
  mycursor.execute("TRUNCATE TABLE passes")
  myconnection.commit()
  print(str(datetime.datetime.now()) + ": ingesting " + str(len(mypasses)) + " passes")


for i in mypasses:
  scid = 'NA'
  orbit = 'NA'
  aos = 'NA'
  los = 'NA'
  anx = 'NA'
  passtype = 'NORMAL' 
  
  
  scid = i[0]
  orbit = i[1]
  aos = i[2]
  los = i[3]
  for j in myanx:
    if j[0] == scid and j[1] == orbit:
      anx = j[2]
      
  ###########################
  #Conditions for DEF_ROUT
  defrout_start = datetime.time(2, 0, 0)
  defrout_end = datetime.time(3, 41, 0)
  if (anx.time() > defrout_start and anx.time() < defrout_end):
    passtype = 'DEF_ROUT'
  
  ########################################################################################################################
  #Conditions for AOCS - this is a bit more tricky as we have to show that there is exctly one more LOS before 01:00/13:00
  #First get the next 2 los times
  nextlos = 0
  nextnextlos = 0
  for k in mypasses:
    if k[0] == scid and k[1] == orbit + 1:
      nextlos = k[3]
    if k[0] == scid and k[1] == orbit + 2:
      nextnextlos = k[3]
  
  if nextlos != 0 and nextnextlos != 0:   
    morning_aocs_exec = datetime.time(13, 0, 0)
    evening_aocs_exec = datetime.time(1, 0, 0)
    #First the Morning AOCS Pass
    if (nextnextlos.time() > morning_aocs_exec and nextlos.time() < morning_aocs_exec):    
      passtype = 'AOCS'
    #Then the Evening AOCS Pass
    if (nextnextlos.time() > evening_aocs_exec and nextnextlos.time() < morning_aocs_exec and (nextlos.time() < evening_aocs_exec or nextlos.time() > morning_aocs_exec)):    
      passtype = 'AOCS'  
  
  myconnection = mysql.connector.connect(host="localhost", user="metopmon", passwd="metop1", database="metopmon")
  mycursor = myconnection.cursor() 
  mysqlstmt = "INSERT INTO passes VALUES (%s, %s, %s, %s, %s, %s)"
  myvalues = (scid, orbit, anx, aos, los, passtype)
  mycursor.execute(mysqlstmt, myvalues)
  myconnection.commit()
  
#Check that the Insertion has completed
numpasses = 0
while numpasses != len(mypasses):
  myconnection = mysql.connector.connect(host="localhost", user="metopmon", passwd="metop1", database="metopmon")
  mycursor = myconnection.cursor() 
  mycursor.execute("Select * from passes")
  ingestedpasses = mycursor.fetchall()
  numpasses = len(ingestedpasses)
  time.sleep(5)
    
  
#Finally, we have to clean up the processed passes and notified passes - they both need to remain small
mysqlstmt = "DELETE p FROM processed_passes p WHERE NOT EXISTS (SELECT 1 FROM passes n WHERE p.scid = n.scid and p.orbit = n.orbit)"
mycursor.execute(mysqlstmt)
myconnection.commit()
mysqlstmt = "DELETE p FROM notified_passes p WHERE NOT EXISTS (SELECT 1 FROM passes n WHERE p.scid = n.scid and p.orbit = n.orbit)"
mycursor.execute(mysqlstmt)
myconnection.commit()
  
  
