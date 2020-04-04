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
  
  #Conditions for AOCS
  morning_aocs_start = datetime.time(9, 30, 0)
  morning_aocs_end = datetime.time(11, 10, 0)
  evening_aocs_start = datetime.time(21, 30, 0)
  evening_aocs_end = datetime.time(23, 13, 0)
  if (aos.time() > morning_aocs_start and aos.time() < morning_aocs_end) or (aos.time() > evening_aocs_start and aos.time() < evening_aocs_end):    
    passtype = 'AOCS'
  
  myconnection = mysql.connector.connect(host="localhost", user="metopmon", passwd="metop1", database="metopmon")
  mycursor = myconnection.cursor() 
  mysqlstmt = "INSERT INTO passes VALUES (%s, %s, %s, %s, %s, %s)"
  myvalues = (scid, orbit, anx, aos, los, passtype)
  mycursor.execute(mysqlstmt, myvalues)
  myconnection.commit()
  
  
