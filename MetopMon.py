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


def epsmcf_query(mydb, mysqlstmt, myvalues):
  mcfserver = metopmon_query("SELECT * FROM mcf_server LIMIT 1", 0)
  mcfservername = mcfserver[0][0]
  mcfusername = mcfserver[0][1]
  mcfpassword = mcfserver[0][2]

  myconnection = mysql.connector.connect(host=mcfservername, user=mcfusername, passwd=mcfpassword, database=mydb)

  mycursor = myconnection.cursor()
  if myvalues != 0:
    mycursor.execute(mysqlstmt, myvalues)
  if myvalues == 0:
    mycursor.execute(mysqlstmt)
  myresult = mycursor.fetchall()
  return(myresult)

def metopmon_query(mysqlstmt, myvalues):
  myconnection = mysql.connector.connect(host="localhost", user="metopmon", passwd="metop1", database="metopmon")
  mycursor = myconnection.cursor()
  if myvalues != 0:
    mycursor.execute(mysqlstmt, myvalues)
  if myvalues == 0:
    mycursor.execute(mysqlstmt)
  myresult = mycursor.fetchall()
  return(myresult)

def metopmon_insert(mysqlstmt, myvalues):
  myconnection = mysql.connector.connect(host="localhost", user="metopmon", passwd="metop1", database="metopmon")
  mycursor = myconnection.cursor() 
  mycursor.execute(mysqlstmt, myvalues)
  myconnection.commit()
  return  
  
def metopmon_event(myvalues):
  metopmon_insert("INSERT INTO events (scid, orbit, passtype, aos, subsystem, message, criticality) VALUES (%s, %s, %s, %s, %s, %s, %s)", myvalues)
  return    

###################################################################
#First check stream status - just count events between aos and los
def process_stream(scid, orbit, anx, aos, los, passtype):
  
  mydb = "g1_events_" + scid.lower() 
  mystream = scid.lower() + "s_nom"
  myvalues = (mystream, aos, los)
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = %s AND eventTime BETWEEN DATE_ADD(%s, INTERVAL -5 MINUTE) AND %s"
  stream = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]


  
  return (stream)

#########################################################
#TM Processing - check connection and tm reception status   
def process_tlm(scid, orbit, anx, aos, los, passtype):

  oknok = 1
  
  mydb = "g1_events_" + scid.lower() 
  mystream = scid.lower() + "s_nom"
  myvalues = (mystream, aos, los)
  
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = %s AND code = 2110 AND eventTime BETWEEN DATE_ADD(%s, INTERVAL -5 MINUTE) AND %s"
  StatCon = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = %s AND code = 2111 AND eventTime BETWEEN DATE_ADD(%s, INTERVAL -1 MINUTE) AND %s"
  TmRx = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  
  if StatCon > 0 and TmRx > 0: #TM OK
    message = "TM Connection OK"
    metopmon_event((scid, orbit, passtype, aos, "TM", message, 1))
  if StatCon == 0: #No TM Connection to CDA
    message = "No TM Connection to CDA"
    metopmon_event((scid, orbit, passtype, aos, "TM", message, 3))
    oknok = 0
  if StatCon > 0 and TmRx == 0: #TM Connection to CDA, but no TLM Received - potential NO TLM!!   
    message = "No TM From Spacecraft despite successful connection to CDA"
    metopmon_event((scid, orbit, passtype, aos, "TM", message, 4))
    oknok = 0
 
  return(oknok)

############################################################
#ICU Reports   
def process_icureports(scid, orbit, anx, aos, los, passtype):  
  
  oknok = 1 
   
  myICUs=["ASCAT", "GOME", "GRAS", "IASI", "MPU", "NIU"]
  myvalues = (aos, los, ) 
  mydb = "g1_tmrep_" + scid.lower()
  mysqlstmt = "SELECT Originator_Cal FROM entries WHERE Originator >= 10 AND Reptype = 0 AND SourceStream = 'S' AND ObtUtc BETWEEN %s AND %s"
  myICUReps = epsmcf_query(mydb, mysqlstmt, myvalues)
  MissingICUReps = numpy.setdiff1d(myICUs,myICUReps,assume_unique=False).tolist()
  
  if len(MissingICUReps) > 0:
    seperator = ', '
    message = "Missing Reports For ICU(s): " + seperator.join(MissingICUReps)
    metopmon_event((scid, orbit, passtype, aos, "TM", message, 2))
    oknok = 0
  elif len(MissingICUReps) == 0:
    message = "All ICU Reports Received"
    metopmon_event((scid, orbit, passtype, aos, "TM", message, 1))

  return(oknok)

#######################################################
#TC Processing
def process_tc(scid, orbit, anx, aos, los, passtype):
  
  oknok = 1   
  
  queuesNumeric = [2, 3]
  queuesAlpha = ["MISSION", "EVENT"]
  
  for queueNumeric in queuesNumeric:
    
    queueAlpha = queuesAlpha[queuesNumeric.index(queueNumeric)]
    
    critical_adjust = 0
    #Criticality is adjusted depending on the Queue and whether this is an AOCS pass
    if queueAlpha == "EVENT" and passtype == "AOCS":
      critical_adjust = 2
    if queueAlpha == "EVENT" and passtype != "AOCS":
      critical_adjust = 1      

           
    #Check for queue opening and queue errors
    mydb = "g1_events_" + scid.lower() 
    mystream = scid.lower() + "s_nom"
    myqueue = "%" + queueAlpha + "%"

    myvalues = (mystream, myqueue, aos, los)
    mysqlstmt_queueOpen = "SELECT COUNT(*) FROM entries WHERE stream = %s AND code = 2702 AND message LIKE %s AND eventTime BETWEEN %s AND %s"
    mysqlstmt_queueErrors = "SELECT COUNT(*) FROM entries WHERE stream = %s AND (code = 2803 OR code = 2809) AND message LIKE %s AND eventTime BETWEEN %s AND %s"
    QueueErrors = epsmcf_query(mydb, mysqlstmt_queueErrors, myvalues)[0][0]
    QueueOpen = epsmcf_query(mydb, mysqlstmt_queueOpen, myvalues)[0][0]
    
    #Check for CV Failures      
    myvalues = (queueNumeric, aos, los) 
    mydb = "g1_tchist_" + scid.lower() 
    mysqlstmt = "SELECT cv_status FROM commands WHERE queue = %s AND cmd_type <> 10 AND build_send_time BETWEEN %s AND %s"
    myTCs = epsmcf_query(mydb, mysqlstmt, myvalues)
    numTCs = len(myTCs)
    numTCsOK = sum([sum(i) for i in myTCs])
    numTCsNOK = numTCs - numTCsOK
    
    if QueueOpen == 0: #Queue failed to Open
      message = queueAlpha + " Queue Failed to Open"
      metopmon_event((scid, orbit, passtype, aos, "TC", message, 2 + critical_adjust)) 
      oknok = 0
    elif QueueOpen > 0 and (numTCsNOK != 0 or QueueErrors !=0): #Queue opened, but there are errors
      message = queueAlpha + " Queue " + str(numTCsNOK) + " of " + str(numTCs) +  " TCs failed CV, " + str(QueueErrors) + " Queue Errors"
      metopmon_event((scid, orbit, passtype, aos, "TC", message, 2 + critical_adjust)) 
      oknok = 0
    elif QueueOpen > 0 and QueueErrors == 0 and numTCsNOK == 0:
      message = queueAlpha + " Queue " + str(numTCsOK) + " TCs successfully released"
      metopmon_event((scid, orbit, passtype, aos, "TC", message, 1))
  
  return(oknok)  

#########################################################
#Check for any PI errors in the *last Orbit* 
def process_pi(scid, orbit, anx, aos, los, passtype):  

  oknok = 1

  mydb = "g1_events_" + scid.lower() 
  mystream = scid.lower() + "s_nom"
  myvalues = (mystream, aos, aos)
  mysqlstmt_ProcFails = "SELECT COUNT(*) FROM entries WHERE stream = %s AND code = 2927 AND message LIKE '%_MP_%' AND eventTime BETWEEN DATE_ADD(%s, INTERVAL -102 MINUTE) AND %s"
  mysqlstmt_PiFails = "SELECT COUNT(*) FROM entries WHERE stream = %s AND code = 2959 AND message LIKE '%_MP_%' AND eventTime BETWEEN DATE_ADD(%s, INTERVAL -102 MINUTE) AND %s"
  mysqlstmt_PiCompleted = "SELECT COUNT(*) FROM entries WHERE stream = %s AND code = 2925 AND value = 'COMPLETED' AND eventTime BETWEEN DATE_ADD(%s, INTERVAL -102 MINUTE) AND %s"

  ProcFails = epsmcf_query(mydb, mysqlstmt_ProcFails, myvalues)[0][0]
  PiFails = epsmcf_query(mydb, mysqlstmt_PiFails, myvalues)[0][0]
  PiCompleted = epsmcf_query(mydb, mysqlstmt_PiCompleted, myvalues)[0][0]
  
  if ProcFails == 0 and PiFails == 0: 
    message = str(PiCompleted) + " PI Procedures OK"
    metopmon_event((scid, orbit, passtype, aos, "PI", message, 1))
  elif ProcFails != 0 or PiFails != 0:
    message = str(ProcFails) + " MP Procedures Failed Execution, " + str(PiFails) + " Procedures in Activation T/O or Aborted, " + str(PiCompleted) + " PI Procedures OK"
    metopmon_event((scid, orbit, passtype, aos, "PI", message, 1))
    oknok = 0
   
  return (oknok)


#######################################################
#In 'SYS', we're looking for PLSOL, RRM or ESM
def process_sys(scid, orbit, anx, aos, los, passtype):  
  
  oknok = 1
  
  mydb = "g1_events_" + scid.lower() 
  mystream = scid.lower() + "s_nom"
  myvalues = (mystream, aos, los)
  mysqlstmt_RRM = "SELECT COUNT(*) FROM entries WHERE stream = %s AND (code = 106 OR code = 107) AND mnemonic = 'MSTE381' AND eventTime BETWEEN %s AND %s"
  mysqlstmt_PLSOL = "SELECT COUNT(*) FROM entries WHERE stream = %s AND (code = 106 OR code = 107) AND mnemonic = 'UZPLM' AND eventTime BETWEEN %s AND %s"
  mysqlstmt_ESM = "SELECT COUNT(*) FROM entries WHERE stream = %s AND (code = 106 OR code = 107) AND mnemonic = 'UZMODLV' AND eventTime BETWEEN %s AND %s"
  
  RRM = epsmcf_query(mydb, mysqlstmt_RRM, myvalues)[0][0]
  PLSOL = epsmcf_query(mydb, mysqlstmt_PLSOL, myvalues)[0][0]
  ESM = epsmcf_query(mydb, mysqlstmt_ESM, myvalues)[0][0] 
  
  
  if RRM == 0 and PLSOL == 0 and ESM == 0: 
    message = "Satellite System OK"
    metopmon_event((scid, orbit, passtype, aos, "SYS", message, 1))
  elif RRM != 0 or PLSOL != 0 or ESM !=0 :
    message = "Critical System Error: PLSOL = " + str(PLSOL) + ", RRM = " + str(RRM) + ", ESM = " + str(ESM)
    metopmon_event((scid, orbit, passtype, aos, "SYS", message, 4))
    oknok = 0
    
  return(oknok)  
  
####################################################################################
#Loop through ICUs checking for ROOLs or TMREP warnings
def process_ins(scid, orbit, anx, aos, los, passtype):    
  

  oknok = 1
 
  myICUs=["ASCAT", "GOME", "GRAS", "IASI", "MPU", "NIU"]
  myICUmnems=["BN%", "ON%", "GN%", "EN%", "HN%", "FN%"] 
 
  #Check for ROOLs and TMREP Warnings
  for myICU in myICUs:   
    
   
    #ROOLS
    myICUmnem = myICUmnems[myICUs.index(myICU)]
    mydb = "g1_events_" + scid.lower() 
    mystream = scid.lower() + "s_nom"
    mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = %s AND (code = 106 OR code = 107) AND mnemonic LIKE %s AND eventTime BETWEEN %s AND %s"
    myvalues = (mystream, myICUmnem, aos, los)
    Rools = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]

    #TMREP Warnings
    mydb = "g1_tmrep_" + scid.lower() 
    mysqlstmt = "SELECT COUNT(*) FROM entries WHERE Originator_Cal = %s AND (Reptype = 2 OR Reptype = 4 OR Reptype = 6 OR Reptype = 14) AND ConstructUtc BETWEEN %s AND %s"
    myvalues = (myICU, aos, los)
    TmrepWrn = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]


    if TmrepWrn <> 0 or Rools <> 0:
      message = myICU + ": " + str(Rools) + " RED OOLs, " + str(TmrepWrn) + " TMREP Warnings"
      metopmon_event((scid, orbit, passtype, aos, "INS", message, 3))
      oknok = 0

  if oknok == 1:
    message = "No RED OOLs or TMREP Warnings"
    metopmon_event((scid, orbit, passtype, aos, "INS", message, 1))

  return (oknok)

####################################################################################
#Check PLM for ROOLs or TMREP warnings
def process_plm(scid, orbit, anx, aos, los, passtype):
  
  oknok = 1
  
  #Red OOLs
  mydb = "g1_events_" + scid.lower() 
  mystream = scid.lower() + "s_nom"
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = %s AND (code = 106 OR code = 107) AND mnemonic LIKE 'LN%' AND eventTime BETWEEN %s AND %s"
  myvalues = (mystream, aos, los)
  Rools = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
   
  #TMREP Warnings
  mydb = "g1_tmrep_" + scid.lower() 
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE Originator_Cal = 'PLM' AND (Reptype = 1 OR Reptype = 3) AND ConstructUtc BETWEEN %s AND %s"
  myvalues = (aos, los)
  TmrepWrn = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]

  if TmrepWrn + Rools <> 0:
    message = str(Rools) + " RED OOLs, " + str(TmrepWrn) + " TMREP Warnings"
    metopmon_event((scid, orbit, passtype, aos, "PLM",  message, 3))
    oknok = 0
  if TmrepWrn + Rools == 0:
    message = "No RED OOLs or TMREP Warnings"
    metopmon_event((scid, orbit, passtype, aos, "PLM", message, 1))

  return (oknok)
  
#########################################################  
#Check SVM for Functional Assemplies and TMREP Warnings
def process_svm(scid, orbit, anx, aos, los, passtype):
  
  oknok = 1

  #Functional Assemblies
  mydb = "g1_events_" + scid.lower() 
  mystream = scid.lower() + "s_nom"
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = %s AND (code = 106 OR code = 107) AND (mnemonic LIKE 'USEF%' OR mnemonic = 'USSADEPL' OR mnemonic Like 'ISCNFUR%') AND eventTime BETWEEN %s AND %s"
  myvalues = (mystream, aos, los)
  Fassies = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]  

  ###############  
  #TANM1 Warnings
  mydb = "g1_tmrep_" + scid.lower() 
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE Originator_Cal = 'SVM' AND Reptype = 0 AND ConstructUtc BETWEEN %s AND %s"
  myvalues = (aos, los)
  TANM1 = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  
  ###############
  #TANM2 Warnings
  mydb = "g1_tmrep_" + scid.lower() 
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE Originator_Cal = 'SVM' AND Reptype = 1 AND ConstructUtc BETWEEN %s AND %s"
  myvalues = (aos, los)
  TANM2 = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  
  #################
  #TVRPM Warnings
  mydb = "g1_tmrep_" + scid.lower() 
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE Originator_Cal = 'SVM' AND Reptype = 3 AND Subtype = 1 AND ConstructUtc BETWEEN %s AND %s"
  myvalues = (aos, los)
  TVRPM = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  
  ##################
  #TEXTR Warnings
  TEXTR_WHITELIST = 0
  mydb = "g1_tmrep_" + scid.lower() 
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE Originator_Cal = 'SVM' AND Reptype = 5 AND (Subtype = 1 OR Subtype = 2 OR Subtype = 3) AND ConstructUtc BETWEEN %s AND %s"
  myvalues = (aos, los)
  TEXTR = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  #Find Wite Listed Errors
  if scid.lower() == 'm03':
    mydb = "g1_tmrep_" + scid.lower() 
    mysqlstmt = "SELECT COUNT(*) FROM entries WHERE Originator_Cal = 'SVM' AND Reptype = 5 AND Subtype = 1 AND Paramid_Cal = 'AUBNR' AND ConstructUtc BETWEEN %s AND %s"
    myvalues = (aos, los)
    TEXTR_WHITELIST = TEXTR_WHITELIST + epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  TEXTR = TEXTR - TEXTR_WHITELIST

  if Fassies <> 0:
    message = "Reconfigurations on " + str(Fassies) + " Functional Assemblies" 
    metopmon_event((scid, orbit, passtype, aos, "SVM", message, 4))
    oknok = 0
  if TANM1 <> 0 or TANM2 <> 0 or TVRPM <> 0 or TEXTR <> 0:
    message = "TMREP Warnings: TANM1 " + str(TANM1) + ", TANM2 " + str(TANM2) + ", TVRPM " + str(TVRPM) + ", TEXTR " + str(TEXTR) 
    metopmon_event((scid, orbit, passtype, aos, "SVM", message, 3))
    oknok = 0
  if Fassies == 0 and TANM1 == 0 and TANM2 == 0 and TVRPM == 0 and TEXTR == 0:
    message = "No RED OOLs or TMREP Warnings"
    metopmon_event((scid, orbit, passtype, aos, "SVM", message, 1))

  return (oknok)
  
  
##############################################################################################################
#### MAIN LOOP - GET LIST OF PASSES IN LAST ORBIT, THEN FOR EACH PASS CHECK WHETHER IT HAS BEEN PROCESSED ####
##############################################################################################################

mypasses = metopmon_query("SELECT * FROM passes WHERE los BETWEEN DATE_ADD(NOW(), INTERVAL - 101 MINUTE) AND NOW()", 0)

for mypass in mypasses:

  scid = mypass[0]
  orbit = mypass[1]
  anx = mypass[2]
  aos = mypass[3]
  los = mypass[4]
  passtype = mypass[5]

  #Get any matching passes already in processed_passes
  mycheckpass = metopmon_query("SELECT scid, orbit, status, tries FROM processed_passes where scid = %s and orbit = %s", (scid, orbit))
  
  #This can be a NEW pass, a FISHY pass (i.e. one previously found to be lacking events), or a pass which is being processed    
  if len(mycheckpass) == 0: 
    status = 'NEW'
    tries = 0
  if len(mycheckpass) > 0:
    status = mycheckpass[0][2]
    tries = mycheckpass[0][3]
  
  retryLimit = 30
  eventsLimit = 20
  
  #If this is a NEW pass or a FISHY one which has been tried less than retry Limit
  if status == 'NEW' or (status == 'FISHY' and tries < retryLimit):
    eventsActual = process_stream(scid, orbit, anx, aos, los, passtype)
    #If we see more than 20 entries between aos and los, the pass is ready to process
    if eventsActual >= eventsLimit:      
      if status == 'NEW':
        metopmon_insert("INSERT INTO processed_passes (scid, orbit, status, tries) VALUES (%s, %s, %s, %s)", (scid, orbit, 'READY', 1)) 
      elif status == 'FISHY':
        metopmon_insert("UPDATE processed_passes SET status = 'READY' WHERE scid = %s AND orbit = %s", (scid, orbit))   
      status = 'READY'
      print(str(datetime.datetime.now()) + ": " + scid + " pass " + str(orbit) + " is ready to be processed")          
    #if we have less than 20...  
    elif eventsActual < eventsLimit:
      #....and this is a new pass, then insert it into processed passes with tries = 1.....
      if status == 'NEW':
        metopmon_insert("INSERT INTO processed_passes (scid, orbit, status, tries) VALUES (%s, %s, %s, %s)", (scid, orbit, 'FISHY', 1))
      #....otherwise, increment the 'tries' counter      
      elif status == 'FISHY':
        tries = tries + 1
        metopmon_insert("UPDATE processed_passes SET tries = %s WHERE scid = %s AND orbit = %s", (tries, scid, orbit))
      print(str(datetime.datetime.now()) + ": " + scid + " pass " + str(orbit) + " is fishy after "  + str(tries) +  " tries") 
  
  #If this is a FISHY pass which has been tried as many times as the retry Limit    
  if status == 'FISHY' and tries >= retryLimit:
    status == 'FAILED'
    message = "Not enough events for this pass after " + str(retryLimit) + " retries"
    metopmon_event((scid, orbit, passtype, aos, "STREAM", message, 3)) 
    metopmon_insert("UPDATE processed_passes SET status = 'FAILED' WHERE scid = %s AND orbit = %s", (scid, orbit))
    print(str(datetime.datetime.now()) + ": " + scid + " pass " + str(orbit) + " has failed after "  + str(tries) +  " tries")  
     
  
  #If the Pass is ready, then go for it!
  if status == 'READY':
    

    #Indicate the processing has started in the database - it can take more than 1 minute to process a pass!
    metopmon_insert("UPDATE processed_passes SET status = 'STARTED' WHERE scid = %s AND orbit = %s", (scid, orbit))
    complete = 0
    print(str(datetime.datetime.now()) + ": " + scid + " pass " + str(orbit) + " is being processed") 
    
    #We may have started processing immediately as events are available, so give 60s for TMREP and TCHIST to catch up.
    time.sleep(60)    
    
    try:
      
      #Process each 'system' one at a time. Apply some basic logic - e.g. don't check TC if NO TLM etc.
      
      pi = process_pi(scid, orbit, anx, aos, los, passtype) 
      tlm = process_tlm(scid, orbit, anx, aos, los, passtype)  
      if tlm == 1:
        tc = process_tc(scid, orbit, anx, aos, los, passtype)
        sys = process_sys(scid, orbit, anx, aos, los, passtype)
      if tlm == 1 and sys == 1:
        plm = process_plm(scid, orbit, anx, aos, los, passtype)  
        svm = process_svm(scid, orbit, anx, aos, los, passtype)
        ins = process_ins(scid, orbit, anx, aos, los, passtype)        
      if tlm == 1 and sys == 1 and passtype <> "DEF_ROUT":
        icureps = process_icureports(scid, orbit, anx, aos, los, passtype) 
      complete = 1
    except:
      print(str(datetime.datetime.now()) + ": " + scid + " pass " + str(orbit) + " failed somewhere") 
      metopmon_insert("DELETE FROM processed_passes WHERE scid = %s AND orbit = %s", (scid, orbit))
      metopmon_insert("DELETE FROM events WHERE scid = %s AND orbit = %s", (scid, orbit))      

    if complete == 1:
      print(str(datetime.datetime.now()) + ": " + scid + " pass " + str(orbit) + " processed successfully") 
      metopmon_insert("UPDATE processed_passes SET status = 'COMPLETED' WHERE scid = %s AND orbit = %s", (scid, orbit))
