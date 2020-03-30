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
  myconnection = mysql.connector.connect(host="epsmcf.eumetsat.int", user="richarddyer", passwd="doochuM6", database=mydb)
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
  
def process_tlm(scid, orbit, myaos, mylos, mysqlevt):
  #################################################################################################################
  #TM Processing
  #First check connection status
  mydb = "g1_events_" + scid.lower() 
  myvalues = (myaos, mylos, )
  
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = '" + scid.lower() + "s_nom' AND code = 2110 AND eventTime BETWEEN DATE_ADD(%s, INTERVAL -5 MINUTE) AND %s"
  StatCon = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = '" + scid.lower() + "s_nom' AND code = 2111 AND eventTime BETWEEN DATE_ADD(%s, INTERVAL -1 MINUTE) AND %s"
  TmRx = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  
  if StatCon > 0 and TmRx > 0: #TM OK
    myvalues = (scid, orbit, myaos, "TM", "TM Connection OK", 1)
    metopmon_insert(mysqlevt, myvalues) 
  if StatCon == 0: #No TM Connection to CDA
    myvalues = (scid, orbit, myaos, "TM", "No TM Connection to CDA", 2)
    metopmon_insert(mysqlevt, myvalues)    
  if StatCon > 0 and TmRx == 0: #TM Connection to CDA, but no TLM Received - potential NO TLM!!   
    myvalues = (scid, orbit, myaos, "TM", "No TM From Spacecraft despite successful connection to CDA", 4)
    metopmon_insert(mysqlevt, myvalues) 
  
  defrout_pass = 0
  mydb = "fdfdb"
  myvalues = (scid, orbit)
  mysqlstmt = "SELECT utc FROM events WHERE name = 'ANX' and scid = %s and orbit = %s LIMIT 1"
  anx = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0].time() 
  defrout_start = datetime.time(2, 0, 0)
  defrout_end = datetime.time(3, 41, 0)
  if (anx > defrout_start and anx < defrout_end):
    defrout_pass = 1
    print(scid + " " + str(orbit) + " is a DEF_ROUT Pass")
  
  
  ###################
  #Then ICU Reports  
  myICUs=["ASCAT", "GOME", "GRAS", "IASI", "MPU", "NIU"]
  myvalues = (myaos, mylos, ) 
  mydb = "g1_tmrep_" + scid.lower()
  mysqlstmt = "SELECT Originator_Cal FROM entries WHERE Originator >= 10 AND Reptype = 0 AND SourceStream = 'S' AND ObtUtc BETWEEN %s AND %s"
  myICUReps = epsmcf_query(mydb, mysqlstmt, myvalues)
  MissingICUReps = numpy.setdiff1d(myICUs,myICUReps,assume_unique=False).tolist()
  
  if len(MissingICUReps) > 0 and defrout_pass == 0:
    seperator = ', '
    MissingICUReps = "Missing Reports For ICU(s): " + seperator.join(MissingICUReps)
    myvalues = (scid, orbit, myaos, "TM", MissingICUReps, 2)
    metopmon_insert(mysqlevt, myvalues)
  elif len(MissingICUReps) == 0 and defrout_pass == 0:
    myvalues = (scid, orbit, myaos, "TM", "All ICU Reports Received", 1)
    metopmon_insert(mysqlevt, myvalues) 
  elif defrout_pass == 1: 
    myvalues = (scid, orbit, myaos, "TM", "DEF_ROUT Pass", 1)
    metopmon_insert(mysqlevt, myvalues)     

def process_tc(scid, orbit, myaos, mylos, mysqlevt):
  #################################################################################################################
  #TC Processing
  
  queuesNumeric = [2, 3]
  queuesAlpha = ["MISSION", "EVENT"]
  
  for x in queuesNumeric:
    
    queueAlpha = queuesAlpha[queuesNumeric.index(x)]
    
    #Check for queue opening
    mydb = "g1_events_" + scid.lower() 
    myvalues = (myaos, mylos, )
    mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = '" + scid.lower() + "s_nom' AND code = 2702 AND message LIKE '" + queueAlpha + "%' AND eventTime BETWEEN %s AND %s"
    QueueOpen = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
    
    #Check for queue errors
    mydb = "g1_events_" + scid.lower() 
    myvalues = (myaos, mylos, )
    mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = '" + scid.lower() + "s_nom' AND (code = 2803 OR code = 2809) AND message LIKE '" + queueAlpha + "%' AND eventTime BETWEEN %s AND %s"
    QueueErrors = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
    
    #Check for CV Failures      
    myvalues = (myaos, mylos, ) 
    mydb = "g1_tchist_" + scid.lower() 
    mysqlstmt = "SELECT cv_status FROM commands WHERE queue = " + str(x) + " AND cmd_type <> 10 AND build_send_time BETWEEN %s AND %s"
    myTCs = epsmcf_query(mydb, mysqlstmt, myvalues)
    numTCs = len(myTCs)
    numTCsOK = sum([sum(i) for i in myTCs])
    numTCsNOK = numTCs - numTCsOK
    
    if QueueOpen == 0: #Queue failed to Open
      myvalues = (scid, orbit, myaos, "TC", queueAlpha + " Queue Failed to Open", x) # Note that criticality = queue (2 for mission, 3 for event)
      metopmon_insert(mysqlevt, myvalues) 
    elif QueueOpen > 0 and QueueErrors == 0 and numTCsNOK == 0:
      myvalues = (scid, orbit, myaos, "TC", queueAlpha + " Queue " + str(numTCsOK) + " TCs successfully released", 1)
      metopmon_insert(mysqlevt, myvalues) 
    elif QueueOpen > 0 and (numTCsNOK != 0 or QueueErrors !=0):
      myvalues = (scid, orbit, myaos, "TC", queueAlpha + " Queue " + str(numTCsNOK) + " TCs failed CV, " + str(QueueErrors) + " Queue Errors", x) # Note that criticality = queue (2 for mission, 3 for event)
      metopmon_insert(mysqlevt, myvalues) 
 
def process_pi(scid, orbit, myaos, mylos, mysqlevt):  
  #Check for any PI errors in the *last Orbit*
  mydb = "g1_events_" + scid.lower() 
  myvalues = (myaos, myaos, )
  
  #Proc Failures
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = '" + scid.lower() + "s_nom' AND code = 2927 AND message LIKE '%_MP_%' AND eventTime BETWEEN DATE_ADD(%s, INTERVAL -102 MINUTE) AND %s"
  ProcFails = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  
  #Pi Failures
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = '" + scid.lower() + "s_nom' AND code = 2959 AND message LIKE '%_MP_%' AND eventTime BETWEEN DATE_ADD(%s, INTERVAL -102 MINUTE) AND %s"
  PiFails = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  
  if ProcFails == 0 and PiFails == 0: 
    myvalues = (scid, orbit, myaos, "PI", "All MP Procedures OK", 1)
    metopmon_insert(mysqlevt, myvalues) 
  elif ProcFails != 0 or PiFails != 0:
    myvalues = (scid, orbit, myaos, "PI", str(ProcFails) + " MP Procedures Failed Execution, " + str(PiFails) + " Procedures in Activation T/O or Aborted", 1)
    metopmon_insert(mysqlevt, myvalues) 

def process_sys(scid, orbit, myaos, mylos, mysqlevt):  
  #Check for any PI errors in the *last Orbit*
  mydb = "g1_events_" + scid.lower() 
  myvalues = (myaos, mylos, )

  #RRM
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = '" + scid.lower() + "s_nom' AND (code = 106 OR code = 107) AND mnemonic = 'MSTE381' AND eventTime BETWEEN %s AND %s"
  RRM = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  
  #PLSOL
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = '" + scid.lower() + "s_nom' AND (code = 106 OR code = 107) AND mnemonic = 'UZPLM' AND eventTime BETWEEN %s AND %s"
  PLSOL = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  
  #ESM
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = '" + scid.lower() + "s_nom' AND (code = 106 OR code = 107) AND mnemonic = 'UZMODLV' AND eventTime BETWEEN %s AND %s"
  ESM = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  
  if RRM == 0 and PLSOL == 0 and ESM == 0: 
    myvalues = (scid, orbit, myaos, "SYS", "Satellite System OK", 1)
    metopmon_insert(mysqlevt, myvalues) 
  elif RRM != 0 or PLSOL != 0 or ESM !=0 :
    myvalues = (scid, orbit, myaos, "SYS", "Critical System Error: PLSOL = " + str(PLSOL) + ", RRM = " + str(RRM) + ", ESM = " + str(ESM), 4)
    metopmon_insert(mysqlevt, myvalues) 

def process_ins(scid, orbit, myaos, mylos, mysqlevt):    
  ####################################################################################
 
  myICUs=["ASCAT", "GOME", "GRAS", "IASI", "MPU", "NIU"]
  myICUmnems=["BN%", "ON%", "GN%", "EN%", "HN%", "FN%"] 
 
  #Check for ROOLs and TMREP Warnings
  for x in myICUs:   
    
    ICUerrors = 0
    
    #ROOLS
    myICUmnem = myICUmnems[myICUs.index(x)]
    mydb = "g1_events_" + scid.lower() 
    mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = '" + scid.lower() + "s_nom' AND (code = 106 OR code = 107) AND mnemonic LIKE '" + myICUmnem + "' AND eventTime BETWEEN %s AND %s"
    myvalues = (myaos, mylos)
    Rools = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]

    #TMREP Warnings
    mydb = "g1_tmrep_" + scid.lower() 
    mysqlstmt = "SELECT COUNT(*) FROM entries WHERE Originator_Cal = '" + x + "' AND (Reptype = 2 OR Reptype = 4 OR Reptype = 6 OR Reptype = 14) AND ConstructUtc BETWEEN %s AND %s"
    myvalues = (myaos, mylos)
    TmrepWrn = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]


    if TmrepWrn + Rools <> 0:
      ICUerrors = ICUerrors + 1
      myvalues = (scid, orbit, myaos, "INS", x + ": " + str(Rools) + " RED OOLs, " + str(TmrepWrn) + " TMREP Warnings" , 3)
      metopmon_insert(mysqlevt, myvalues)

  if ICUerrors == 0:
    myvalues = (scid, orbit, myaos, "INS", "No RED OOLs or TMREP Warnings" , 1)
    metopmon_insert(mysqlevt, myvalues)

def process_plm(scid, orbit, myaos, mylos, mysqlevt):
  
  #Red OOLs
  mydb = "g1_events_" + scid.lower() 
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = '" + scid.lower() + "s_nom' AND (code = 106 OR code = 107) AND mnemonic LIKE 'LN%' AND eventTime BETWEEN %s AND %s"
  myvalues = (myaos, mylos)
  Rools = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
   
  #TMREP Warnings
  mydb = "g1_tmrep_" + scid.lower() 
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE Originator_Cal = 'PLM' AND (Reptype = 1 OR Reptype = 3) AND ConstructUtc BETWEEN %s AND %s"
  myvalues = (myaos, mylos)
  TmrepWrn = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]

  if TmrepWrn + Rools <> 0:
    myvalues = (scid, orbit, myaos, "PLM", str(Rools) + " RED OOLs, " + str(TmrepWrn) + " TMREP Warnings" , 3)
    metopmon_insert(mysqlevt, myvalues)
    
  if TmrepWrn + Rools == 0:
    myvalues = (scid, orbit, myaos, "PLM", "No RED OOLs or TMREP Warnings" , 1)
    metopmon_insert(mysqlevt, myvalues)

def process_svm(scid, orbit, myaos, mylos, mysqlevt):

  #Functional Assemblies
  mydb = "g1_events_" + scid.lower() 
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE stream = '" + scid.lower() + "s_nom' AND (code = 106 OR code = 107) AND (mnemonic LIKE 'USEF%' OR mnemonic = 'USSADEPL' OR mnemonic Like 'ISCNFUR%') AND eventTime BETWEEN %s AND %s"
  myvalues = (myaos, mylos)
  Fassies = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]  

    
  #TANM1 Warnings
  mydb = "g1_tmrep_" + scid.lower() 
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE Originator_Cal = 'SVM' AND Reptype = 0 AND ConstructUtc BETWEEN %s AND %s"
  myvalues = (myaos, mylos)
  TANM1 = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  #TANM2 Warnings
  mydb = "g1_tmrep_" + scid.lower() 
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE Originator_Cal = 'SVM' AND Reptype = 1 AND ConstructUtc BETWEEN %s AND %s"
  myvalues = (myaos, mylos)
  TANM2 = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  
  #################
  #TVRPM Warnings
  mydb = "g1_tmrep_" + scid.lower() 
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE Originator_Cal = 'SVM' AND Reptype = 3 AND Subtype = 1 AND ConstructUtc BETWEEN %s AND %s"
  myvalues = (myaos, mylos)
  TVRPM = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  
  ##################
  #TEXTR Warnings
  TEXTR_WHITELIST = 0
  mydb = "g1_tmrep_" + scid.lower() 
  mysqlstmt = "SELECT COUNT(*) FROM entries WHERE Originator_Cal = 'SVM' AND Reptype = 5 AND (Subtype = 1 OR Subtype = 2 OR Subtype = 3) AND ConstructUtc BETWEEN %s AND %s"
  myvalues = (myaos, mylos)
  TEXTR = epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  #Find Wite Listed Errors
  if scid.lower() == 'm03':
    mydb = "g1_tmrep_" + scid.lower() 
    mysqlstmt = "SELECT COUNT(*) FROM entries WHERE Originator_Cal = 'SVM' AND Reptype = 5 AND Subtype = 1 AND Paramid_Cal = 'AUBNR' AND ConstructUtc BETWEEN %s AND %s"
    myvalues = (myaos, mylos)
    TEXTR_WHITELIST = TEXTR_WHITELIST + epsmcf_query(mydb, mysqlstmt, myvalues)[0][0]
  TEXTR = TEXTR - TEXTR_WHITELIST

  if Fassies <> 0:
    myvalues = (scid, orbit, myaos, "SVM", "Reconfigurations on " + str(Fassies) + " Functional Assemblies" , 4)
    metopmon_insert(mysqlevt, myvalues)
  if TANM1 <> 0 or TANM2 <> 0 or TVRPM <> 0 or TEXTR <> 0:
    myvalues = (scid, orbit, myaos, "SVM", "TMREP Warnings: TANM1 " + str(TANM1) + ", TANM2 " + str(TANM2) + ", TVRPM " + str(TVRPM) + ", TEXTR " + str(TEXTR) , 3)
    metopmon_insert(mysqlevt, myvalues)
  if Fassies == 0 and TANM1 == 0 and TANM2 == 0 and TVRPM == 0 and TEXTR == 0:
    myvalues = (scid, orbit, myaos, "SVM", "No RED OOLs or TMREP Warnings" , 1)
    metopmon_insert(mysqlevt, myvalues)

      
def process_pass(scid, orbit):
  print(scid + " pass " + str(orbit) + " is being processed.....") 
  
  #get aos & los time of this pass and use aos as timestamp
  mysqlstmt = "SELECT utc FROM events WHERE scid = %s AND orbit = %s AND target = 'CDA' and extra = 'CONSTANT_0' ORDER BY utc ASC"
  myvalues = (scid, orbit, )
  mytimes = epsmcf_query("fdfdb", mysqlstmt, myvalues)
  myaos = mytimes[0][0]
  mylos = mytimes[1][0]
  
  #generic SQL Statement to Insert any event
  mysqlevt = "INSERT INTO events (scid, orbit, aos, subsystem, message, criticality) VALUES (%s, %s, %s, %s, %s, %s)"

  process_tlm(scid, orbit, myaos, mylos, mysqlevt)  
  process_tc(scid, orbit, myaos, mylos, mysqlevt)
  process_pi(scid, orbit, myaos, mylos, mysqlevt) 
  process_sys(scid, orbit, myaos, mylos, mysqlevt)
  process_ins(scid, orbit, myaos, mylos, mysqlevt)  
  process_plm(scid, orbit, myaos, mylos, mysqlevt)  
  process_svm(scid, orbit, myaos, mylos, mysqlevt)   

    
  #############################################################################################################################################
  


  
  
    

#Get the list of passes to be processed. A pass can only be processed if the LOS time is at least 15 minutes in the past and we should check 101 minutes worth (= 1 orbit)

myconnection = mysql.connector.connect(host="localhost", user="metopmon", passwd="metop1", database="metopmon")
mycursor = myconnection.cursor() 
#mycursor.execute("TRUNCATE events")
#myconnection.commit()
#mycursor.execute("TRUNCATE processed_passes")
#myconnection.commit()

mysqlstmt = "SELECT scid, orbit FROM events WHERE (scid = 'M01' OR scid = 'M02' OR scid = 'M03') AND name = 'STAT_LOS' AND target = 'CDA' AND extra = 'CONSTANT_0' AND utc BETWEEN DATE_ADD(NOW(), INTERVAL - 116 MINUTE) AND DATE_ADD(NOW(), INTERVAL -15 MINUTE) LIMIT 100"
myvalues = 0
mypasses = epsmcf_query("fdfdb", mysqlstmt, myvalues)

#Go through the list of passes and check whether they have already been processed
for row in mypasses:

  scid = row[0]
  orbit = row[1]

  myconnection = mysql.connector.connect(host="localhost", user="metopmon", passwd="metop1", database="metopmon")
  mycursor = myconnection.cursor() 
  mysqlstmt = "SELECT scid, orbit FROM processed_passes where scid = %s and orbit = %s"
  myvalues = (scid, orbit, )
  mycursor.execute(mysqlstmt, myvalues)
  mycheckpasses = mycursor.fetchall()
  
  if len(mycheckpasses) > 0:
    print(scid + " pass " + str(orbit) + " has already been processed") 

  if len(mycheckpasses) == 0:
    mysqlstmt = "INSERT INTO processed_passes (scid, orbit) VALUES (%s, %s)" 
    myvalues = (scid, orbit, ) 
    metopmon_insert(mysqlstmt, myvalues)
    process_pass(scid, orbit)
 


