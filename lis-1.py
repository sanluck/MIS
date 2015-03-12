#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# lis-1.py - запрос заданий на выполнение исследований
#

import os
import sys
import ConfigParser
import time
from datetime import datetime
import logging

Config = ConfigParser.ConfigParser()
PATH = os.path.dirname(sys.argv[0])
FINI = PATH + "/" + "lis.ini"

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

import fdb
from dbmis_connect2 import DBMIS
from dbmysql_connect import DBMY

if __name__ == "__main__":
    LOG_FILENAME = '_lis1.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

from ConfigSection import ConfigSectionMap
# read INI data
Config.read(FINI)
# [DBMIS]
Config1 = ConfigSectionMap(Config, "DBMIS")
HOST = Config1['host']
DB = Config1['db']

Config2 = ConfigSectionMap(Config, "CLINIC")
CLINIC_ID = int(Config2['clinic_id'])

Config3 = ConfigSectionMap(Config, "LIS")
SDATE = Config3['date']
DATE = datetime.strptime(SDATE,"%Y-%m-%d")

STEP = 100

SQLT_T = """SELECT ticket_id FROM tickets WHERE visit_date = ?;"""
SQLT_T1 = """SELECT ticket_id FROM tickets WHERE clinic_id_fk = ? AND visit_date = ?;"""

SQLT_MT = """SELECT mt.med_test_id
FROM med_tests mt
WHERE mt.ticket_id_fk IN (SELECT t.ticket_id
FROM tickets t WHERE t.visit_date = ?);"""

if __name__ == "__main__":

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('-----------------------------------------------------------------------------------')
    log.info('LIS1 Start {0}'.format(localtime))

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    dbc = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    
    cname = dbc.name.encode('utf-8')
    sout = "clinic_id: {0} clinic_name: {1}".format(CLINIC_ID, cname)
    log.info(sout)
    
    sout = "Request date: {0}".format(DATE.strftime("%Y-%m-%d"))
    log.info(sout)
    
    cur = dbc.con.cursor()
    
    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = dbc.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    ro_cur = ro_transaction.cursor()    
    
    ro_cur.execute(SQLT_T, (DATE,))
    
    recs = ro_cur.fetchall()
    
    tlist = []
    for rec in recs:
        ticket_id = rec[0]
        tlist.append(ticket_id)
        
    
    ltlist = len(tlist)
    sout = "Totally we have got {0} tickets".format(ltlist)
    log.info(sout)

    ro_cur.execute(SQLT_T1, (CLINIC_ID, DATE,))
    recs = ro_cur.fetchall()
    
    tlist1 = []
    for rec in recs:
        ticket_id = rec[0]
        tlist1.append(ticket_id)
        
    
    ltlist1 = len(tlist1)
    sout = "Clinic {0} has got {1} tickets".format(CLINIC_ID, ltlist1)
    log.info(sout)

    ro_cur.execute(SQLT_MT, (DATE,))
    recs = ro_cur.fetchall()
    
    mtlist = []
    for rec in recs:
        med_test_id = rec[0]
        mtlist.append(med_test_id)
    
    lmtlist = len(mtlist)
    sout = "Totally we have got {0} med_tests".format(lmtlist)
    log.info(sout)
    
    dbc.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('-----------------------------------------------------------------------------------')
    log.info('LIS1 Finish {0}'.format(localtime))
    sys.exit(0)
    