#!/usr/bin/python
# coding: utf-8
# csip_select_2.py - проверка пациентов,
#  выбранных ранее csip_select пациентов
#  применив более жесткие условия
#

import os, sys, codecs
from datetime import datetime

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

from dbmysql_connect import DBMY
import fdb
from dbmis_connect2 import DBMIS

from dbmis2dbmisr import TICKET

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

import logging

LOG_FILENAME = '_csip_select_peoples.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

import ConfigParser
from ConfigSection import ConfigSectionMap

FINI = "csip.ini"
Config = ConfigParser.ConfigParser()
Config.read(FINI)
Config1 = ConfigSectionMap(Config, "CSIP")
START_DATE = Config1['start_date']

sout = "START_DATE: {0}".format(START_DATE)
log.info(sout)
start_date = datetime.strptime(START_DATE, "%Y-%m-%d")


STEP = 100

HOST = "fb2.ctmed.ru"
DB = "DBMIS"

M_HOST = "ct208.ctmed.ru"
M_DB = "mis"

DSS = [['G80.1','G80.2'], \
       ['G80.8','G80.8']]


SQLT_GET_PLIST = """SELECT id, people_id FROM csip;"""

SQLT_GET_DIAGS = """SELECT t.ticket_id, t.worker_id_fk,
td.diagnosis_id_fk,
w.speciality_id_fk
FROM tickets t
LEFT JOIN ticket_diagnosis td ON t.ticket_id = td.ticket_id_fk
LEFT JOIN workers w ON t.worker_id_fk = w.worker_id
WHERE t.people_id_fk = ?
AND t.visit_date >= ?
AND w.speciality_id_fk = 8;"""

SQLT_SET_LUSE = """UPDATE csip
SET luse = %s
WHERE id = %s;"""

def get_plist(cur):

    cur.execute(SQLT_GET_PLIST)
    results = cur.fetchall()

    arr = []
    for rec in results:
        _id = rec[0]
        p_id = rec[1]
        arr.append([_id, p_id])


    return arr

def get_dlist(cur, people_id):

    cur.execute(SQLT_GET_DIAGS, (people_id, start_date, ))
    results = cur.fetchall()

    arr = []
    for rec in results:

        ticket_id = rec[0]
        worker_id_fk = rec[1]
        diagnosis_id_fk = rec[2]
        speciality_id_fk = rec[3]
        arr.append(diagnosis_id_fk)

    return arr


if __name__ == "__main__":

    import time

    log.info("======================= CSIP_SELECT_2 =======================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('CSIP SELECT 2. Start {0}'.format(localtime))

    sout = "DBMY: {0}:{1}".format(M_HOST, M_DB)
    log.info(sout)

    sout = "DBMIS: {0}:{1}".format(HOST, DB)
    log.info(sout)

    dbmy = DBMY(host = M_HOST, db = M_DB)
    curm = dbmy.con.cursor()

    dbc = DBMIS(mis_host = HOST, mis_db = DB)
    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = dbc.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    ro_cur = ro_transaction.cursor()

    p_list = get_plist(curm)
    lp_list = len(p_list)
    sout = "Totally {0} records to be processed".format(lp_list)

    i = 0
    for ppp in p_list:
        _id = ppp[0]
        p_id = ppp[1]

        i += 1

        if i % STEP == 0:
            sout = " {0}: id: {1} people_id: {2}".format(i, _id, p_id)
            log.info(sout)

        dlist = get_dlist(ro_cur, p_id)

        luse = 0
        dcount = 0
        for ds in dlist:
            if not ds: continue

            for DS in DSS:
                ds5 = ds[:5]
                if (ds5 >= DS[0]) and (ds5 <= DS[1]):
                    dcount += 1
                    break

        if dcount > 1:
            luse = 1

        curm.execute(SQLT_SET_LUSE, (luse, _id))
        dbmy.con.commit()

    dbmy.con.close()
    dbc.con.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('CSIP SELECT PEOPLES. Finish {0}'.format(localtime))

    sys.exit(0)
