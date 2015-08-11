#!/usr/bin/python
# coding: utf-8
# csip_select.py - выборка пациентов для регистра ДЦП
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

LOG_FILENAME = '_csip_select.out'
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
#START_TICKET_ID = 129119089
START_TICKET_ID = int(Config1['start_ticket_id'])
STEP = int(Config1['step'])
LIMIT = int(Config1['limit'])
START_DATE = Config1['start_date']
START_BD = Config1['start_bd']

HOST = "fb2.ctmed.ru"
DB = "DBMIS"

M_HOST = "ct216.ctmed.ru"
M_DB = "mis"

# выборка талонов для пациентов,
# 17 лет и моложе (на сегодняшний день)
# за период с 2013-01-01

SQLT_GET_TICKETS = """SELECT FIRST ?
t.ticket_id,
t.clinic_id_fk, t.people_id_fk, t.visit_date, t.visit_time,
t.visit_motive_id_fk
FROM tickets t
LEFT JOIN peoples p ON t.people_id_fk = p.people_id
WHERE t.ticket_id > ?
AND t.visit_date >= ?
AND p.birthday >= ?
ORDER BY t.ticket_id;"""

SQLT_FIND_PEOPLE = """SELECT id FROM csip
WHERE people_id = %s
AND clinic_id = %s;"""

SQLT_PUT_CSIP = """INSERT INTO csip
(people_id, clinic_id, mcod)
VALUES
(%s, %s, %s);"""

DSS = [['M25.3','M25.3'], \
       ['M25.6','M25.6'], \
       ['M95.8','M95.8'], \
       ['G80.1','G80.2'], \
       ['G80.8','G80.8']]

if __name__ == "__main__":

    import time

    log.info("======================= CSIP_SELECT ==============================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('CSIP SELECT. Start {0}'.format(localtime))

    sout = "Source database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    sout = "Target database: {0}:{1}".format(M_HOST, M_DB)
    log.info(sout)

    dbc = DBMIS(mis_host = HOST, mis_db = DB)
    cur = dbc.con.cursor()

    dbmy = DBMY(host = M_HOST, db = M_DB)
    curm = dbmy.con.cursor()

    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = dbc.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    ro_cur = ro_transaction.cursor()

    sout = "START_DATE: {0}".format(START_DATE)
    log.info(sout)
    sout = "START_BD: {0}".format(START_BD)
    log.info(sout)
    sout = "START_TICKET_ID: {0}".format(START_TICKET_ID)
    log.info(sout)
    sout = "LIMIT: {0}".format(LIMIT)
    log.info(sout)
    
    cur.execute(SQLT_GET_TICKETS, (LIMIT, START_TICKET_ID, START_DATE, START_BD))
    results = cur.fetchall()

    i = 0

    for rec in results:
        ticket_id = rec[0]
        clinic_id_fk = rec[1]
        people_id_fk = rec[2]
        visit_date = rec[3]
        visit_time = rec[4]
        visit_motive_id_fk = rec[5]

        ticket= TICKET(ticket_id)
        ticket.clinic_id_fk = clinic_id_fk
        ticket.people_id_fk = people_id_fk
        ticket.visit_date = visit_date
        ticket.visit_time = visit_time
        ticket.visit_motive_id_fk = visit_motive_id_fk

        ticket.get_people(ro_cur)
        ticket.get_clinic(ro_cur)
        mcod = ticket.mcod
        ticket.get_diags(ro_cur)

        i += 1

        if i % STEP == 0:
            sout = " {0}: ticket_id: {1}".format(i, ticket_id)
            log.info(sout)

        lcsip = False
        for diag in ticket.diags:
            ds = diag.diagnosis_id_fk
            for DS in DSS:
                ds5 = ds[:5]
                if (ds5 >= DS[0]) and (ds5 <= DS[1]):
                    lcsip = True
                    break

        if lcsip:
            curm.execute(SQLT_FIND_PEOPLE, (people_id_fk, clinic_id_fk))
            rec = curm.fetchone()
            if not rec:
                curm.execute(SQLT_PUT_CSIP, (people_id_fk, clinic_id_fk, mcod))
                dbmy.con.commit()

    sout = "Set START_TICKET_ID to: {0}".format(ticket_id)
    log.info(sout)
    cfgfile = open(FINI,'w')
    Config.set('CSIP','start_ticket_id', ticket_id)
    Config.write(cfgfile)
    cfgfile.close()

    dbmy.con.close()
    dbc.con.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('CSIP SELECT. Finish {0}'.format(localtime))

    sys.exit(0)
