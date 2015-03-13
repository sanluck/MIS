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
SDATE = Config3['beg_date']
BEG_DATE = datetime.strptime(SDATE,"%Y-%m-%d")
SDATE = Config3['end_date']
END_DATE = datetime.strptime(SDATE,"%Y-%m-%d")

STEP = 100

SQLT_T = """SELECT ticket_id FROM tickets WHERE visit_date between ? AND ?;"""
SQLT_T1 = """SELECT ticket_id FROM tickets WHERE clinic_id_fk = ? AND visit_date between ? AND ?;"""

SQLT_MTO = """SELECT med_tests_order_id, med_test_name
FROM med_tests_order;"""

SQLT_MT = """SELECT mt.med_test_id, mt.med_tests_order_id_fk,
mto.med_test_code, mto.med_tests_lab_id_fk
FROM med_tests mt
LEFT JOIN med_tests_order mto ON mt.med_tests_order_id_fk = mto.med_tests_order_id
WHERE mt.ticket_id_fk IN (SELECT t.ticket_id
FROM tickets t WHERE t.clinic_id_fk = ? AND t.visit_date between ? AND ?);"""

if __name__ == "__main__":

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('===================================================================================')
    log.info('LIS1 Start {0}'.format(localtime))

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    dbc = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)

    cname = dbc.name.encode('utf-8')
    sout = "clinic_id: {0} clinic_name: {1}".format(CLINIC_ID, cname)
    log.info(sout)

    sout = "Request dates: {0} - {1}".format(BEG_DATE.strftime("%Y-%m-%d"), END_DATE.strftime("%Y-%m-%d"))
    log.info(sout)

    cur = dbc.con.cursor()

    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = dbc.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    ro_cur = ro_transaction.cursor()

    ro_cur.execute(SQLT_MTO)
    recs = ro_cur.fetchall()
    # mto - словарь названий методик
    # ключ - order_id (код методики)
    mto = {}
    for rec in recs:
        order_id = rec[0]
        med_test_name = rec[1]
        mto[order_id] = med_test_name

    lmto = len(mto)
    sout = "Totally we have got {0} orders".format(lmto)
    log.info(sout)

    ro_cur.execute(SQLT_T, (BEG_DATE, END_DATE, ))

    recs = ro_cur.fetchall()

    tlist = []
    for rec in recs:
        ticket_id = rec[0]
        tlist.append(ticket_id)


    ltlist = len(tlist)
    sout = "Totally we have got {0} tickets".format(ltlist)
    log.info(sout)

    ro_cur.execute(SQLT_T1, (CLINIC_ID, BEG_DATE, END_DATE, ))
    recs = ro_cur.fetchall()

    tlist1 = []
    for rec in recs:
        ticket_id = rec[0]
        tlist1.append(ticket_id)


    ltlist1 = len(tlist1)
    sout = "Clinic {0} has got {1} tickets".format(CLINIC_ID, ltlist1)
    log.info(sout)

    ro_cur.execute(SQLT_MT, (CLINIC_ID, BEG_DATE, END_DATE,))
    recs = ro_cur.fetchall()

    # mtldict - список заказов по лабораториям
    #
    mtlist = []
    mtldict = {}
    for rec in recs:
        med_test_id = rec[0]
        med_test_order = rec[1]
        med_test_code = rec[2]
        lab_id = rec[3]
        mtlist.append(med_test_id)
        if mtldict.has_key(lab_id):
            mtldict[lab_id].append([med_test_id, med_test_order, med_test_code])
        else:
            mtldict[lab_id] = [[med_test_id, med_test_order, med_test_code]]

    lmtlist = len(mtlist)
    sout = "Totally we have got {0} med_tests".format(lmtlist)
    log.info(sout)

    for lab_id in mtldict.keys():
        # mtl - список заказов в текущей лоборатории
        mtl = mtldict[lab_id]
        lmtl = len(mtl)
        sout = "lab_id: {0} orders: {1}".format(lab_id, lmtl)
        log.info(sout)
        # lmto - количество заказов по методикам (med_test_order)
        lmto = {}
        for mt in mtl:
            med_test_id = mt[0]
            med_test_order = mt[1]
            med_test_code = mt[2]
            if lmto.has_key(med_test_order):
                lmto[med_test_order] += 1
            else:
                lmto[med_test_order] = 1

        for med_test_order in lmto.keys():
            order_count = lmto[med_test_order]
            order_name = mto[med_test_order].strip()
            sout = "{0}: {1}".format(order_count, order_name.encode('utf-8'))
            log.info(sout)

        log.info('-----------------------------------------------------------------------------------')

    dbc.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('LIS1 Finish {0}'.format(localtime))
    sys.exit(0)
