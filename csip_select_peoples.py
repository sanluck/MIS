#!/usr/bin/python
# coding: utf-8
# csip_select_peoples.py - формирование файла
#  с реквизитами, выбранных ранее csip_select пациентов
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

STEP = 100
LIMIT = 1000000

#START_TICKET_ID = 129119089
START_TICKET_ID = 44964514
START_DATE = '2013-01-01'
START_BD = '1998-01-01'

HOST = "fb2.ctmed.ru"
DB = "DBMIS"

M_HOST = "ct216.ctmed.ru"
M_DB = "mis"

SQLT_GET_PLIST = """SELECT DISTINCT people_id FROM csip;"""

SQLT_GET_PEOPLE = """SELECT
fio, sex, birthday,
addr_jure_country_town, addr_jure_street_name, addr_jure_house,
addr_jure_corps, addr_jure_flat,
addr_fact_country_town, addr_fact_street_name, addr_fact_house,
addr_fact_corps, addr_fact_flat
FROM vw_peoples_short
WHERE people_id = ?;"""

F_NAME = "csip_list.xls"
F_PATH = "CSIP"
F_FNAME = F_PATH + "/" + F_NAME

if __name__ == "__main__":

    import time
    import xlwt

    log.info("======================= CSIP_SELECT_PEOPLES=======================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('CSIP SELECT PEOPLES. Start {0}'.format(localtime))

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

    curm.execute(SQLT_GET_PLIST)
    results = curm.fetchall()

    i = 0

    wb = xlwt.Workbook(encoding='cp1251')
    ws = wb.add_sheet('CSIP List')

    ws.write(0,0,"FIO")
    ws.write(0,1,"G")
    ws.write(0,2,"BD")
    ws.write(0,3,"Addres de Jure")
    ws.write(0,4,"Addres de Facto")
    row = 1

    for rec in results:
        people_id = rec[0]

        i += 1

        if i % STEP == 0:
            sout = " {0}: people_id: {1}".format(i, people_id)
            log.info(sout)

        ro_cur.execute(SQLT_GET_PEOPLE, (people_id, ))
        recp = ro_cur.fetchone()
        if recp:
            fio = recp[0]
            sex = recp[1]
            birthday = recp[2]
            addr_jure_country_town = recp[3]
            addr_jure_street_name = recp[4]
            addr_jure_house = recp[5]
            addr_jure_corps = recp[6]
            addr_jure_flat = recp[7]
            addr_fact_country_town = recp[8]
            addr_fact_street_name = recp[9]
            addr_fact_house = recp[10]
            addr_fact_corps = recp[11]
            addr_fact_flat = recp[12]

            if addr_jure_country_town:
                addr_jure = addr_jure_country_town
            else:
                addr_jure = u""

            if addr_jure_street_name:
                addr_jure += u", " + addr_jure_street_name

            if addr_jure_house:
                addr_jure += u", " + addr_jure_house

            if addr_jure_corps:
                addr_jure += u"/" + addr_jure_corps

            if addr_jure_flat:
                addr_jure += u"-" + addr_jure_flat

            if addr_fact_country_town:
                addr_fact = addr_fact_country_town
            else:
                addr_fact = u""

            if addr_fact_street_name:
                addr_fact += u", " + addr_fact_street_name

            if addr_fact_house:
                addr_fact += u", " + addr_fact_house

            if addr_fact_corps:
                addr_fact += u"/" + addr_fact_corps

            if addr_fact_flat:
                addr_fact += u"-" + addr_fact_flat

            row += 1
            ws.write(row,0,fio)
            ws.write(row,1,sex)
            ws.write(row,2,birthday.strftime('%d.%m.%Y'))
            ws.write(row,3,addr_jure)
            ws.write(row,4,addr_fact)

    wb.save(F_FNAME)
    dbmy.con.close()
    dbc.con.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('CSIP SELECT PEOPLES. Finish {0}'.format(localtime))

    sys.exit(0)
