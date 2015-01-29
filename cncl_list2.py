#!/usr/bin/python
# -*- coding: utf-8 -*-
# cncl_list2.py - для заполненного списка ЛПУ (cncl_clinics) в базе web2py_mis
#                выполняется расчет количества талонов,
#                имеющих заполненные заключения
#

import logging
import os
import sys, codecs
import ConfigParser

from dbmysql_connect import DBMY

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_cncl_list2.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

Config = ConfigParser.ConfigParser()
PATH = os.getcwd()
FINI = PATH + "/" + "cncl_list.ini"

from ConfigSection import ConfigSectionMap
# read INI data
Config.read(FINI)
# [DBMIS]
Config1 = ConfigSectionMap(Config, "DBMIS")
HOST = Config1['host']
DB = Config1['db']

sout = "Database: {0}:{1}".format(HOST, DB)
log.info(sout)

# [CNCL_COUNT]
Config2 = ConfigSectionMap(Config, "CNCL_COUNT")
D_START = Config2['d_start']
D_FINISH = Config2['d_finish']
C_METHOD = int(Config2['c_method'])
# C_METHOD: метод расчета
# 1 - учитывать только "Результаты осмотра"
# 2 - учитывать только "Рекомендации"
# иначе - учитывать и "Результаты осмотра" и "Результаты осмотра"
#         (должно быть заполнено что-то одно из двух)
#

sout = "Диапазон дат: [{0}] - [{1}]".format(D_START, D_FINISH)
log.info( sout )

log.info("Метод расчета:")
if C_METHOD == 1:
    sout = '1 - учитывать только "Результаты осмотра"'
elif C_METHOD == 2:
    sout = '2 - учитывать только "Рекомендации"'
else:
    sout = 'учитывать и "Результаты осмотра" и "Результаты осмотра"'

log.info( sout )

CNCL_MAX = int(Config2['cncl_max'])
# CNCL_MAX: максимальное количесвто обнаруженных заполненных заключений
# после чего расчет по текущей клинике завершается
# если поставить ноль или меньше, то будут просчитаны все заключения
#

NEW_COUNT = int(Config2['new_count'])
# Если = 1, то расчет с начала по всем клиникам
# иначе - продолжаем расчет

FNAME = Config2['fname']
# имя файла для записи результата
#

SQLT_TCOUNT = """SELECT count(t.ticket_id)
FROM tickets t
WHERE t.clinic_id_fk = ?
AND t.visit_date BETWEEN ? AND ?;"""

SQLT_CLIST = """SELECT t.ticket_id,
c.ms_uid
FROM tickets t
LEFT JOIN conclusions c ON t.ticket_id = c.ticket_id_fk
WHERE t.clinic_id_fk = ?
AND t.visit_date BETWEEN ? AND ?
AND c.ms_uid is not Null;"""

MY_DB = "web2py_mis"
MY_DBHOST = "ct216.ctmed.ru"

SQLT_NEWCOUNT = """UPDATE cncl_clinics
SET cncl_count = Null
WHERE id > 0;"""

SQLT_GLIST = """SELECT
id, clinic_id, name, t_count
FROM cncl_clinics
WHERE cncl_count is Null;"""

SQLT_ILIST = """INSERT INTO cncl_clinics
(clinic_id, mcod, name, t_count)
VALUES
(%s, %s, %s, %s);"""

SQLT_ULIST = """UPDATE cncl_clinics
SET
cncl_count = %s,
t_count  = %s,
uid_count = %s
WHERE id = %s;"""

STEP = 1000

def get_uid_list(clinic_id, d_start = D_START, d_end = D_FINISH):
    import fdb
    from datetime import datetime
    from dbmis_connect2 import DBMIS

    d1 = datetime.strptime(d_start, '%Y-%m-%d')
    d2 = datetime.strptime(d_end, '%Y-%m-%d')

    c_arr = []

    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
    cur = dbc.con.cursor()

    dbc.con.begin(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)

    cur.execute(SQLT_TCOUNT, (clinic_id, d1, d2, ))
    rec = cur.fetchone()
    dbc.con.commit()
    if rec is None: return c_arr, 0
    t_count = rec[0]

    cur.execute(SQLT_CLIST, (clinic_id, d1, d2, ))
    results = cur.fetchall()
    dbc.con.commit()

    for rec in results:
        ticket_id = rec[0]
        ms_uid = rec[1]
        c_arr.append([ticket_id, ms_uid])

    dbc.close()

    return c_arr, t_count

def write_result(fname = FNAME):
    import xlwt

    dbmy = DBMY(host = MY_DBHOST, db = MY_DB)
    cur = dbmy.con.cursor()
    ssql = "SELECT * FROM cncl_clinics;"
    cur.execute(ssql)
    result = cur.fetchall()

    wb = xlwt.Workbook(encoding='cp1251')
    ws = wb.add_sheet('Conclusions Count')

    ws.write(0,0,"clinic_id")
    ws.write(0,1,"mcod")
    ws.write(0,2,"name")
    ws.write(0,3,"cncl_count")
    ws.write(0,4,"t_count")
    ws.write(0,5,"uid_count")

    row = 1
    for rec in result:
        clinic_id = rec[1]
        mcod = rec[2]
        name = rec[3]
        cncl_count = rec[4]
        t_count = rec[5]
        uid_count = rec[6]

        row += 1

        ws.write(row,0,clinic_id)
        ws.write(row,1,mcod)
        ws.write(row,2,name)
        ws.write(row,3,cncl_count)
        ws.write(row,4,t_count)
        ws.write(row,5,uid_count)

    wb.save(fname)
    dbmy.close()

if __name__ == "__main__":
    import time
    from medlib.modules.medobjects.Tickets import Ticket, get_from_mongo
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('----------------------------------------------------------------------------------')
    log.info('Get Conclusions Count. Start {0}'.format(localtime))

    dbmy = DBMY(host = MY_DBHOST, db = MY_DB)
    curr = dbmy.con.cursor()
    curm = dbmy.con.cursor()

    # cncl_list = get_cncl_list()

    if NEW_COUNT == 1:
    # Если = 1, то расчет с начала по всем клиникам
        curm.execute(SQLT_NEWCOUNT)
        dbmy.con.commit()
        log.info("Расчет с начала по всем клиникам")
    else:
        log.info("Продолжение расчета по клиникам")

    cncl_list = []

    curr.execute(SQLT_GLIST)
    result = curr.fetchall()
    for rec in result:
        # id, clinic_id, mcod, name, t_count
        _id  = rec[0]
        c_id = rec[1]
        name = rec[2]
        tcnt = rec[3]
        ccnt = None
        cncl_list.append([_id, c_id, name, tcnt, ccnt])

    for clinic in cncl_list:
        _id  = clinic[0]
        c_id = clinic[1]
        name = clinic[2].encode("utf-8")
        tcnt = clinic[3]

        uid_list, t_count = get_uid_list(c_id)
        l_uid = len(uid_list)

        localtime = time.asctime( time.localtime(time.time()) )

        sout = "Start  cncl_count {0} : {1} {2} {3}/{4}".format(localtime, c_id, name, l_uid, t_count)
        log.info(sout)

        cncl_count = 0
        i = 0
        for uid in uid_list:
            t_id   = uid[0]
            ms_uid = uid[1]
            cncl = get_from_mongo(c_id, ms_uid)
            s1 = cncl[0]
            s2 = cncl[1]
            s3 = cncl[2]

            if C_METHOD == 1:
                if len(s1) > 2 : cncl_count += 1
            elif C_METHOD == 2:
                if len(s3) > 2 : cncl_count += 1
            else:
                if (len(s3) > 2) or (len(s1) > 2) : cncl_count += 1

            if (CNCL_MAX > 0) and (cncl_count > CNCL_MAX): break

            i += 1
            if i % STEP == 0: print i, cncl_count, l_uid

        localtime = time.asctime( time.localtime(time.time()) )
        sout = "Finish cncl_count {0} : {1}".format(localtime, cncl_count)
        log.info(sout)
        curm.execute(SQLT_ULIST,(cncl_count, t_count, l_uid, _id, ))
        dbmy.con.commit()

    dbmy.close()
    write_result()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Get Conclusions Count. Finish  '+localtime)

    sys.exit(0)
