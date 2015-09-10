#!/usr/bin/python
# -*- coding: utf-8 -*-
# az-1.py - запрос информации
#           об активности пользователей по ЛПУ
#

import os
import sys, codecs
import logging
import fdb

from dbmis_connect2 import DBMIS

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_az1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

LIMIT = 10
# запрос списка активных пользователей
SQLAUL = """SELECT user_id, count(user_id)
FROM document_log
GROUP BY user_id;"""

SQLT_UINFO = """SELECT
lpu_id_fk
FROM users
WHERE
user_id = ?;"""

SQLT_CINFO = """SELECT
clinic_name, mcod
FROM clinics
WHERE
clinic_id = ?;"""

F_PATH = "./REPORT/"
F_NAME = "az1.xls"
f_fname = F_PATH + F_NAME

if __name__ == "__main__":
    import time
    import xlwt

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('Analyze Request 1 Start {0}'.format(localtime))

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)
    dbc = DBMIS(mis_host = HOST, mis_db = DB)

    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = dbc.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    ro_cur = ro_transaction.cursor()

    ro_cur.execute(SQLAUL)
    results = ro_cur.fetchall()

    aul = []
    for rec in results:
        user_id = rec[0]
        count = rec[1]
        if count < LIMIT: continue
        aul.append(user_id)

    aul_count = len(aul)
    sout = "Totally active users found: {0}".format(aul_count)
    log.info(sout)

    c_users = {}
    for au_id in aul:
        ro_cur.execute(SQLT_UINFO, (au_id,))
        rec = ro_cur.fetchone()
        if not rec: continue
        clinic_id = rec[0]
        if c_users.has_key(clinic_id):
            c_users[clinic_id] += 1
        else:
            c_users[clinic_id] = 1

    sout = "Output results to: {0}".format(f_fname)
    log.info(sout)

    wb = xlwt.Workbook(encoding='utf-8')
    ws = wb.add_sheet(u"Active users")

    row_num = 0

    columns = [
        (u"CLINIC_ID", 3000),
        (u"MCOD", 2000),
        (u"Пользователей", 2000),
        (u"Наименование клиники", 15000),
    ]
    font_style = xlwt.XFStyle()
    font_style.font.bold = True

    for col_num in xrange(len(columns)):
        ws.write(row_num, col_num, columns[col_num][0], font_style)
        # set column width
        ws.col(col_num).width = columns[col_num][1]

    font_style = xlwt.XFStyle()
    font_style.alignment.wrap = 1

    for clinic_id in c_users.keys():
        ucount = c_users[clinic_id]
        ro_cur.execute(SQLT_CINFO, (clinic_id,))
        rec = ro_cur.fetchone()

        if rec:
            moname = rec[0]
            mcod = rec[1]
        else:
            mcod = None
            moname = u""

        row_num += 1
        ws.write(row_num, 0, clinic_id, font_style)
        ws.write(row_num, 1, mcod, font_style)
        ws.write(row_num, 2, ucount, font_style)
        ws.write(row_num, 3, moname, font_style)

    wb.save(f_fname)

    dbc.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Analyze Request 1 Finish  '+localtime)

    sys.exit(0)



