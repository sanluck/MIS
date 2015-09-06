#!/usr/bin/python
# -*- coding: utf-8 -*-
# d-list3.py - составление отчетной таблицы
#              по результатам работы d-list1, d-list2
#
#

import logging
import sys, codecs
from datetime import datetime

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_dlist3.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

STEP = 1000

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"
CLINIC_ID = 22

SQLT1 = """SELECT id, md_id, c_check_id, lname, fname, mname, sex, birth_dt,
d_end_1, lpu, mkb_itog, d_dt
FROM md$list
WHERE c_check_id is not Null
AND p_print = 1;"""

SQLF1 = """SELECT cc.clinical_checkup_id, cc.clinic_id_fk,
cc.ds_1,
cl.clinic_name
FROM clinical_checkups cc
LEFT JOIN clinics cl ON cc.clinic_id_fk = cl.clinic_id
WHERE
cc.clinical_checkup_id = ?;"""

F_NAME = "md_report.xls"
F_PATH = "MEDDEM"
F_FNAME = F_PATH + "/" + F_NAME

if __name__ == "__main__":
    import time
    import datetime
    from dbmis_connect2 import DBMIS
    from dbmysql_connect import DBMY
    import xlwt

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('md$list report. Start {0}'.format(localtime))

    dbmy = DBMY()
    curr = dbmy.con.cursor()
    curu = dbmy.con.cursor()

    p_arr = []
    curr.execute(SQLT1)
    recs = curr.fetchall()
    for rec in curr:

        _id      = rec[0]
        md_id    = rec[1]
        cc_id    = rec[2]
        lname    = rec[3]
        fname    = rec[4]
        mname    = rec[5]
        sex      = rec[6]
        birth    = rec[7]
        d_end_1  = rec[8]
        lpu      = rec[9]
        mkb_itog = rec[10]
        d_dt     = rec[11]
        p_arr.append([_id, md_id, cc_id, lname, fname, mname, sex, birth, d_end_1, lpu, mkb_itog, d_dt])

    nnn = len(p_arr)
    sout = "Totally {0} cases to be processed".format(nnn)
    log.info(sout)

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    clinic_id = CLINIC_ID
    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
    cursor = dbc.con.cursor()

    wb = xlwt.Workbook(encoding='cp1251')
    ws = wb.add_sheet('Report')

    ws.write(0,0,"FIO")
    ws.write(0,1,"G")
    ws.write(0,2,"BD")
    ws.write(0,3,"DVN Date")
    ws.write(0,4,"D Date")
    ws.write(0,5,"DVN Clinic")
    ws.write(0,6,"D Clinic")
    ws.write(0,7,"DVN DS1")
    ws.write(0,8,"D DS")

    row = 1

    for p_md in p_arr:

        _id      = p_md[0]
        md_id    = p_md[1]
        cc_id    = p_md[2]
        lname    = p_md[3]
        fname    = p_md[4]
        mname    = p_md[5]
        sex      = p_md[6]
        birth    = p_md[7]
        d_end_1  = p_md[8]
        lpu      = p_md[9]
        mkb_itog = p_md[10]
        d_dt     = p_md[11]

        cursor.execute(SQLF1, (cc_id, ))
        rec = cursor.fetchone()
        if rec is None:
            clinic_id = "-"
            ds_1 = "-"
            clinic_name = "-"
        else:
            clinic_id = rec[1]
            ds_1 = rec[2]
            clinic_name = rec[3]

        row += 1

        fio = lname.strip() + " " + fname.strip()
        if mname is not None: fio += " " + mname.strip()
        ws.write(row,0,fio)
        ws.write(row,1,sex)
        bd = birth.strftime("%Y-%m-%d")
        ws.write(row,2,bd)
        if d_end_1 is None:
            d1 = "-"
        else:
            d1 = d_end_1.strftime("%Y-%m-%d")
        ws.write(row,3,d1)
        d2 = d_dt.strftime("%Y-%m-%d")
        ws.write(row,4,d2)
        ws.write(row,5,clinic_name)
        ws.write(row,6,lpu)
        ws.write(row,7,ds_1)
        ws.write(row,8,mkb_itog)



    wb.save(F_FNAME)

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('md$list report. Finish  '+localtime)

    dbc.close()
    dbmy.close()
    sys.exit(0)
