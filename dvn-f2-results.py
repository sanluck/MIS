#!/usr/bin/python
# -*- coding: utf-8 -*-
# dvn-f1-results.py - отчет о сформированных маршрутных картах ДВН
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()
from insorglist import InsorgInfoList
insorgs = InsorgInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_dvnf2result.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

N_MIN = 0
N_MAX = 500


def get_cclist(db):
    s_sqlt = """SELECT cc_id, people_id, clinic_id
FROM clinical_checkups
WHERE (cc_id is Not Null) AND (ccr_dcreated is not Null)
ORDER BY clinic_id;"""
    s_sql = s_sqlt
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    records = cursor.fetchall()
    ar = []
    for rec in records:
        cc_id     = rec[0]
        people_id = rec[1]
        clinic_id = rec[2]
        ar.append([cc_id, people_id, clinic_id])
    return ar

def get_clinics_list(db):
    s_sqlt = """SELECT clinic_id, COUNT(people_id)
    FROM mis.clinical_checkups
    WHERE (cc_id is Not Null) AND (ccr_dcreated is not Null)
    GROUP BY clinic_id
    ORDER BY clinic_id;"""
    s_sql = s_sqlt
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    records = cursor.fetchall()
    ar = []
    for rec in records:
        clinic_id = rec[0]
        count     = rec[1]
        ar.append([clinic_id, count])
    return ar

def rep_clinics(db, car):
    import xlwt
    
    book = xlwt.Workbook(encoding="cp1251")
    sheet1 = book.add_sheet("Clinics List")
    sheet1.write(0, 0, "Clinic_id")
    sheet1.write(0, 1, "Clinic_name")
    sheet1.write(0, 2, "DVN Count")
    
    s_sqlt = """SELECT clinic_name
    FROM clinics
    WHERE clinic_id = {0};"""
    cursor = db.con.cursor()
    
    i = 0
    nnn = 0
    for ccc in car:
        clinic_id = ccc[0]
        count     = ccc[1]
        s_sql = s_sqlt.format(clinic_id)
        cursor.execute(s_sql)
        rec = cursor.fetchone()
        if rec is None:
            clinic_name = u"None"
        else:
            clinic_name = rec[0]
        
        i += 1
        sheet1.write(i, 0, clinic_id)
        sheet1.write(i, 1, clinic_name)
        sheet1.write(i, 2, count)
        nnn += count
        
    i += 2
    
    sheet1.write(i, 1, u'ИТОГО')
    sheet1.write(i, 2, nnn)
    
    book.save("dvn_rep1.xls")

def rep_peoples(db, ar):
    import xlwt
    
    book = xlwt.Workbook(encoding="cp1251")
    sheet1 = book.add_sheet("Patients List")
    sheet1.write(0, 0, "Clinic_id")
    sheet1.write(0, 1, "People_id")
    sheet1.write(0, 2, "FIO")
    
    s_sqlt = """SELECT lname, fname, mname
    FROM peoples
    WHERE people_id = {0};"""
    cursor = db.con.cursor()
    
    i = 0
    n = 0
    for ppp in ar:
        
        n += 1
        
        if n < N_MIN: continue
        if n > N_MAX: break
        
        cc_id     = ppp[0]
        people_id = ppp[1]
        clinic_id = ppp[2]

        s_sql = s_sqlt.format(people_id)
        cursor.execute(s_sql)
        rec = cursor.fetchone()
        if rec is None:
            fio = u"None"
        else:
            if rec[0] is None:
                fname = ""
            else:
                fname = rec[0].encode('cp1251')
            
            if rec[1] is None:
                lname = ""
            else:
                lname = rec[1].encode('cp1251')

            if rec[2] is None:
                mname = ""
            else:
                mname = rec[2].encode('cp1251')

                
            fio = "{0} {1} {2}".format(fname, lname, mname)
        
        i += 1
        sheet1.write(i, 0, clinic_id)
        sheet1.write(i, 1, people_id)
        sheet1.write(i, 2, fio)
        
    book.save("dvn_rep2.xls")


if __name__ == "__main__":
    
    import os    
    import datetime

    from dbmis_connect2 import DBMIS
    from dbmysql_connect import DBMY
    from PatientInfo2 import PatientInfo2
        
    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('DVN Clinical Checkups Results Report Start {0}'.format(localtime))

    dbmy = DBMY()

    car = get_clinics_list(dbmy)
    
    ar  = get_cclist(dbmy)

    dbc2 = DBMIS()
    
    rep_clinics(dbc2, car)
    
    rep_peoples(dbc2, ar)
        
    dbmy.close()
    dbc2.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('DVN Clinical Checkups Results Report Finish  '+localtime)
    
    sys.exit(0)
