#!/usr/bin/python
# -*- coding: utf-8 -*-
# dvn-f2.py - формирование маршрутных карт ДВН
#             по сформированным записям в clinical_checkups
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()
from insorglist import InsorgInfoList
insorgs = InsorgInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_dvnf2.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

STEP = 100

CC_LINES = []
CC_DOCS = {}
CURRENT_CLINIC = 0

def add_ccr(db, cur, cc_id, people_id, clinic_id):
    # create records in the clinical_checkup_results table
    import datetime
    import random
    from PatientInfo2 import PatientInfo2
    from dbmis_connect2 import dset

    p_obj = PatientInfo2()

    p_obj.initFromCur(cur, people_id)

    s_sqlt = """SELECT
    people_id_fk, date_stage_1, date_end_1
    FROM clinical_checkups
    WHERE clinical_checkup_id = {0};"""
    s_sql = s_sqlt.format(cc_id)
    cur.execute(s_sql)
    rec = cur.fetchone()
    if rec == None:
        return 0

    date_stage_1 = rec[1]
    date_end_1   = rec[2]
    d1 = "%04d-%02d-%02d" % (date_stage_1.year, date_stage_1.month, date_stage_1.day)
    d2 = "%04d-%02d-%02d" % (date_end_1.year, date_end_1.month, date_end_1.day)

    # yc  = datetime.date.today().year # current year
    yc = date_stage_1.year
    bd  = p_obj.birthday
    yp  = bd.year
    age = yc - yp
    sex = p_obj.sex

    for rec in CC_LINES:
        cc_line          = rec[0]
        cc_version_id_fk = rec[1]
        cc_sex           = rec[2]
        cc_ds            = rec[3]
        cc_age           = rec[4]

        if (cc_sex is not None) and (cc_sex <> sex): continue
        cc_age_list = [int(i) for i in cc_age.split(',')]
        if age not in cc_age_list: continue

        if cc_line in (10, 11, 13):
            ddd = dset(d1, d2)
            cc_docs_list = CC_DOCS[136]
            if len(cc_docs_list) < 1: cc_docs_list = CC_DOCS[97]
            ln = len(cc_docs_list)
            if ln == 0:
                worker_id = None
                doctor_id = None
                sout = "No doctors for people_id: {0} clinic_id: {1}".format(people_id, clinic_id)
                log.warn(sout)
            else:
                i = random.randint(0,ln-1)
                worker_id = cc_docs_list[i][0]
                doctor_id = cc_docs_list[i][1]
        elif cc_line == 23:
            ddd = dset(d1, d2)
            cc_docs_list = CC_DOCS[53]
            if len(cc_docs_list) <1: cc_docs_list = CC_DOCS[97]
            ln = len(cc_docs_list)
            if ln == 0:
                worker_id = None
                doctor_id = None
                sout = "No doctors for people_id: {0} clinic_id: {1}".format(people_id, clinic_id)
                log.warn(sout)
            else:
                i = random.randint(0,ln-1)
                worker_id = cc_docs_list[i][0]
                doctor_id = cc_docs_list[i][1]
        elif cc_line in (24, 25):
            ddd = d2
            cc_docs_list = CC_DOCS[97]
            ln = len(cc_docs_list)
            if ln == 0:
                worker_id = None
                doctor_id = None
                sout = "No doctors for people_id: {0} clinic_id: {1}".format(people_id, clinic_id)
                log.warn(sout)
            else:
                i = random.randint(0,ln-1)
                worker_id = cc_docs_list[i][0]
                doctor_id = cc_docs_list[i][1]
        else:
            ddd = dset(d1, d2)
            worker_id = None
            doctor_id = None

        if worker_id == None:
            s_sqlt = """INSERT INTO clinical_checkup_results
            (clinical_checkup_id_fk, cc_line, cc_version_id_fk,
            date_checkup, diagnosis_id_fk, order_no, status)
            VALUES
            ({0}, {1}, {2}, '{3}', '{4}', 0, 0);"""
            s_sql = s_sqlt.format(cc_id, cc_line, cc_version_id_fk, ddd, cc_ds)
        else:
            s_sqlt = """INSERT INTO clinical_checkup_results
            (clinical_checkup_id_fk, cc_line, cc_version_id_fk,
            worker_id_fk, doctor_id_fk,
            date_checkup, diagnosis_id_fk, order_no, status)
            VALUES
            ({0}, {1}, {2}, {3}, {4}, '{5}', '{6}', 0, 0);"""
            s_sql = s_sqlt.format(cc_id, cc_line, cc_version_id_fk, worker_id, doctor_id, ddd, cc_ds)

        db.con.execute_immediate(s_sql) #cur.execute(s_sql)
        db.con.commit()

    return 1

def register_ccr(dbmy, cc_id):
    import datetime

    today = datetime.datetime.today()
    d_now = "%04d-%02d-%02d" % (today.year, today.month, today.day)

    # register clinical_checkup_results in the MySQL database (ct216.ctmed.ru:mis)
    s_sqlt = """UPDATE clinical_checkups SET
    ccr_dcreated = '{0}'
    WHERE cc_id = {1}
    """
    s_sql = s_sqlt.format(d_now, cc_id)
    cursor = dbmy.con.cursor()
    cursor.execute(s_sql)
    dbmy.con.commit()

def get_cclist(db):
    s_sqlt = """SELECT cc_id, people_id, clinic_id
FROM clinical_checkups
WHERE (cc_id is Not Null) AND (cc_id != 0) AND (ccr_dcreated is Null)
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

def get_cclines(db):
    s_sqlt = """SELECT
cc_line, cc_version_id_fk, cc_sex, cc_ds, cc_age
FROM clinical_checkup_list
WHERE cc_stage = 1;"""
    s_sql = s_sqlt
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    records = cursor.fetchall()
    ar = []
    for rec in records:
        cc_line          = rec[0]
        cc_version_id_fk = rec[1]
        cc_sex           = rec[2]
        cc_ds            = rec[3]
        cc_age           = rec[4]
        ar.append([cc_line, cc_version_id_fk, cc_sex, cc_ds, cc_age])
    return ar

def get_ccdoc(db, clinic_id, medical_help_profile_id):
    s_sqlt = """SELECT w.worker_id, w.doctor_id_fk, doc.people_id_fk, p.insurance_certificate
FROM workers w
LEFT JOIN departments dep ON w.department_id_fk = dep.department_id
LEFT JOIN doctors doc ON w.doctor_id_fk = doc.doctor_id
LEFT JOIN peoples p ON doc.people_id_fk = p.people_id
WHERE dep.clinic_id_fk = {0}
AND w.medical_help_profile_id_fk = {1}
AND w.status = 0 AND p.insurance_certificate is Not Null;"""

    s_sql = s_sqlt.format(clinic_id, medical_help_profile_id)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    records = cursor.fetchall()
    ar = []
    for rec in records:
        worker_id = rec[0]
        doctor_id = rec[1]
        ar.append([worker_id, doctor_id])

    return ar

def get_ccdocs(db, clinic_id):
# 136 - акушерство и гинекология
    CC_DOCS[136] = get_ccdoc(db, clinic_id, 136)
# 53 - неврология
    CC_DOCS[53] = get_ccdoc(db, clinic_id, 53)
# 97 - терапия
    CC_DOCS[97] = get_ccdoc(db, clinic_id, 97)



if __name__ == "__main__":

    import os
    import datetime

    from dbmis_connect2 import DBMIS
    from dbmysql_connect import DBMY
    from PatientInfo2 import PatientInfo2

    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('DVN Clinical Checkups Results Processing Start {0}'.format(localtime))

    dbmy = DBMY()

    ar = get_cclist(dbmy)

    dbc2 = DBMIS()
    cur2 = dbc2.con.cursor()

    CC_LINES = get_cclines(dbc2)

    ncount = 0
    for rec in ar:
        ncount += 1
        cc_id     = rec[0]
        people_id = rec[1]
        clinic_id = rec[2]

        if clinic_id <> CURRENT_CLINIC:
            get_ccdocs(dbc2, clinic_id)
            CURRENT_CLINIC = clinic_id

        result = add_ccr(dbc2, cur2, cc_id, people_id, clinic_id)

        if result == 1: register_ccr(dbmy, cc_id)

        if ncount % STEP == 0:
            sout = " {0} cc_id: {1} people_id: {2}".format(ncount, cc_id, people_id)
            log.info(sout)

    dbmy.close()
    dbc2.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('DVN Clinical Checkups Results Processing Finish  '+localtime)
    sout = 'Totally {0} patients have been processed'.format(ncount)
    log.info( sout )

    sys.exit(0)
