#!/usr/bin/python
# -*- coding: utf-8 -*-
# dvn-f1.py - формирование маршрутных карт ДВН
#l             по сформированным записям в clinical_checkups
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

DATE_STAGE_1 = '2013-08-22'
DATE_END_1 = '2013-09-18'
HEALTH_GROUP_1 = 1
RESULT_1 = 317
DS_1 = 'Z00.0'
PEOPLE_STATUS_CODE = 3

CC_LINES = []
CC_DOCS = {}
CURRENT_CLINIC = 0

def add_ccr(db, cc_id, people_id, clinic_id):
    # create records in the clinical_checkup_results table
    from PatientInfo2 import PatientInfo2
    
    p_obj = PatientInfo2()
    
    p_obj.initFromDb(db, people_id)
    
    
    s_sqlt = """INSERT INTO clinical_checkups
    (clinic_id_fk, people_id_fk, date_stage_1, date_end_1, 
    health_group_1, result_1, ds_1,
    people_status_code)
    VALUES
    ({0}, {1}, '{2}', '{3}', {4}, {5}, '{6}');"""
    s_sql = s_sqlt.format(clinic_id, people_id, DATE_STAGE_1, DATE_END_1, HEALTH_GROUP_1, RESULT_1, DS_1, PEOPLE_STATUS_CODE)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    db.con.commit()
    s_sqlt = """SELECT 
    clinical_checkup_id 
    FROM clinical_checkups
    WHERE people_id_fk = {0};"""
    s_sql = s_sqlt.format(people_id)
    cursor.execute(s_sql)
    rec = cursor.fetchone()
    if rec == None:
        return 0
    else:
        return rec[0]
        
def register_ccr(dbmy, cc_id):
    import datetime
    
    today = datetime.datetime.today()
    d_now = "%04d-%02d-%02d" % (today.year, today.month, today.day)
    
    # register clinical_checkup_results in the MySQL database (bs.ctmed.ru:mis)
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
WHERE ccr_dcreated is Null;"""
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
        
        add_ccr(dbc2, cc_id, people_id, clinic_id)
        
        register_ccr(dbmy, cc_id)

        if ncount % STEP == 0:
            sout = " {0} cc_id: {1} people_id: {1}".format(ncount, cc_id, people_id)
            log.info(sout)        
        
    dbmy.close()
    dbc2.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('DVN Clinical Checkups Results Processing Finish  '+localtime)
    sout = 'Totally {0} patients have been processed'.format(ncount)
    log.info( sout )
    
    sys.exit(0)
