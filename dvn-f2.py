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


def add_ccr(db, cc_id):
    # create records in the clinical_checkup_results table
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
    
def pclinic(clinic_id, mcod):
    from dbmis_connect2 import DBMIS
    from dbmysql_connect import DBMY
    from PatientInfo2 import PatientInfo2
        
    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('DVN List Processing Start {0}'.format(localtime))
    
    fname = FNAME.format(mcod)
    sout = "Input file: {0}".format(fname)
    log.info(sout)
    
    ppp = plist_in(fname)
    
    sout = "Totally {0} lines have been read from file <{1}>".format(len(ppp), fname)
    log.info(sout)

    p_obj = PatientInfo2()

    dbc = DBMIS(clinic_id)
    if dbc.ogrn == None:
        CLINIC_OGRN = u""
    else:
        CLINIC_OGRN = dbc.ogrn

    cogrn = CLINIC_OGRN.encode('utf-8')
    cname = dbc.name.encode('utf-8')
    
    if SKIP_OGRN: CLINIC_OGRN = u""
    
    sout = "clinic_id: {0} clinic_name: {1} clinic_ogrn: {2} cod_mo: {3}".format(clinic_id, cname, cogrn, mcod)
    log.info(sout)
    
    if dbc.clinic_areas == None:
        sout = "Clinic has not got any areas"
        log.warn(sout)
        dbc.close()
        return
    else:
        nareas = len(dbc.clinic_areas)
        area_id = dbc.clinic_areas[0][0]
        area_nu = dbc.clinic_areas[0][1]
        sout = "Clinic has got {0} areas".format(nareas)
        log.info(sout)
        sout = "Using area_id: {0} area_number: {1}".format(area_id, area_nu)
        log.info(sout)

    wrong_clinic = 0
    wrong_insorg = 0
    ncount = 0
    dbc2 = DBMIS(clinic_id)
    cur2 = dbc2.con.cursor()
    dbmy = DBMY()

    dvn_number = 0
    
    for prec in ppp:
        ncount += 1
        people_id = prec[0]
        insorg_mcod = prec[2]
        if insorg_mcod == "":
            insorg_id = 0
        else:
            insorg_id = int(insorg_mcod) - 22000
        medical_insurance_series = prec[4]
        medical_insurance_number = prec[5]
        p_obj.initFromDb(dbc, people_id)

        if ncount % STEP == 0:
            sout = " {0} people_id: {1} clinic_id: {2} dvn_number: {3}".format(ncount, people_id, p_obj.clinic_id, dvn_number)
            log.info(sout)

        if clinic_id <> p_obj.clinic_id:
            wrong_clinic += 1
            continue

        if check_person(dbc, people_id):

            # check if clinical_checkups table 
            # already has got a record with current people_id
            if person_in_cc(dbc, people_id): continue
            
            set_insorg(dbc2, people_id, insorg_id, medical_insurance_series, medical_insurance_number)
            
            cc_id = add_cc(dbc2, clinic_id, people_id)
            
            register_cc(dbmy, cc_id, people_id)
            
            dvn_number += 1


    sout = "Wrong clinic: {0}".format(wrong_clinic)
    log.info(sout)
    sout = "DVN cases number: {0}".format(dvn_number)
    log.info(sout)
    
    dbc.close()
    dbc2.close()
    dbmy.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('DVN List Processing Finish  '+localtime)
    
def get_cclist(db):
    s_sqlt = """SELECT cc_id, people_id
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
        ar.append([cc_id, people_id])
    return ar

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
    
    ncount = 0
    for rec in ar:
        ncount += 1
        cc_id     = rec[0]
        people_id = rec[1]
        
        add_ccr(dbc2, cc_id)
        
        register_ccr(dbmy, cc_id)

        if ncount % STEP == 0:
            sout = " {0} cc_id: {1} people_id: {1}".format(ncount, cc_id, people_id)
            log.info(sout)        
        
    dbmy.close()
    dbc2.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('DVN Clinical Checkups Results Processing Finish  '+localtime)
    
    
    sys.exit(0)
