#!/usr/bin/python
# -*- coding: utf-8 -*-
# dvn-f1.py - формирование карт ДВН
#             по списку из ответа на запрос страховой принадлежности
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()
from insorglist import InsorgInfoList
insorgs = InsorgInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_dvnf1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

#clist = (101, 105, 110, 119, 121, 125, 133, 140, 141, 142, 146, 147, 148, 150, 152, 161, 163, 165, 167, 169, 170, 174, 175, 176, 178, 181, 182, 186)
clist  = [224]
molist = [220015]

CLINIC_OGRN = u""

FNAME = "IT22M{0}_13091.csv"

STEP = 100
DOC_TYPES = {1:u"1",
             2:u"2",
             3:u"3"}

SKIP_OGRN = True

SET_MO = False

SET_INSORG = True

DS_WHITE_LIST = []
DS_WHITE_COUNT = 395

D_DATE_STAGE_1 = ['2013-08-15', '2013-09-25']
D_DATE_END_1   = ['2013-09-01', '2013-09-30']
HEALTH_GROUP_1 = 1
RESULT_1 = 317
DS_1 = 'Z00.0'
PEOPLE_STATUS_CODE = 3

def get_wlist(fname="ds_white_list.xls"):
    import xlrd
    workbook = xlrd.open_workbook(fname)
    worksheets = workbook.sheet_names()
    wshn0 = worksheets[0]
    worksheet = workbook.sheet_by_name(wshn0)
    curr_cell = 0
    for curr_row in range(DS_WHITE_COUNT):
        cell_value = worksheet.cell_value(curr_row, curr_cell)
        DS_WHITE_LIST.append(cell_value)
    
    workbook.release_resources()
    

def plist_in(fname):
# read file <fname>
# and get peoples list
    fi = open(fname, "r")
    arr = []
    for line in fi:
        arl = line.split("|")
        sss = arl[4]
        arl[4] = sss.decode('cp1251')
        arr.append(arl)
    fi.close()
    return arr

def check_person(db, people_id):
    import datetime
    s_sqlt = """SELECT t.ticket_id, td.diagnosis_id_fk
FROM tickets t
LEFT JOIN ticket_diagnosis td ON t.ticket_id = td.ticket_id_fk
WHERE t.people_id_fk = {0} and visit_date > '2013-01-01';"""
    s_sql = s_sqlt.format(people_id)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    records = cursor.fetchall()
    for rec in records:
        dss = rec[1]
        if dss == None: continue
        ds = dss.strip()
        if ds not in DS_WHITE_LIST:
            return False
    return True

def person_in_cc(db, people_id):
    # check if clinical_checkups table already has got a record with current people_id
    s_sqlt = "SELECT * FROM clinical_checkups WHERE people_id_fk = {0};"
    s_sql  = s_sqlt.format(people_id)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    rec = cursor.fetchone()
    if rec == None:
        return False
    else:
        return True
    
def set_insorg(db, people_id, insorg_id, medical_insurance_series, medical_insurance_number):
    mi_s = medical_insurance_series.encode('cp1251')
    mi_n = medical_insurance_number.encode('cp1251')
    cursor = db.con.cursor()
    if insorg_id == 0:
        s_sqlt = """UPDATE peoples SET
        medical_insurance_series = '{0}',
        medical_insurance_number = '{1}'
        WHERE people_id = {2};"""
        s_sql = s_sqlt.format(mi_s, mi_n, people_id)
        cursor.execute(s_sql)
    else:
        insorg = insorgs[insorg_id]
        i_name = insorg.name.encode('cp1251')
        i_ogrn = insorg.ogrn
        s_sqlt = """UPDATE peoples SET
        medical_insurance_series = '{0}',
        medical_insurance_number = '{1}',
        medical_insurance_company_name = '{2}',
        insorg_id = {3},
        insorg_ogrn = '{4}'
        WHERE people_id = {5};"""
        s_sql = s_sqlt.format(mi_s, mi_n, i_name, insorg_id, i_ogrn, people_id)
        cursor.execute(s_sql)
    
    db.con.commit()

def add_cc(db, clinic_id, people_id):
    # create record in the clinical_checkups table
    import sys
    from dbmis_connect2 import dset
    from datetime import datetime, timedelta
    d1 = D_DATE_STAGE_1[0]
    d2 = D_DATE_STAGE_1[1]
    DATE_STAGE_1 = dset(d1, d2)
    if D_DATE_END_1[0] > DATE_STAGE_1:
        d1 = D_DATE_END_1[0]
    else:
        d1 = DATE_STAGE_1
        dd1 = datetime.strptime(d1, "%Y-%m-%d").date()
        dd2 = dd1 + timedelta(days=1)
        if dd2.weekday() == 5: dd2 = dd1 + timedelta(days=3)
        d1 = "%04d-%02d-%02d" % (dd2.year, dd2.month, dd2.day)
    d2 = D_DATE_END_1[1]
    DATE_END_1 = dset(d1, d2)
    s_sqlt = """INSERT INTO clinical_checkups
    (clinic_id_fk, people_id_fk, date_stage_1, date_end_1, 
    health_group_1, result_1, ds_1,
    people_status_code)
    VALUES
    ({0}, {1}, '{2}', '{3}', {4}, {5}, '{6}', {7});"""
    s_sql = s_sqlt.format(clinic_id, people_id, DATE_STAGE_1, DATE_END_1, HEALTH_GROUP_1, RESULT_1, DS_1, PEOPLE_STATUS_CODE)
    cursor = db.con.cursor()
    try:
        cursor.execute(s_sql)
        db.con.commit()
    except:
        log.warn('Create record in the clinical_checkups table Error.\SQL Code:\n')
        log.warn(s_sql)
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
        
def register_cc(dbmy, cc_id, people_id, clinic_id):
    import datetime
    today = datetime.datetime.today()
    d_now = "%04d-%02d-%02d" % (today.year, today.month, today.day)
    
    # register clinical_checkup in the MySQL database (bs.ctmed.ru:mis)
    s_sqlt = """INSERT INTO clinical_checkups
    (cc_id, people_id, date_created, clinic_id)
    VALUES
    ({0}, {1}, '{2}', {3})
    """
    s_sql = s_sqlt.format(cc_id, people_id, d_now, clinic_id)
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
            
            register_cc(dbmy, cc_id, people_id, clinic_id)
            
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
    

if __name__ == "__main__":
    
    import os    
    import datetime
    
    get_wlist()

#    for clinic_id in clist:
#        try:
#            mcod = modb.moCodeByMisId(clinic_id)
#            sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod) 
#            log.debug(sout)
#        except:
#            sout = "Have not got MO Code for clinic_id {0}".format(clinic_id)
#            log.warn(sout)
#            mcod = 0
#
#        pclinic(clinic_id, mcod)    

    for mcod in molist:
        try:
            mo = modb[mcod]
            clinic_id = mo.mis_code
            sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod) 
            log.debug(sout)
        except:
            sout = "Clinic with MO Code {0} was not found".format(mcod)
            log.warn(sout)
            continue
            

        pclinic(clinic_id, mcod)    


    sys.exit(0)
