#!/usr/bin/python
# -*- coding: utf-8 -*-
# dvn-f1.py - формирование карт ДВН
#             по списку из ответа на запрос страховой принадлежности
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_dvnf1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

#clist = (101, 105, 110, 119, 121, 125, 133, 140, 141, 142, 146, 147, 148, 150, 152, 161, 163, 165, 167, 169, 170, 174, 175, 176, 178, 181, 182, 186)
clist = [224]

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
    

def pclinic(clinic_id, mcod):
    from dbmis_connect2 import DBMIS
    from PatientInfo2 import PatientInfo2
    from insorglist import InsorgInfoList
    
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
    insorgs = InsorgInfoList()

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
        if clinic_id <> p_obj.clinic_id:
            wrong_clinic += 1
            continue

        if check_person(dbc, people_id):
            dvn_number += 1

        if ncount % STEP == 0:
            sout = " {0} people_id: {1} clinic_id: {2} dvn_number: {3}".format(ncount, people_id, p_obj.clinic_id, dvn_number)
            log.info(sout)

    sout = "Wrong clinic: {0}".format(wrong_clinic)
    log.info(sout)
    sout = "DVN cases number: {0}".format(dvn_number)
    log.info(sout)
    
    
    dbc.close()
    dbc2.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('DVN List Processing Finish  '+localtime)
    

if __name__ == "__main__":
    
    import os    
    import datetime
    
    get_wlist()

    for clinic_id in clist:
        try:
            mcod = modb.moCodeByMisId(clinic_id)
            sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod) 
            log.debug(sout)
        except:
            sout = "Have not got MO Code for clinic_id {0}".format(clinic_id)
            log.warn(sout)
            mcod = 0

        pclinic(clinic_id, mcod)    

    sys.exit(0)
