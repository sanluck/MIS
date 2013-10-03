#!/usr/bin/python
# -*- coding: utf-8 -*-
# insr-2.py - обработка ответа на запрос страховой пренадлежности
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insr2.out'
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

def pclinic(clinic_id, mcod):
    from dbmis_connect2 import DBMIS
    from PatientInfo2 import PatientInfo2
    from insorglist import InsorgInfoList
    
    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('Insurance Belongings Results Processing Start {0}'.format(localtime))
    
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

    not_belongs_2_clinic = 0
    wrong_insorg = 0
    ncount = 0
    dbc2 = DBMIS(clinic_id)
    cur2 = dbc2.con.cursor()
    
    for prec in ppp:
        ncount += 1
        people_id = prec[0]
        insorg_mcod = prec[2]
        if insorg_mcod == "":
            insorg_id = 0
        else:
            insorg_id = int(insorg_mcod) - 22000
        p_obj.initFromDb(dbc, people_id)
        if clinic_id <> p_obj.clinic_id:
            not_belongs_2_clinic += 1
            if SET_MO:
                area_people_id = p_obj.area_people_id
                s_sqlt = "UPDATE area_peoples SET area_id_fk = {0} WHERE area_people_id = {1};"
                s_sql  = s_sqlt.format(area_id, area_people_id)
                cur2.execute(s_sql)
                dbc2.con.commit()
        if (insorg_id <> 0) and (insorg_id <> p_obj.insorg_id):
            wrong_insorg += 1
            if SET_INSORG:
                s_sqlt = "UPDATE peoples SET insorg_id = {0} WHERE people_id = {1};"
                s_sql  = s_sqlt.format(insorg_id, people_id)
                cur2.execute(s_sql)
                dbc2.con.commit()
                
        if ncount % STEP == 0:
            sout = " {0} people_id: {1} clinic_id: {2}".format(ncount, people_id, p_obj.clinic_id)
            log.info(sout)

    sout = "Wrong clinic: {0}".format(not_belongs_2_clinic)
    log.info(sout)
    sout = "Wrong insorg: {0}".format(wrong_insorg)
    log.info(sout)
    
    dbc.close()
    dbc2.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Insurance Belongings Results Processing Finish  '+localtime)
    

if __name__ == "__main__":
    
    import os    
    import datetime

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
