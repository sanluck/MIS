#!/usr/bin/python
# coding: utf-8
# report-2019.py - Отчет по умершим по МО (в разрезе участков)
#                  задача 2019
#

import os, sys
from datetime import datetime
import time
import random

import sys, codecs
import ConfigParser
import logging

from dbmysql_connect import DBMY

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_report_2019.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

Config = ConfigParser.ConfigParser()
#PATH = os.path.dirname(sys.argv[0])
#PATH = os.path.realpath(__file__)
PATH = os.getcwd()
FINI = PATH + "/" + "r2019.ini"

log.info("INI File: {0}".format(FINI))

from ConfigSection import ConfigSectionMap
# read INI data
Config.read(FINI)
# [MEDDOC]
Config1 = ConfigSectionMap(Config, "MEDDOC")
MD_HOST = Config1['md_host']
MD_DB = Config1['md_db']

# [MIS]
Config1 = ConfigSectionMap(Config, "MIS")
MIS_HOST = Config1['mis_host']
MIS_DB = Config1['mis_db']

# [DBMIS]
Config1 = ConfigSectionMap(Config, "DBMIS")
HOST = Config1['host']
DB = Config1['db']

# [Report]
Config2 = ConfigSectionMap(Config, "Report")
D_START = Config2['d_start']
D_FINISH = Config2['d_finish']
DATE_RANGE = [D_START,D_FINISH]

F_PATH    = "./REPORT/"
FIN_NAME  = "rep2019.xls"
FOUT_NAME = "rep2019_out.xls"

SQLT1 = """
SELECT d.id_incident, d.datetime, d.id_death_cause,
i.surname, i.name, i.patronymic, i.birthday,
adc.id_document,
cd.id_type_cause, cd.id_mkb,
mkb.code, mkb.name
FROM death d
LEFT JOIN incident i ON d.id_incident = i.id_incident
LEFT JOIN actual_death_causes adc ON d.id_incident = adc.id_incident
LEFT JOIN cause_death cd ON adc.id_document = cd.id_document
LEFT JOIN sprav_mkb mkb ON cd.id_mkb = mkb.id_mkb
WHERE d.datetime BETWEEN %s AND %s AND i.birthday is not Null;"""

SQLT2 = """SELECT
ap.area_people_id, ap.area_id_fk, ap.date_beg, ap.motive_attach_beg_id_fk,
ca.clinic_id_fk,
ar.area_number, ar.area_id,
ca.speciality_id_fk
FROM area_peoples ap
LEFT JOIN areas ar ON ap.area_id_fk = ar.area_id
LEFT JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
WHERE ap.people_id_fk = ?
AND ca.basic_speciality = 1
AND ca.speciality_id_fk IN (1,7,38,51)
AND (ap.date_end is Null OR ap.motive_attach_end_id_fk = 5)
ORDER BY ap.date_beg DESC;"""

# d_cases
SQLT3 = """INSERT INTO d_cases 
(people_id, lname, fname, mname, birthday,
clinic_id, area_id, area_number, speciality_id, 
id_incident, d_dt, id_document) 
VALUES (%s, %s, %s, %s, %s,
%s, %s, %s, %s,
%s, %s, %s);"""

# d_ds
SQLT4 = """INSERT INTO d_ds
(people_id, ds)
VALUES 
(%s, %s);"""

#
SQLT5 = """SELECT 
people_id, clinic_id, area_id, area_number 
FROM mis.d_cases 
WHERE clinic_id is not Null 
ORDER BY clinic_id, area_number;"""

class D_CASE:
    def __init__(self):
        self.people_id = None
        self.lname = None
        self.fname = None
        self.mname = None
        self.birthday = None
        self.sex      = None
        self.snils    = None
        
        self.clinic_id = None
        self.mcod      = None
        self.area_id = None
        self.area_number = None
        self.speciality_id = None
        self.worker_id = None
        self.doctor_id = None
        self.doctor_fio = None
        
        self.id_incident = None
        self.d_dt = None
        self.id_document = None
        self.ds = []

class R_ITEM:
    def __init__(self, d_ds_group = []):
        self.clinic_id = None
        self.clinic_name = None
        self.area_number = None
        self.speciality_id = None
        self.speciality_name = None
        self.doctor_fio = None
        
        self.d_count = []
        
        for ddd in d_ds_group:
            ddn = ddd[3]
            self.d_count.append(ddn)
        
    
    def save2db(self, con):
        
        cursor = con.cursor()
        
        ssql = """INSERT INTO r2019
        (clinic_id, clinic_name, area_number, 
        speciality_id, speciality_name, doctor_fio) 
        VALUES
        (%s, %s, %s, 
        %s, %s, %s);"""
        
        cursor.execute(ssql, (self.clinic_id, self.clinic_name, \
                              self.area_number, \
                              self.speciality_id, self.speciality_name, \
                              self.doctor_fio, ))
        
        con.commit()
        _id = con.insert_id()
        
        ssql = """INSERT INTO r2019_ds
        (r2019_id, d_ds_id, d_ds_number) 
        VALUES
        (%s, %s, %s);"""
        i = 0
        for ddd in self.d_count:
            i += 1
            cursor.execute(ssql, (_id, i, ddd, ))
            
        con.commit()

def get_d(d_start, d_finish):
    
    dbmy = DBMY(host = MD_HOST, db = MD_DB)
    cursor = dbmy.con.cursor()
    cursor.execute(SQLT1, (d_start, d_finish, ))
    
    results = cursor.fetchall()
    
    ddd = {}
    
    for rec in results:
        id_incident = rec[0]
        d_dt = rec[1]
        id_death_cause = rec[2]
        surname = rec[3]
        name = rec[4]
        patronymic = rec[5]
        birthday = rec[6]
        id_document = rec[7]
        id_type_cause = rec[8]
        id_mkb = rec[9]
        mkb_code = rec[10]
        mkb_name = rec[11]
        if ddd.has_key(id_incident):
            ddd[id_incident].ds.append(mkb_code)
        else:
            d = D_CASE()
            d.id_incident = id_incident

            d.lname = surname
            d.fname = name
            d.mname = patronymic
            d.birthday = birthday
            
            d.d_dt = d_dt
            d.id_document = id_document
            d.ds = [mkb_code]
            ddd[id_incident] = d
            
    dbmy.close()
    return ddd

def set_people_id(cur, d):
    from people import get_people
    
    lname = d.lname
    fname = d.fname
    mname = d.mname
    bd    = d.birthday
    precs = get_people(cur, lname, fname, mname, bd)
    
    if precs:
        people_id = precs[0][0]
        d.people_id = people_id
        return len(precs)
    else:
        return None

def set_people_area(cur, d):
    
    people_id = d.people_id
    
    if people_id is None: return None
    
    cur.execute(SQLT2, (people_id, ))
    results = cur.fetchall()
    
    if (results is None) or (len(results)==0): return None
    
    rec = results[0]
    area_people_id = rec[0]
    area_id_fk = rec[1]
    date_beg = rec[2]
    motive_attach_beg_id_fk  =rec[3]
    clinic_id = rec[4]
    area_number = rec[5]
    area_id = rec[6]
    speciality_id = rec[7]
    
    d.area_id = area_id
    d.clinic_id = clinic_id
    d.area_number = area_number
    d.speciality_id = speciality_id
    
    return len(results)

def find_peoples(ddd):
    import fdb
    from dbmis_connect2 import DBMIS
    
    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    dbc = DBMIS(mis_host = HOST, mis_db = DB)

    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = dbc.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    ro_cur = ro_transaction.cursor()
    
    nnf = 0 # number of not found peoples
    nfd = 0 # number of found duplicated peoples
    nna = 0 # number of peoples without area_number
    nda = 0 # number of peoples having more than one area number
    
    for d_key in ddd.keys():
        d = ddd[d_key]
        
        nfound = set_people_id(ro_cur, d)
        
        if nfound:
            ddd[d_key] = d
            if nfound > 1: nfd += 1
        else:
            nnf += 1
            continue
        
        nap = set_people_area(ro_cur, d)
        if nap:
            ddd[d_key] = d
            if nap > 1: nda += 1
        else:
            nna += 1

    dbc.close()
    
    sout = "Not found peoples: {0}".format(nnf)
    log.info(sout)
    
    sout = "Found duplicates: {0}".format(nfd)
    log.info(sout)
    
    sout = "Number of peoples without area_number: {0}".format(nna)
    log.info(sout)
    
    sout = "Number of peoples having more than one area number: {0}".format(nda)
    log.info(sout)
        
def save_d_dict(ddd, clear_before = True):
    dbmy = DBMY(host = MIS_HOST, db = MIS_DB)
    cursor = dbmy.con.cursor()
    
    if clear_before:
        ssql = "TRUNCATE TABLE d_cases;"
        cursor.execute(ssql)
        ssql = "TRUNCATE TABLE d_ds;"
        cursor.execute(ssql)
        dbmy.con.commit()

    for d_key in ddd.keys():
        d = ddd[d_key]
        people_id = d.people_id
        if people_id:
            lname = d.lname
            fname = d.fname
            mname = d.mname
            birthday = d.birthday
            clinic_id = d.clinic_id
            area_id = d.area_id
            area_number = d.area_number
            speciality_id = d.speciality_id
            id_incident = d.id_incident
            d_dt = d.d_dt
            id_document = d.id_document
            
            try:
                cursor.execute(SQLT3, (people_id, lname, fname, mname, birthday, \
                                       clinic_id, area_id, area_number, speciality_id, \
                                       id_incident, d_dt, id_document, )) 

                for ds in d.ds:
                    cursor.execute(SQLT4, (people_id, ds,))
   
                dbmy.con.commit()
            except Exception, e:
                sout = "Inserting into d_cases table Error:"
                log.warn(sout)
                sout = "{0}".format(e)
                log.warn(sout)

def stage1():
# stage 1: select data from meddoc and identify people_id
    
    sout = "MEDDOC Database: {0}:{1}".format(MD_HOST, MD_DB)
    log.info(sout)

    d_start = datetime.strptime(D_START, "%Y-%m-%d")
    sout = "d_start: {0}".format(d_start)
    log.info(sout)

    d_finish = datetime.strptime(D_FINISH, "%Y-%m-%d")
    sout = "d_finish: {0}".format(d_finish)
    log.info(sout)
    
    d_dict = get_d(d_start, d_finish)
    
    ld = len(d_dict)
    sout = "Totally we have got {0} incidents".format(ld)
    log.info(sout)
    
    find_peoples(d_dict)

    sout = "MIS Database: {0}:{1}".format(MIS_HOST, MIS_DB)
    log.info(sout)
    
    save_d_dict(d_dict, clear_before = True)
    

def stage2():
# stage 2: use selected data from meddoc (stage1)
#          write report data
    from clinic_areas_doctors import get_cad
    
    log.info("Stage II")
    
    sout = "MIS Database: {0}:{1}".format(MIS_HOST, MIS_DB)
    log.info(sout)

    dbmy = DBMY(host = MIS_HOST, db = MIS_DB)
    cursor = dbmy.con.cursor()

    d_ds_group = []
    ssql = """SELECT type, ds1, ds2 FROM d_ds_groups;"""
    cursor.execute(ssql)
    results = cursor.fetchall()
    for rec in results:
        ds_type = rec[0]
        ds1 = rec[1]
        ds2 = rec[2]
        d_ds_group.append([ds_type, ds1, ds2, 0])
    
    cursor.execute(SQLT5)
    
    results = cursor.fetchall()
    
    c_id = 0
    a_id = 0
    p_id = 0
    
    r_item = R_ITEM(d_ds_group)
    
    for rec in results:
        people_id = rec[0]
        clinic_id = rec[1]
        area_id = rec[2]
        area_number = rec[3]
        if c_id != clinic_id:
            
            if c_id != 0: r_item.save2db(dbmy.con)
            c_id = clinic_id
            a_id = area_id
            r_item = R_ITEM(d_ds_group)
        elif a_id != area_id:
            if a_id != 0: r_item.save2db(dbmy.con)
            a_id = area_id
            r_item = R_ITEM(d_ds_group)
        
            
if __name__ == "__main__":

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('-----------------------------------------------------------------------------------')
    log.info('Report 2019 Start {0}'.format(localtime))

    stage1()
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Report 2019 Finish {0}'.format(localtime))
    sys.exit(0)
