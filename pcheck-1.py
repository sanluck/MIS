#!/usr/bin/python
# -*- coding: utf-8 -*-
# pcheck-1.py - проверка записей в таблице peoples
#               для пациентов заданной клиники
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_pcheck1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

CLINIC_ID = 106

class PEOPLE:
    def __init__(self):
        self.people_id = None
        self.lname = None
        self.fname = None
        self.mname = None
        self.birthday = None
        self.p_payment_type_id_fk = None
        self.medical_insurance_region_id_fk = None
        self.insorg_id = None
        self.social_status_id_fk = None
        self.territory_id_fk = None
        self.addr_jure_region_code = None
        self.addr_jure_area_code = None
        self.addr_jure_area_name = None
        self.addr_jure_town_code = None
        self.addr_jure_town_name = None
    
    def initFromRec(self, rec):
        self.people_id = rec[0]
        self.lname = rec[1]
        self.fname = rec[2]
        self.mname = rec[3]
        self.birthday = rec[4]
        self.p_payment_type_id_fk = rec[5]
        self.medical_insurance_region_id_fk = rec[6]
        self.insorg_id = rec[7]
        self.social_status_id_fk = rec[8]
        self.territory_id_fk = rec[9]
        self.addr_jure_region_code = rec[10]
        self.addr_jure_area_code = rec[11]
        self.addr_jure_area_name = rec[12]
        self.addr_jure_town_code = rec[13]
        self.addr_jure_town_name = rec[14]

if __name__ == "__main__":
    import time
    import datetime
    from dbmis_connect2 import DBMIS

    now = datetime.datetime.now()
    s_now = "%04d-%02d-%02d" % (now.year, now.month, now.day)    
    y_now = now.year
    y_now = 2013
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('Peoples Check Start {0}'.format(localtime))
    sout = "clinic_id: {0} database: {1}:{2}".format(CLINIC_ID, HOST, DB)
    log.info( sout )

    dbc = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    people = PEOPLE()
    
    s_sqlt = """SELECT DISTINCT p.people_id,
p.lname, p.fname, p.mname, p.birthday,
p.p_payment_type_id_fk, p.medical_insurance_region_id_fk, p.insorg_id,
p.social_status_id_fk, p.territory_id_fk,
p.addr_jure_region_code, p.addr_jure_area_code, p.addr_jure_area_name,
p.addr_jure_town_code, p.addr_jure_town_name
FROM peoples p
JOIN area_peoples ap ON p.people_id = ap.people_id_fk
JOIN areas ar ON ap.area_id_fk = ar.area_id
JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
WHERE ca.clinic_id_fk = {0} AND ca.basic_speciality = 1
AND ap.date_end is Null;"""
    s_sql = s_sqlt.format(CLINIC_ID)
    
    cursor = dbc.con.cursor()
    cursor.execute(s_sql)
    results = cursor.fetchall()

    counta  = 0
    count0  = 0
    for rec in results:
        
        counta += 1
        people.initFromRec(rec)
        
        if people.p_payment_type_id_fk is None: count0 += 1

        p_bd = people.birthday
        p_by = p_bd.year
        age  = y_now - p_by
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Peoples Check Finish  '+localtime)    
    sout = "Totally {0} peoples of {1} have not got payment type".format(count0, counta)
    log.info( sout )
    dbc.close()
    sys.exit(0)