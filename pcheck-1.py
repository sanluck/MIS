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

CLINIC_ID  = 106
DBF_DIR    = "/home/gnv/MIS/import/106/"
table_name = DBF_DIR + "REGISTRY.DBF"

STEP = 100

s_sqlt1 = """UPDATE peoples
SET p_payment_type_id_fk = ?
WHERE people_id = ?"""

s_sqlt2 = """UPDATE peoples
SET medical_insurance_region_id_fk = 22
WHERE people_id = ?"""

s_sqlt3 = """UPDATE peoples
SET 
insorg_id = ?,
medical_insurance_company_name = ?
WHERE people_id = ?"""

s_sqlt4 = """UPDATE peoples
SET social_status_id_fk = ?
WHERE people_id = ?"""

# territory_id_fk, addr_jure_region_code, addr_jure_area_code, addr_jure_area_name, addr_jure_town_code, addr_jure_town_name
kladr = {}
kladr[132] = [u"1101202000", 22, 2200300000000, u"Алтайский р-н", None, None] # Алтайская ЦРБ
kladr[106] = [u"1101204000", 22, 2200500000000, u"Бийский р-н", None, None] # Бийская ЦРБ
kladr[149] = [u"1101219000", 22, 2202100000000, u"Красногорский р-н", None, None] # Красногорская ЦРБ
kladr[217] = [u"1101233000", 22, 2203500000000, u"Петропавловский р-н", None, None] # Петропавловская ЦРБ
kladr[171] = [u"1101240000", 22, 2204200000000, u"Смоленский р-н", None, None] # Смоленская ЦРБ
kladr[172] = [u"1101242000", 22, 2204300000000, u"Советский р-н", None, None] # Советская ЦРБ
kladr[185] = [u"1101257000", 22, 2205800000000, u"Целинный р-н", None, None] # Целинная ЦРБ
kladr[117] = [u"1101405000", 22, None, None, 2200000400000, u"Бийск"] # Бийская ГП-6
kladr[156] = [u"1101405000", 22, None, None, 2200000400000, u"Бийск"] # Бийская ГБ-3

s_sqlt56 = """UPDATE peoples
SET 
territory_id_fk = ?,
addr_jure_region_code = ?,
addr_jure_area_code = ?, 
addr_jure_area_name = ?, 
addr_jure_town_code = ?,
addr_jure_town_name = ?
WHERE people_id = ?"""

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
        self.soato = None
        self.fio = None
    
    def initFromRec(self, rec):
        self.people_id = rec[0]
        self.lname = rec[1].strip()
        self.fname = rec[2].strip()
        if rec[3] is None:
            self.mname = rec[3]
        else:
            self.mname = rec[3].strip()
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
    
    def initFromDBF(self, rec):
        self.people_id = rec.number
        self.lname = rec.surname.strip()
        self.fname = rec.name.strip()
        if rec.patronymic is None:
            self.mname = rec.patronymic
        else:
            self.mname = rec.patronymic.strip()
        self.birthday = rec.birthday
        self.insorg_id = rec.kod_smo
        self.soato = rec.soato
        self.fio = self.lname + self.fname + self.mname
    

def get_registry():
    import dbf
    
    table = dbf.Table(table_name)
    table.open()

    p_arr = []
    for rec in table:
        p_dbf = PEOPLE()
        p_dbf.initFromDBF(rec)
        p_arr.append(p_dbf)
        
    return p_arr

def find_registry(people, p_arr):
    lname    = people.lname
    fname    = people.fname
    mname    = people.mname
    birthday = people.birthday
    u_lname  = lname.upper()
    u_fname  = fname.upper()
    if mname is None:
        u_mname  = mname
    else:
        u_mname  = mname.upper()
        
    for p_dbf in p_arr:
        if (u_lname == p_dbf.lname) and (u_fname == p_dbf.fname) and (u_mname == p_dbf.mname) and (birthday == p_dbf.birthday):
            return p_dbf
        
    return None

if __name__ == "__main__":
    import time
    import datetime
    from dbmis_connect2 import DBMIS
    from insorglist import InsorgInfoList

    insorgs = InsorgInfoList()
    
    now = datetime.datetime.now()
    s_now = "%04d-%02d-%02d" % (now.year, now.month, now.day)    
    y_now = now.year
    y_now = 2013
    
    kkk = kladr[CLINIC_ID]
    territory_id_fk       = kkk[0]
    addr_jure_region_code = kkk[1]
    addr_jure_area_code   = kkk[2]
    addr_jure_area_name   = kkk[3]
    addr_jure_town_code   = kkk[4]
    addr_jure_town_name   = kkk[5]
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('Peoples Check Start {0}'.format(localtime))

    sout = "Table Name: {0}".format(table_name)
    log.info( sout )
    
    p_arr = get_registry()
    l_p_arr = len(p_arr)
    sout = "Table has got {0} records".format(l_p_arr)
    log.info( sout )
    
    sout = "clinic_id: {0} database: {1}:{2}".format(CLINIC_ID, HOST, DB)
    log.info( sout )

    dbc = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    
    clinic_name = dbc.name
    sout = "clinic_name: {0}".format(clinic_name.encode("utf-8"))
    log.info( sout )
    
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
    r_count = len(results)

    sout = "Totally {0} records has been fetched from the peoples table".format(r_count)
    log.info( sout )

    dbcw = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    conw = dbcw.con
    curw = conw.cursor()

    counta  = 0
    count0  = 0
    countf  = 0
    for rec in results:
        
        counta += 1
        people.initFromRec(rec)
        people_id = people.people_id
        
        p_bd = people.birthday
        p_by = p_bd.year
        age  = y_now - p_by
        
        p_dbf = find_registry(people, p_arr)
        
        if p_dbf is not None:
            countf += 1
            kod_smo = p_dbf.insorg_id
            soato   = p_dbf.soato
            
            if counta % STEP == 0: 
                sout = "{0} {1} {2}".format(counta, people_id, p_dbf.people_id)
                log.info( sout )
                
        elif counta % STEP == 0: 
            kod_smo = None
            soato   = None
            sout = "{0} {1} 'None'".format(counta, people_id)
            log.info( sout )            

        # p_payment_type_id_fk
        if people.p_payment_type_id_fk is None:
            count0 += 1
            
            if (age >= 18) and (age <= 60):
                ppt = 1
            else:
                ppt = 2
            
            curw.execute(s_sqlt1,(ppt, people_id))
        
        # medical_insurance_region_id_fk
        if people.medical_insurance_region_id_fk is None:
            
            curw.execute(s_sqlt2,(people_id,))
            
        # insorg_id, medical_insurance_company_name
        if (people.insorg_id is None) and (kod_smo is not None):
            try:
                insorg = insorgs[kod_smo]
            except:
                sout = "People_id: {0}. Have not got insorg_id: {1}".format(people_id, kod_smo)
                log.debug(sout)
                insorg = insorgs[0]
                
            curw.execute(s_sqlt3,(kod_smo, insorg.name, people_id))
            
        # social_status_id_fk
        if people.social_status_id_fk is None:
            
            if (age >= 18):
                ss_id = 5
            elif (age >= 6):
                ss_id = 3
            else:
                ss_id = 2
            
            curw.execute(s_sqlt4,(ss_id, people_id))
            
        # territory_id_fk, addr_jure_region_code, addr_jure_area_code, addr_jure_area_name, addr_jure_town_code, addr_jure_town_name
        if people.territory_id_fk is None:
            curw.execute(s_sqlt56,(territory_id_fk, addr_jure_region_code, addr_jure_area_code, addr_jure_area_name, addr_jure_town_code, addr_jure_town_name, people_id))
            
        conw.commit()
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Peoples Check Finish  '+localtime)    
    sout = "Totally {0} peoples of {1} have not got payment type".format(count0, counta)
    log.info( sout )
    sout = "{0} peoples have been found in the registry.dbf".format(countf)
    log.info( sout )
    
    dbc.close()
    dbcw.close()
    sys.exit(0)