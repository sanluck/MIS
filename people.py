#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import logging

log = logging.getLogger(__name__)

SQLT_PEOPLE = """SELECT DISTINCT p.people_id,
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

SQLT_FPEOPLE = """SELECT FIRST 20
    PEOPLE_ID,
    LNAME,
    FNAME,
    MNAME,
    LNAME ||' '|| FNAME ||' '|| coalesce(' '||MNAME, '') As FIO,
    BIRTHDAY,
    SEX,
    DOCUMENT_TYPE_ID_FK,
    DOCUMENT_SERIES,
    DOCUMENT_NUMBER,
    INSURANCE_CERTIFICATE,
    MEDICAL_INSURANCE_COMPANY_NAME,
    MEDICAL_INSURANCE_SERIES,
    MEDICAL_INSURANCE_NUMBER,
    BIRTHPLACE,
    insorg_id
FROM
    PEOPLES
Where PEOPLE_ID>0
AND (Upper(lname) like Upper('{0}')||'%')
and (Upper(fname) like Upper('{1}')||'%')
and (Upper(mname) like Upper('{2}')||'%')
and birthday = '{3}'
Order by LNAME, FNAME, MNAME;"""

SQLT_FPEOPLE0 = """SELECT FIRST 20
    PEOPLE_ID,
    LNAME,
    FNAME,
    MNAME,
    LNAME ||' '|| FNAME ||' '|| coalesce(' '||MNAME, '') As FIO,
    BIRTHDAY,
    SEX,
    DOCUMENT_TYPE_ID_FK,
    DOCUMENT_SERIES,
    DOCUMENT_NUMBER,
    INSURANCE_CERTIFICATE,
    MEDICAL_INSURANCE_COMPANY_NAME,
    MEDICAL_INSURANCE_SERIES,
    MEDICAL_INSURANCE_NUMBER,
    BIRTHPLACE,
    insorg_id
FROM
    PEOPLES
Where PEOPLE_ID>0
AND (Upper(lname) like Upper('{0}')||'%')
and (Upper(fname) like Upper('{1}')||'%')
and birthday = '{2}'
Order by LNAME, FNAME;"""

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
        
        if rec[12] is None:
            self.addr_jure_area_name = rec[12]
        else:
            self.addr_jure_area_name = rec[12].strip()
            
        self.addr_jure_town_code = rec[13]
        if rec[14] is None:
            self.addr_jure_town_name = rec[14]
        else:
            self.addr_jure_town_name = rec[14].strip()
    
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
    
def get_registry(table_name):
    import dbf
    
    table = dbf.Table(table_name)
    table.open()

    p_arr = []
    for rec in table:
        p_dbf = PEOPLE()
        p_dbf.initFromDBF(rec)
        p_arr.append(p_dbf)
        
    table.close()
    return p_arr

def get_people(cursor, lname, fname, mname, birthday):
    
    lname1251 = lname.encode('cp1251')
    fname1251 = fname.encode('cp1251')
    s_birthday = "%04d-%02d-%02d" % (birthday.year, birthday.month, birthday.day)
    if mname is None:
        s_sql = SQLT_FPEOPLE0.format(lname1251, fname1251, s_birthday)
    else:
        mname1251 = mname.encode('cp1251')
        s_sql = SQLT_FPEOPLE.format(lname1251, fname1251, mname1251, s_birthday)

    try:
        cursor.execute(s_sql)
        results = cursor.fetchall()
        return results
    except Exception, e:
        r_msg = 'Ошибка запроса данных из DBMIS: {0} {1}'.format(sys.stderr, e)
        log.error( r_msg )
        return None
    