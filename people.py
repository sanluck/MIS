#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import logging

STEP = 1000
PRINT_FOUND = False
SM2DO_PATH  = "./SM2DO"

log = logging.getLogger(__name__)

SQLT_PEOPLE = """SELECT DISTINCT p.people_id,
p.lname, p.fname, p.mname, p.birthday,
p.p_payment_type_id_fk, p.medical_insurance_region_id_fk, p.insorg_id,
p.social_status_id_fk, p.territory_id_fk,
p.addr_jure_region_code, p.addr_jure_area_code, p.addr_jure_area_name,
p.addr_jure_town_code, p.addr_jure_town_name,
p.birthplace,
p.document_type_id_fk, p.document_series, p.document_number,
p.citizenship
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
    SEX
FROM VW_PEOPLES_SMALL_EXT
WHERE 
upper(LNAME) starting '{0}'
AND upper(FNAME) starting '{1}' 
AND upper(MNAME) starting '{2}'
AND BIRTHDAY='{3}';"""

SQLT_FPEOPLE0 = """SELECT FIRST 20
    PEOPLE_ID,
    LNAME,
    FNAME,
    MNAME,
    LNAME ||' '|| FNAME ||' '|| coalesce(' '||MNAME, '') As FIO,
    BIRTHDAY,
    SEX
    FROM VW_PEOPLES_SMALL_EXT
    WHERE 
    upper(LNAME) starting '{0}'
    AND upper(FNAME) starting '{1}' 
    AND BIRTHDAY='{2}';"""

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
        self.birthplace = None
        self.document_type_id_fk = None
        self.document_series = None
        self.document_number = None
        self.citizenship = None
        
    
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

        if rec[15] is None:
            self.birthplace = rec[15]
        else:
            self.birthplace = rec[15].strip()

        self.document_type_id_fk = rec[16]
        if rec[17] is None:
            self.document_series = None
        else:
            self.document_series = rec[17].strip()
        self.document_number = rec[18]

        self.citizenship = rec[19]

    
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
        if rec.mrod is None:
            self.birthplace = None
        else:
            self.birthplace = rec.mrod.strip()
        
        self.document_type_id_fk = rec.c_doc
        if rec.s_doc is None:
            self.document_series = None
        else:
            self.document_series = rec.s_doc.strip()
            
        self.document_number = rec.n_doc
        

class SM_PEOPLE:
    def __init__(self):
        self.people_id = None
        self.lname = None
        self.fname = None
        self.mname = None
        self.birthday         = None
	self.sex              = None
        self.document_type_id = None
        self.document_series  = None
        self.document_number  = None
	self.snils            = None
	self.smo_ogrn         = None
	self.ocato            = None
	self.enp              = None
	
	self.dpfs             = None
	self.s_oms            = None
	self.n_oms            = None
	
	self.mcod             = None
    
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

def get_patients(db, clinic_id):
#
# get patients (people's list) for the clinic
#
    s_sql = SQLT_PEOPLE.format(clinic_id)
    
    
    try:
        cursor = db.con.cursor()
        cursor.execute(s_sql)
        results = cursor.fetchall()
    except Exception, e:
        r_msg = 'Ошибка запроса данных из DBMIS: {0} {1}'.format(sys.stderr, e)
        log.error( r_msg )
        results = None
    
    return results
    
def get_fnames(path = SM2DO_PATH, file_ext = '.csv'):
    
    import os    
    
    fnames = []
    for subdir, dirs, files in os.walk(path):
        for fname in files:
            if fname.find(file_ext) > 1:
                log.info(fname)
                fnames.append(fname)
    
    return fnames    

def d_series(document_series):
    
    if (document_series is not None) and (document_series.find('I') >= 0):
	a_ar = document_series.split('I')
	sss  = ''.join(a_ar)
	if len(sss) > 2: sss = sss[:2] + " " + sss[2:]
	
	return sss
    else:
	return document_series

def d_number(document_number):
    
    if (document_number is not None) and (document_number.find('I') >= 0):
	a_ar = document_number.split('I')
	n = len(a_ar)
	b_ar = []
	i = 0
	while i < n:
	    a = a_ar[i]
	    i += 1
	    if a == '':
		if (i < n) and (a_ar[i] == ''):
		    i += 2
		    b_ar.append('I')
	    else:
		b_ar.append(a)
		
	sss  = ''.join(b_ar)
	return sss
    else:
	return document_number

def get_sm(fname, mcod = None):
    from datetime import datetime
    
    ins = open( fname, "r" )

    array = []
    for line in ins:
	u_line = line.decode('cp1251')
	a_line = u_line.split("|")
	people_id  = int(a_line[0])
	lname = a_line[1]
	fname = a_line[2]
	mname = a_line[3]
	s_bd  = a_line[4]
	
	try:
	    bd = datetime.strptime(s_bd, '%Y-%m-%d')
	except:
	    bd = None
	
	sex   = int(a_line[5])
	
	doc_type_id     = a_line[6]
	document_series = a_line[7]
	document_number = a_line[8]
	snils           = a_line[9]
	
	smo_ogrn        = a_line[10]
	ocato           = a_line[11]
	enp             = a_line[12]
	
	dpfs            = a_line[13]
	s_oms           = a_line[14]
	n_oms           = a_line[15]
	
	sm_p = SM_PEOPLE()
	
	sm_p.people_id = people_id
	sm_p.lname = lname
	sm_p.fname = fname
	sm_p.mname = mname
	sm_p.birthday         = bd
	sm_p.sex              = sex
	sm_p.document_type_id = doc_type_id

	if doc_type_id == '14':
	    sm_p.document_series  = d_series(document_series)
	else:
	    sm_p.document_series  = d_number(document_series)
	sm_p.document_number  = d_number(document_number)
	sm_p.snils            = snils
	sm_p.smo_ogrn         = smo_ogrn
	sm_p.ocato            = ocato
	sm_p.enp              = enp
	
	if len(dpfs) == 0:
	    sm_p.dpfs = None
	else:
	    sm_p.dpfs         = dpfs
	sm_p.s_oms            = s_oms
	sm_p.n_oms            = n_oms
	
	sm_p.mcod             = mcod
	
	array.append( sm_p )
    
    ins.close()    
    
    return array

def put_sm2mira(db, ar_sm, upd = False):
    
    s_sqlf = """SELECT oms_series, oms_number, enp, mcod
    FROM
    mira$peoples
    WHERE people_id = %s"""

    s_sqli = """INSERT INTO
    mira$peoples
    (people_id, lname, fname, mname, birthday, sex,
    document_type_id_fk, document_series, document_number,
    snils,
    dpfs, oms_series, oms_number, enp,
    ocato, mcod, smo_ogrn)
    VALUES 
    (%s, %s, %s, %s, %s, %s,
    %s, %s, %s,
    %s,
    %s, %s, %s, %s,
    %s, %s, %s);"""


    s_sqlu = """UPDATE
    mira$peoples
    SET
    lname = %s,
    fname = %s,
    mname = %s,
    birthday = %s,
    sex = %s,
    document_type_id_fk = %s,
    document_series = %s,
    document_number = %s,
    snils = %s,
    dpfs = %s, 
    oms_series = %s, 
    oms_number = %s,
    enp = %s?,
    ocato = %s,
    mcod = %s,
    smo_ogrn = %s
    WHERE 
    people_id = %s;"""

    
    curr = db.con.cursor()
    curw = db.con.cursor()
    count_a = 0
    count_i = 0
    count_u = 0
    
    for sm in ar_sm:
	count_a += 1

	people_id        = sm.people_id
	lname            = sm.lname
	fname            = sm.fname
	mname            = sm.mname
	birthday         = sm.birthday
	sex              = sm.sex
	document_type_id = sm.document_type_id
	document_series  = sm.document_series
	document_number  = sm.document_number
	snils            = sm.snils
	smo_ogrn         = sm.smo_ogrn
	ocato            = sm.ocato
	enp              = sm.enp
	
	dpfs             = sm.dpfs
	oms_series       = sm.s_oms
	oms_number       = sm.n_oms
	
	mcod             = sm.mcod

	if count_a % STEP == 0:
	    sout = " {0} people_id: {1} enp: {2} mcod: {3}".format(count_a, people_id, enp, mcod)
	    log.info(sout)
	
	curr.execute(s_sqlf,(people_id,))
	rec = curr.fetchone()

	if rec is None:
	    try:
		curw.execute(s_sqli,(people_id, lname, fname, mname, birthday, sex, document_type_id, document_series, document_number, snils, dpfs, oms_series, oms_number, enp, ocato, mcod, smo_ogrn))
		db.con.commit()	
		count_i += 1
	    except Exception, e:
		sout = "Can't insert into mira$peoples table. UID: {0}".format(people_id)
		log.error(sout)
		sout = "{0}".format(e)
		log.error(sout)
	else:
	    if upd:
		try:
		    curw.execute(s_sqlu,(lname, fname, mname, birthday, sex, document_type_id, document_series, document_number, snils, dpfs, oms_series, oms_number, enp, ocato, mcod, amo_ogrn, people_id,))
		    db.con.commit()	
		    count_u += 1
		except Exception, e:
		    sout = "Can't update mira$peoples table. UID: {0}".format(people_id)
		    log.error(sout)
		    sout = "{0}".format(e)
		    log.error(sout)
	    if PRINT_FOUND:
		f_oms_series = rec[0]
		f_oms_number = rec[1]
		f_enp        = rec[2]
		f_mcod       = rec[3]
		
		sout = "Found in mira$peoples: {0} enp: {1} | {2} mcod: {3} | {4} ".format(people_id, enp, f_enp, mcod, f_mcod)
		log.info(sout)
		
	    
    return count_a, count_i, count_u
