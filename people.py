#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import logging

STEP = 1000
PRINT_FOUND = True
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

SQLT_PMIRA0 = """SELECT
people_id, lname, fname, mname, birthday, sex,
document_type_id_fk, document_series, document_number,
snils, dpfs, oms_series, oms_number, enp,
ocato, mcod, smo_ogrn
FROM mira$peoples
WHERE 
mcod = {0}
AND id_done is Null;"""

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

class ST_PEOPLE:
    def __init__(self):
        self.people_id  = None

	self.ocato      = None
	self.smo_code   = None
	self.dpfs       = None
	self.oms_series = None
	self.oms_number = None
	self.enp        = None
	self.mcod       = None

class MO_PEOPLE:
    def __init__(self):
        self.people_id   = None

	self.dpfs        = None
	self.oms_sn      = None
	self.enp         = None
	self.lname       = None
	self.fname       = None
	self.mname       = None
	self.birthday    = None
	self.birthplace  = None
	self.doc_type_id = None
	self.doc_sn      = None
	self.doc_when    = None
	self.doc_who     = None
	self.snils       = None
	self.mcod        = None
	self.motive_att  = None
	self.type_att    = None
	self.date_att    = None
	self.date_det    = None

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
    
    lname1251 = lname.upper().encode('cp1251')
    fname1251 = fname.upper().encode('cp1251')
    s_birthday = "%04d-%02d-%02d" % (birthday.year, birthday.month, birthday.day)
    if mname is None:
        s_sql = SQLT_FPEOPLE0.format(lname1251, fname1251, s_birthday)
    else:
        mname1251 = mname.upper().encode('cp1251')
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
	if len(a_line) < 15:
	    sout = "Wrang line: {0}".format(u_line.encode('utf-8'))
	    log.warn( sout )
	    continue
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
    enp = %s,
    ocato = %s,
    mcod = %s,
    smo_ogrn = %s,
    id_done = Null
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
		    curw.execute(s_sqlu,(lname, fname, mname, birthday, sex, document_type_id, document_series, document_number, snils, dpfs, oms_series, oms_number, enp, ocato, mcod, smo_ogrn, people_id,))
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

def get_mira_peoples(db, mcod):
#
# get patients (people's list) for the clinic
#

    
    s_sql = SQLT_PMIRA0.format(mcod)
    
    
    try:
        cursor = db.con.cursor()
        cursor.execute(s_sql)
        results = cursor.fetchall()
    except Exception, e:
        r_msg = 'Ошибка запроса данных из DBMYSQL: {0} {1}'.format(sys.stderr, e)
        log.error( r_msg )
        return None
    
    ar_sm = []
    for rec in results:
	people_id = rec[0]
	lname = rec[1]
	fname = rec[2]
	mname = rec[3]
	birthday = rec[4]
	sex = rec[5]
	document_type_id = rec[6]
	document_series = rec[7]
	document_number = rec[8]
	snils = rec[9]
	dpfs = rec[10]
	oms_series = rec[11]
	oms_number = rec[12]
	enp = rec[13]
	ocato = rec[14]
	mcod = rec[15]
	smo_ogrn	 = rec[16]
	
	sm_p = SM_PEOPLE()

	sm_p.people_id        = people_id
	sm_p.lname            = lname
	sm_p.fname            = fname
	sm_p.mname            = mname
	sm_p.birthday         = birthday
	sm_p.sex              = sex
	sm_p.document_type_id = document_type_id
	sm_p.document_series  = document_series
	sm_p.document_number  = document_number
	sm_p.snils            = snils
	sm_p.smo_ogrn         = smo_ogrn
	sm_p.ocato            = ocato
	sm_p.enp              = enp
	
	sm_p.dpfs             = dpfs
	sm_p.s_oms            = oms_series
	sm_p.n_oms            = oms_number
	
	sm_p.mcod             = mcod
	
	ar_sm.append(sm_p)
	
    return ar_sm

def get_st(fname, mcod = None):
    from datetime import datetime
    
    ins = open( fname, "r" )

    array = []
    for line in ins:
	u_line = line.decode('cp1251')
	a_line = u_line.split("|")
	if len(a_line) < 8:
	    sout = "Wrang line: {0}".format(u_line.encode('utf-8'))
	    log.warn( sout )
	    continue

	people_id  = int(a_line[0])
	ocato      = a_line[1]
	s_code     = a_line[2]
	if len(s_code) == 0:
	    smo_code = None
	else:
	    smo_code = int(s_code)
	    
	dpfs       = a_line[3]
	oms_series = a_line[4]
	oms_number = a_line[5]
	enp        = a_line[6]
	mcod       = a_line[7]

	st_p = ST_PEOPLE()
	
	st_p.people_id = people_id
	
	st_p.ocato     = ocato
	st_p.smo_code  = smo_code

	if len(dpfs) == 0:
	    st_p.dpfs = None
	else:
	    st_p.dpfs = int(dpfs)
	    
	st_p.oms_series = oms_series
	st_p.oms_number = oms_number
	st_p.enp        = enp 

	if (len(mcod) == 0) or (mcod[0] not in (u'0', u'1', u'2', u'3', u'4', u'5', u'6', u'7', u'8', u'9')):
	    st_p.mcod = None
	else:
	    st_p.mcod = int(mcod)
	
	array.append( st_p )
    
    ins.close()    
    
    return array

def put_st2mira(db, ar_st, append = False):
    
    s_sqlf = """SELECT f$smo_code, f$oms_series, f$oms_number, f$enp, f$mcod
    FROM
    mira$peoples
    WHERE people_id = %s"""

    s_sqli = """INSERT INTO
    mira$peoples
    (people_id, 
    f$ocato, f$smo_code,
    f$dpfs, f$oms_series, f$oms_number, f$enp,
    f$mcod)
    VALUES 
    (%s, 
    %s, %s, 
    %s, %s, %s, %s,
    %s);"""


    s_sqlu = """UPDATE
    mira$peoples
    SET
    f$ocato = %s,
    f$smo_code = %s,
    f$dpfs = %s,
    f$oms_series = %s,
    f$oms_number = %s,
    f$enp = %s,
    f$mcod = %s
    WHERE 
    people_id = %s;"""

    
    curr = db.con.cursor()
    curw = db.con.cursor()
    count_a = 0
    count_i = 0
    count_u = 0
    
    for st in ar_st:
	count_a += 1

	people_id  = st.people_id

	ocato      = st.ocato
	smo_code   = st.smo_code
	dpfs       = st.dpfs
	oms_series = st.oms_series
	oms_number = st.oms_number
	enp        = st.enp
	mcod       = st.mcod

	if count_a % STEP == 0:
	    sout = " {0} people_id: {1} enp: {2} mcod: {3}".format(count_a, people_id, enp, mcod)
	    log.info(sout)
	
	curr.execute(s_sqlf,(people_id,))
	rec = curr.fetchone()

	if (rec is None) and append:
	    try:
		curw.execute(s_sqli,(people_id, ocato, smo_code, dpfs, oms_series, oms_number, enp, mcod))
		db.con.commit()	
		count_i += 1
	    except Exception, e:
		sout = "Can't insert into mira$peoples table. UID: {0}".format(people_id)
		log.error(sout)
		sout = "{0}".format(e)
		log.error(sout)
	else:
	    try:
		curw.execute(s_sqlu,(ocato, smo_code, dpfs, oms_series, oms_number, enp, mcod, people_id,))
		db.con.commit()	
		count_u += 1
	    except Exception, e:
		sout = "Can't update mira$peoples table. UID: {0}".format(people_id)
		log.error(sout)
		sout = "{0}".format(e)
		log.error(sout)
	    
    return count_a, count_i, count_u

def mo_item(s_mo_item, itype = 'S' ):
    #
    # itype:
    #       S - String
    #       D - Date
    #       I - Integer
    #
    
    from datetime import date
    
    ls = len(s_mo_item)
    if (ls == 0) or (s_mo_item == u'\r\n') or (s_mo_item == u'"NONE"'):
	return None
    else:
	sss = s_mo_item[1:ls-1]
	if itype == 'S':
	    return sss
	elif itype == 'D':
	    year  = int(sss[:4])
	    month = int(sss[4:6])
	    day   = int(sss[6:])
	    ddd = date(year, month, day)
	    return ddd
	elif itype == 'I':
	    iii = int(sss)
	    return iii
	else:
	    return sss

def get_mo(fname, mcod = None):
    from datetime import datetime
    
    ins = open( fname, "r" )

    array = []
    for line in ins:
	u_line = line.decode('cp1251')
	a_line = u_line.split(";")
	if len(a_line) < 18:
	    sout = "Wrang line: {0}".format(u_line.encode('utf-8'))
	    log.warn( sout )
	    continue
	
	p_mo = MO_PEOPLE()
	
	p_mo.dpfs        = mo_item(a_line[0])
	p_mo.oms_sn      = mo_item(a_line[1])
	p_mo.enp         = mo_item(a_line[2])
	p_mo.lname       = mo_item(a_line[3])
	p_mo.fname       = mo_item(a_line[4])
	p_mo.mname       = mo_item(a_line[5])
	p_mo.birthday    = mo_item(a_line[6],'D')
	p_mo.birthplace  = mo_item(a_line[7])
	p_mo.doc_type_id = mo_item(a_line[8],'I')
	p_mo.doc_sn      = mo_item(a_line[9])
	p_mo.doc_when    = mo_item(a_line[10],'D')
	p_mo.doc_who     = mo_item(a_line[11])
	p_mo.snils       = mo_item(a_line[12])
	p_mo.mcod        = mo_item(a_line[13],'I')
	p_mo.motive_att  = mo_item(a_line[14],'I')
	p_mo.type_att    = mo_item(a_line[15])
	p_mo.date_att    = mo_item(a_line[16],'D')
	p_mo.date_det    = mo_item(a_line[17],'D')
	
	array.append( p_mo )
    
    ins.close()    
    
    return array

def put_mo(db, ar, upd = False):
    
    s_sqlf = """SELECT id
    FROM
    mo
    WHERE oms_sn = %s
    AND enp = %s
    AND mcod = %s"""

    s_sqlf_enp = """SELECT id
    FROM
    mo
    WHERE enp = %s;"""

    s_sqlf_oms_sn = """SELECT id
    FROM
    mo
    WHERE oms_sn = %s;"""

    s_sqlf_fio_dr = """SELECT id
    FROM
    mo
    WHERE lname = %s
    AND fname = %s
    AND mname = %s
    AND birthday = %s;"""

    s_sqli = """INSERT INTO
    mo
    (dpfs, oms_sn, enp, 
    lname, fname, mname,
    birthday, birthplace,
    doc_type_id, doc_sn, doc_when, doc_who,
    snils, mcod,
    motive_att, type_att, date_att, date_det)
    VALUES 
    (%s, %s, %s,
    %s, %s, %s,
    %s, %s,
    %s, %s, %s, %s,
    %s, %s,
    %s, %s, %s, %s);"""


    s_sqlu = """UPDATE
    mo
    SET
    dpfs = %s,
    oms_sn = %s,
    enp = %s, 
    lname = %s,
    fname = %s,
    mname = %s,
    birthday = %s,
    birthplace = %s,
    doc_type_id = %s,
    doc_sn = %s,
    doc_when = %s, 
    doc_who = %s,
    snils = %s,
    mcod = %s,
    motive_att = %s,
    type_att = %s,
    date_att = %s,
    date_det = %s
    WHERE 
    id = %s;"""

    
    curr = db.con.cursor()
    curw = db.con.cursor()
    count_a = 0
    count_i = 0
    count_u = 0
    
    for p_mo in ar:
	count_a += 1

	dpfs        = p_mo.dpfs
	oms_sn      = p_mo.oms_sn
	enp         = p_mo.enp
	lname       = p_mo.lname
	fname       = p_mo.fname
	mname       = p_mo.mname
	birthday    = p_mo.birthday
	birthplace  = p_mo.birthplace
	doc_type_id = p_mo.doc_type_id
	doc_sn      = p_mo.doc_sn
	doc_when    = p_mo.doc_when
	doc_who     = p_mo.doc_who
	snils       = p_mo.snils
	mcod        = p_mo.mcod
	motive_att  = p_mo.motive_att
	type_att    = p_mo.type_att
	date_att    = p_mo.date_att
	date_det    = p_mo.date_det

	if oms_sn is None:
	    s_oms_sn = ''
	else:
	    s_oms_sn = oms_sn.encode('utf-8')
	if enp is None:
	    s_enp = ''
	else:
	    s_enp = enp.encode('utf-8')

	
	if count_a % STEP == 0:
	    sout = " {0} oms_sn: {1} enp: {2} mcod: {3}".format(count_a, s_oms_sn, s_enp, mcod)
	    log.info(sout)
	
	if enp is not None:
	    curr.execute(s_sqlf_enp,(enp,))
	elif oms_sn is not None:
	    curr.execute(s_sqlf_oms_sn,(oms_sn,))
	else:
	    curr.execute(s_sqlf_fio_dr,(lname, fname, mname, birthday,))
	rec = curr.fetchone()
	if rec is None:
	    try:
		curw.execute(s_sqli,(dpfs, oms_sn, enp, lname, fname, mname, birthday, birthplace, doc_type_id, doc_sn, doc_when, doc_who, snils, mcod, motive_att, type_att, date_att, date_det,))
		db.con.commit()	
		count_i += 1
	    except Exception, e:
		sout = "Can't insert into mo table. oms_sn: {0} enp: {1}".format(s_oms_sn, s_enp)
		log.error(sout)
		sout = "{0}".format(e)
		log.error(sout)
	else:
	    if upd:
		_id = rec[0]
		try:
		    curw.execute(s_sqlu,(dpfs, oms_sn, enp, lname, fname, mname, birthday, birthplace, doc_type_id, doc_sn, doc_when, doc_who, snils, mcod, motive_att, type_att, date_att, date_det, _id,))
		    db.con.commit()	
		    count_u += 1
		except Exception, e:
		    sout = "Can't update mo table. oms_sn: {0} enp: {1}".format(s_oms_sn, s_enp)
		    log.error(sout)
		    sout = "{0}".format(e)
		    log.error(sout)
		    
	    if PRINT_FOUND:
		sout = "Found in mo: oms_sn: {0} enp: {1} mcod: {2}".format(s_oms_sn, s_enp, mcod)
		log.info(sout)
		    
    return count_a, count_i, count_u

def get_mo_fromdb(db, mcod):
    from datetime import datetime

    s_sqlt = """SELECT 
    dpfs, oms_sn, enp, 
    lname, fname, mname,
    birthday, birthplace,
    doc_type_id, doc_sn, doc_when, doc_who,
    snils, mcod,
    motive_att, type_att, date_att, date_det
    FROM mo
    WHERE mcod = %s;"""
    
    curr = db.con.cursor()
    curr.execute(s_sqlt,(mcod,))
    recs = curr.fetchall()

    array = []
    for rec in recs:
	
	p_mo = MO_PEOPLE()
	
	p_mo.dpfs        = rec[0]
	p_mo.oms_sn      = rec[1]
	p_mo.enp         = rec[2]
	p_mo.lname       = rec[3]
	p_mo.fname       = rec[4]
	p_mo.mname       = rec[5]
	p_mo.birthday    = rec[6]
	p_mo.birthplace  = rec[7]
	p_mo.doc_type_id = rec[8]
	p_mo.doc_sn      = rec[9]
	p_mo.doc_when    = rec[10]
	p_mo.doc_who     = rec[11]
	p_mo.snils       = rec[12]
	p_mo.mcod        = rec[13]
	p_mo.motive_att  = rec[14]
	p_mo.type_att    = rec[15]
	p_mo.date_att    = rec[16]
	p_mo.date_det    = rec[17]
	
	array.append( p_mo )
    
    return array

def mo_string(p_mo):

    sss = u''
    
    if p_mo.dpfs is None:
	sss += u';'
    else:
	sss += u'"' + p_mo.dpfs + u'";'
	
    if p_mo.oms_sn is None:
	sss += u';'
    else:
	sss += u'"' + p_mo.oms_sn + u'";'

    if p_mo.enp is None:
	sss += u';'
    else:
	sss += u'"' + p_mo.enp + u'";'
	
    if p_mo.lname is None:
	sss += u';'
    else:
	sss += u'"' + p_mo.lname + u'";'

    if p_mo.fname is None:
	sss += u';'
    else:
	sss += u'"' + p_mo.fname + u'";'

    if p_mo.mname is None:
	sss += u';'
    else:
	sss += u'"' + p_mo.mname + u'";'
	
    if p_mo.birthday is None:
	sss += u';'
    else:
	sdd = "%04d%02d%02d" % (p_mo.birthday.year, p_mo.birthday.month, p_mo.birthday.day)
	sss += u'"' + sdd + u'";'
    
    if p_mo.birthplace is None:
	sss += u';'
    else:
	sss += u'"' + p_mo.birthplace + u'";'
    
    if p_mo.doc_type_id is None:
	sss += u';'
    else:
	sss += u'"' + str(p_mo.doc_type_id) + u'";'
	
    if p_mo.doc_sn is None:
	sss += u';'
    else:
	sss += u'"' + p_mo.doc_sn + u'";'
    
    if p_mo.doc_when is None:
	sss += u';'
    else:
	sdd = "%04d%02d%02d" % (p_mo.doc_when.year, p_mo.doc_when.month, p_mo.doc_when.day)
	sss += u'"' + sdd + u'";'
    
    if p_mo.doc_who is None:
	sss += u';'
    else:
	sss += u'"' + p_mo.doc_who + u'";'
    
    if p_mo.snils is None:
	sss += u';'
    else:
	sss += u'"' + p_mo.snils + u'";'
    
    if p_mo.mcod is None:
	sss += u';'
    else:
	sss += u'"' + str(p_mo.mcod) + u'";'

    if p_mo.motive_att is None:
	sss += u';'
    else:
	sss += u'"' + str(p_mo.motive_att) + u'";'

    if p_mo.type_att is None:
	sss += u';'
    else:
	sss += u'"' + p_mo.type_att + u'";'
	
    if p_mo.date_att is None:
	sss += u';'
    else:
	sdd = "%04d%02d%02d" % (p_mo.date_att.year, p_mo.date_att.month, p_mo.date_att.day)
	sss += u'"' + sdd + u'";'

    if p_mo.date_det is not None:
	sdd = "%04d%02d%02d" % (p_mo.date_det.year, p_mo.date_det.month, p_mo.date_det.day)
	sss += u'"' + sdd + u'"'

    return sss
    

def write_mo(ar, fname):
    
    fo = open(fname, "wb")
    
    l_ar = len(ar)
    i = 0
    for p_mo in ar:
	
	i += 1
	if i == l_ar:
	    sss = mo_string(p_mo)
	else:
	    sss = mo_string(p_mo) + u"\r\n"
	ps = sss.encode('windows-1251')
	fo.write(ps)
	
    fo.close()
    
    return l_ar

	