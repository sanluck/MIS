#!/usr/bin/python
# -*- coding: utf-8 -*-
# pcheck-8.py - поиск записей в таблице peoples
#               для пациентов из файла SM
#               с последующим заданием документа и полиса ОМС
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_pcheck8.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

STEP = 100

s_sqlt1 = """UPDATE peoples
SET 
document_type_id_fk = ?,
document_series = ?,
document_number = ?
WHERE people_id = ?"""

s_sqlt2 = """UPDATE peoples
SET 
medical_insurance_series = ?,
medical_insurance_number = ?
WHERE people_id = ?"""

SM2DO_PATH        = "./SM2DO"
SMDONE_PATH       = "./SMDONE"

CHECK_REGISTERED  = False
REGISTER_FILE     = False
MOVE_FILE         = False


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
	sss  = ''.join(a_ar)
	return sss
    else:
	return document_number


def get_sm(fname):
    from datetime import datetime
    from people import SM_PEOPLE
    
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
	
	sm_p.dpfs             = dpfs
	sm_p.s_oms            = s_oms
	sm_p.n_oms            = n_oms
	
	array.append( sm_p )
    
    ins.close()    
    
    return array

def check_sm(clinic_id, mcod, sm_ar):

    from dbmis_connect2 import DBMIS
    from people import PEOPLE, get_people

    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
    if dbc.ogrn == None:
        CLINIC_OGRN = u""
    else:
        CLINIC_OGRN = dbc.ogrn

    cogrn = CLINIC_OGRN.encode('utf-8')
    cname = dbc.name.encode('utf-8')
    
    sout = "clinic_id: {0} cod_mo: {1} clinic_name: {2} clinic_ogrn: {3}".format(clinic_id, mcod, cname, cogrn)
    log.info(sout)
    
    curr = dbc.con.cursor()
    curw = dbc.con.cursor()

    counta   = 0
    
    count_nf = 0
    
    for sm_p in sm_ar:
	
	counta  += 1
	
        u_lname  = sm_p.lname
        u_fname  = sm_p.fname
        u_mname  = sm_p.mname
        birthday = sm_p.birthday

        lname = u_lname.encode('utf-8')
        fname = u_fname.encode('utf-8')
        if u_mname is None:
            mname = ""
        else:
            mname = u_mname.encode('utf-8')

	d_type_id = sm_p.document_type_id
	d_series  = sm_p.document_series
	d_number  = sm_p.document_number

	dpfs      = sm_p.dpfs
	s_oms     = sm_p.s_oms
	n_oms     = sm_p.n_oms


        if counta % STEP == 0:
	    if d_series is not None:
		d_sss = d_series.encode('utf-8')
	    else d_sss = 'None'
            sout = "{0} {1} {2} {3} {4} {5} {6}".format(counta, lname, fname, mname, d_type_id, d_sss, d_number)
            log.info( sout )
	    
        
        pf_arr = get_people(curr, u_lname, u_fname, u_mname, birthday)
	
        if (pf_arr is None) or (len(pf_arr) == 0):
	    count_nf += 1
        else:
	    
            for pf in pf_arr:
                people_id = pf[0]
		
		if (d_type_id is not None) and (d_number is not None):
		    curw.execute(s_sqlt1, (d_type_id, d_series, d_number, people_id))
		    dbc.con.commit()

		if (dpfs is not None) and (n_oms is not None):
		    curw.execute(s_sqlt2, (dpfs, s_oms, n_oms, people_id))
		    dbc.con.commit()

		
                if counta % STEP == 0: 
                    sout = "people_id: {0}".format(people_id)
                    log.info( sout )    
	
	
    dbc.close()
    sout = "Totally {0} peoples have been checked".format(counta)
    log.info( sout )    
    
    sout = "{0} peoples have not been found in the DBMIS".format(count_nf)
    log.info( sout )    
	

def register_sm_done(db, mcod, clinic_id, fname):
    import datetime    

    dnow = datetime.datetime.now()
    sdnow = str(dnow)
    s_sqlt = """INSERT INTO
    sm_done
    (mcod, clinic_id, fname, done)
    VALUES
    ({0}, {1}, '{2}', '{3}');
    """

    s_sql = s_sqlt.format(mcod, clinic_id, fname, sdnow)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    db.con.commit()

def sm_done(db, mcod, w_month = '1311'):

    s_sqlt = """SELECT
    fname, done
    FROM
    sm_done
    WHERE mcod = {0} AND fname LIKE '%{1}%';
    """

    s_sql = s_sqlt.format(mcod, w_month)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    rec = cursor.fetchone()
    if rec == None:
	return False, "", ""
    else:
	fname = rec[0]
	done  = rec[1]
	return True, fname, done

if __name__ == "__main__":
    import time
    import datetime
    from dbmysql_connect import DBMY
    from people import PEOPLE, get_registry, get_people
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')

    log.info('PCHECK-8. Start {0}'.format(localtime))

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info( sout )

    fnames = get_fnames()
    n_fnames = len(fnames)
    sout = "Totally {0} files has been found".format(n_fnames)
    log.info( sout )
    
    dbmy2 = DBMY()
    
    for fname in fnames:
	s_mcod  = fname[2:8]
	w_month = fname[12:16]
	mcod = int(s_mcod)
	try:
	    mo = modb[mcod]
	    clinic_id = mo.mis_code
	    sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod) 
	    log.info(sout)
	except:
	    sout = "Clinic not found for mcod = {0}".format(s_mcod)
	    log.warn(sout)
	    clinic_id = 0

	f_fname = SM2DO_PATH + "/" + fname
	sout = "Input file: {0}".format(f_fname)
	log.info(sout)
    
	if CHECK_REGISTERED:
	    ldone, dfname, ddone = sm_done(dbmy2, mcod, w_month)
	else:
	    ldone = False
	    
	if ldone:
	    sout = "On {0} hase been done. Fname: {1}".format(ddone, dfname)
	    log.warn( sout )
	else:
	    #pfile(f_fname)
	    ar = get_sm(f_fname)
	    l_ar = len(ar)
	    sout = "File has got {0} lines".format(l_ar)
	    log.info( sout )

	    check_sm(clinic_id, mcod, ar)

	    log.info( sout )
	    if REGISTER_FILE: register_sm_done(dbmy2, mcod, clinic_id, fname)
	
	if MOVE_FILE:
	# move file
	    source = SM2DO_PATH + "/" + fname
	    destination = SMDONE_PATH + "/" + fname
	    shutil.move(source, destination)
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('PCHECK-8. Finish  '+localtime)  
    
    dbmy2.close()
    sys.exit(0)
