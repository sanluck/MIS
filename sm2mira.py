#!/usr/bin/python
# -*- coding: utf-8 -*-
# sm2mira.py -  импорт данных из файлов SM
#               в таблицу mira$peoples
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_sm2mira.out'
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
