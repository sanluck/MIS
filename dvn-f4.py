#!/usr/bin/python
# -*- coding: utf-8 -*-
# dvn-f4.py - - обработка ответов из ТФОМС (файлы VT22M*.xls)
#
# INPUT DIRECTORY VT2DO - файлы с ошибками в реестрах ДВН
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()
from insorglist import InsorgInfoList
insorgs = InsorgInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_dvnf4.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

VT2DO_PATH    = "./VT2DO"
VTDONE_PATH   = "./VTDONE"
REGISTER_FILE = True
MOVE_FILE     = True

STEP = 100

def plist_in(fname):
# read xls file <fname>
# and get peoples list
    import xlrd
    
    workbook = xlrd.open_workbook(fname)
    
    worksheets = workbook.sheet_names()
    wshn0 = worksheets[0]
    worksheet = workbook.sheet_by_name(wshn0)

    curr_row = 2
    num_rows = worksheet.nrows - 1
    arr = []
    
    while curr_row < num_rows:
	curr_row += 1
	row = worksheet.row(curr_row)
	# Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
	c1_type = worksheet.cell_type(curr_row, 0)
	if c1_type != 2: continue
	    
	code        = worksheet.cell_value(curr_row, 4)
	people_id_s = worksheet.cell_value(curr_row, 11)
	people_id   = int(people_id_s)
	
	arr.append([people_id, code])
    
    workbook.release_resources()
    return arr

def get_cc_lines(db, people_id):
    s_sqlt = """SELECT clinical_checkup_id
    FROM clinical_checkups
    WHERE people_id_fk = {0}
    ORDER BY clinical_checkup_id;"""
    s_sql  = s_sqlt.format(people_id)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    recs = cursor.fetchall()
    ar = []
    for rec in recs:
	cc_id = rec[0]
	ar.append(cc_id)
	
    return ar
    

def pfile(fname):
    from dbmis_connect2 import DBMIS
    from dbmysql_connect import DBMY
    from PatientInfo2 import PatientInfo2
        
    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('VT Processing Start {0}'.format(localtime))
    
    i1 = fname.find("VT22M")
    if i1 < 0:
	sout = "Wrong file name <{0}>".format(fname)
	log.warn(sout)
	return

    s_mcod = fname[i1+5:i1+11]
    mcod   = int(s_mcod)

    try:
	mo = modb[mcod]
	clinic_id = mo.mis_code
	sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod) 
	log.info(sout)
    except:
	sout = "Clinic not found for mcod = {0}".format(s_mcod)
	log.warn(sout)
	return

    
    ppp = plist_in(fname)
    
    sout = "Totally {0} lines have been read from file <{1}>".format(len(ppp), fname)
    log.info(sout)

    

    p_obj = PatientInfo2()

    dbc = DBMIS(clinic_id)
    cname = dbc.name.encode('utf-8')

    sout = "cod_mo: {0} clinic_id: {1} clinic_name: {2} ".format(mcod, clinic_id, cname)
    log.info(sout)

    wrong_clinic = 0
    wrong_insorg = 0
    ncount = 0
    dbc2 = DBMIS(clinic_id)
    cur2 = dbc2.con.cursor()
    dbmy = DBMY()

    double_number = 0
    dd_number     = 0
    condidate_number = 0
    
    for prec in ppp:
        ncount += 1
        people_id = prec[0]
        err_code  = prec[1]

        if ncount % STEP == 0:
            sout = " {0} people_id: {1}".format(ncount, people_id)
            log.info(sout)
	
	if err_code in (54,57):
	    cc_lines = get_cc_lines(dbc, people_id)
	    for cc_id in cc_lines:
		s_sqlt = """UPDATE clinical_checkups
		SET
		people_status_code = 1
		WHERE clinical_checkup_id = {0}"""
		s_sql = s_sqlt.format(cc_id)
		dbc2.con.execute_immediate(s_sql)
		dbc2.con.commit()
		condidate_number += 1
		
	if err_code in (70,71):
	    double_number += 1
	    cc_lines = get_cc_lines(dbc, people_id)
	    if len(cc_lines) > 1:
		dd_number += 1
		iii = 0
		for cc_id in cc_lines:
		    iii += 1
		    sout = "people_id: {0} cc_id: {1}".format(people_id, cc_id)
		    log.info(sout)
		    if iii == 1: continue
		    s_sqlt = """UPDATE clinical_checkups
		    SET
		    people_status_code = 2
		    WHERE clinical_checkup_id = {0}"""
		    s_sql = s_sqlt.format(cc_id)
		    dbc2.con.execute_immediate(s_sql)
		    dbc2.con.commit()
		    
	    elif len(cc_lines) == 1:
		cc_id = cc_lines[0]
		s_sqlt = """UPDATE clinical_checkups
		SET
		people_status_code = 2
		WHERE clinical_checkup_id = {0}"""
		s_sql = s_sqlt.format(cc_id)
		dbc2.con.execute_immediate(s_sql)
		dbc2.con.commit()
		

    sout = "Candidates number: {0}".format(condidate_number)
    log.info(sout)


    sout = "Double DVN cases: {0}".format(double_number)
    log.info(sout)

    sout = "Double people_id: {0}".format(dd_number)
    log.info(sout)

    
    dbc.close()
    dbc2.close()
    dbmy.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('VT Processing Finish  '+localtime)

def get_fnames(path = VT2DO_PATH, file_ext = '.xls'):
    
    import os    
    
    fnames = []
    for subdir, dirs, files in os.walk(path):
        for fname in files:
            if fname.find(file_ext) > 1:
                log.info(fname)
                fnames.append(fname)
    
    return fnames    

def register_vt_done(db, mcod, clinic_id, fname):
    import datetime    

    dnow = datetime.datetime.now()
    sdnow = str(dnow)
    s_sqlt = """INSERT INTO
    vt_done
    (mcod, clinic_id, fname, done)
    VALUES
    ({0}, {1}, '{2}', '{3}');
    """

    s_sql = s_sqlt.format(mcod, clinic_id, fname, sdnow)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    db.con.commit()

def vt_done(db, mcod):

    s_sqlt = """SELECT
    fname, done
    FROM
    vt_done
    WHERE mcod = {0};
    """

    s_sql = s_sqlt.format(mcod)
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
    
    import os, shutil    
    from dbmysql_connect import DBMY

    log.info("======================= DVN-F4 ===========================================")

    fnames = get_fnames()
    n_fnames = len(fnames)
    sout = "Totally {0} files has been found".format(n_fnames)
    log.info( sout )
    
    dbmy2 = DBMY()
    
    for fname in fnames:
	s_mcod = fname[5:11]
	mcod = int(s_mcod)
    
	try:
	    mo = modb[mcod]
	    clinic_id = mo.mis_code
	    sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod) 
	    log.info(sout)
	except:
	    sout = "Clinic not found for mcod = {0}".format(s_mcod)
	    log.warn(sout)
	    continue

	f_fname = VT2DO_PATH + "/" + fname
	sout = "Input file: {0}".format(f_fname)
	log.info(sout)
    
	ldone, dfname, ddone = vt_done(dbmy2, mcod)
	if ldone:
	    sout = "On {0} hase been done. Fname: {1}".format(ddone, dfname)
	    log.warn( sout )
	else:
	    pfile(f_fname)
	    if REGISTER_FILE: register_vt_done(dbmy2, mcod, clinic_id, fname)
	
	if MOVE_FILE:
	# move file
	    source = VT2DO_PATH + "/" + fname
	    destination = VTDONE_PATH + "/" + fname
	    shutil.move(source, destination)
    
    dbmy2.close()
    sys.exit(0)
