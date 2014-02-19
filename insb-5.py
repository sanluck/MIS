#!/usr/bin/python
# -*- coding: utf-8 -*-
# insb-5.py - импорт файлов MO в DBMY (mis.mo)
#
# INPUT DIRECTORY MO2DO
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()
from insorglist import InsorgInfoList
insorgs = InsorgInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insb1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

STEP = 1000
PRINT_FOUND = False

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

ST2DO_PATH        = "./ST2DO"
STDONE_PATH       = "./STDONE"

UPDATE            = True

CHECK_REGISTERED  = True
REGISTER_FILE     = True
MOVE_FILE         = True



def get_fnames(path = ST2DO_PATH, file_ext = '.csv'):
    
    import os    
    
    fnames = []
    for subdir, dirs, files in os.walk(path):
        for fname in files:
            if fname.find(file_ext) > 1:
                log.info(fname)
                fnames.append(fname)
    
    return fnames    

def get_st(fname):
    ins = open( fname, "r" )

    array = []
    for line in ins:
	u_line = line.decode('cp1251')
	a_line = u_line.split("|")
	people_id  = int(a_line[0])
	ocato      = a_line[1]
	s_smo_code = a_line[2]
	if len(s_smo_code) == 0:
	    smo_code = None
	else:
	    smo_code = int(s_smo_code)
	dpfs_code  = int(a_line[3])
	oms_series = a_line[4]
	oms_number = a_line[5]
	enp        = a_line[6]
	if (len(a_line) > 7) and (a_line[7] != u'\r\n'):
	    mcod = int(a_line[7])
	else:
	    mcod    = None
	a_rec = [people_id, ocato, smo_code, dpfs_code, oms_series, oms_number, enp, mcod]
	array.append( a_rec )
    
    ins.close()    
    
    return array

def write_st(db, ar, upd = False):
    
    s_sqlf = """SELECT oms_series, oms_number, enp, mcod
    FROM
    sm
    WHERE people_id = %s"""

    s_sqli = """INSERT INTO
    sm
    (people_id, ocato, 
    smo_code, dpfs_code, oms_series, oms_number, enp,
    mcod)
    VALUES 
    (%s, %s,
    %s, %s, %s, %s, %s,
    %s);"""


    s_sqlu = """UPDATE
    sm
    SET
    ocato = %s, 
    smo_code = %s,
    dpfs_code = %s,
    oms_series = %s,
    oms_number = %s,
    enp = %s,
    mcod = %s
    WHERE 
    people_id = %s;"""

    
    curr = db.con.cursor()
    curw = db.con.cursor()
    count_a = 0
    count_i = 0
    count_u = 0
    
    for rec in ar:
	count_a += 1
	
	people_id  = rec[0]
	ocato      = rec[1]
	smo_code   = rec[2]
	dpfs_code  = rec[3]
	oms_series = rec[4]
	oms_number = rec[5]
	enp        = rec[6]
	mcod       = rec[7]

	if count_a % STEP == 0:
	    sout = " {0} people_id: {1} enp: {2} mcod: {3}".format(count_a, people_id, enp, mcod)
	    log.info(sout)
	
	curr.execute(s_sqlf,(people_id,))
	rec = curr.fetchone()
	if rec is None:
	    try:
		curw.execute(s_sqli,(people_id, ocato, smo_code, dpfs_code, oms_series, oms_number, enp, mcod,))
		db.con.commit()	
		count_i += 1
	    except Exception, e:
		sout = "Can't insert into sm table. UID: {0}".format(people_id)
		log.error(sout)
		sout = "{0}".format(e)
		log.error(sout)
	else:
	    if upd:
		try:
		    curw.execute(s_sqlu,(ocato, smo_code, dpfs_code, oms_series, oms_number, enp, mcod, people_id,))
		    db.con.commit()	
		    count_u += 1
		except Exception, e:
		    sout = "Can't update sm table. UID: {0}".format(people_id)
		    log.error(sout)
		    sout = "{0}".format(e)
		    log.error(sout)
	    if PRINT_FOUND:
		f_oms_series = rec[0]
		f_oms_number = rec[1]
		f_enp        = rec[2]
		f_mcod       = rec[3]
		
		sout = "Found in sm: {0} enp: {1} | {2} mcod: {3} | {4} ".format(people_id, enp, f_enp, mcod, f_mcod)
		log.info(sout)
		
	    
    return count_a, count_i, count_u
	
def register_st_done(db, mcod, clinic_id, fname):
    import datetime    

    dnow = datetime.datetime.now()
    sdnow = str(dnow)
    s_sqlt = """INSERT INTO
    st_done
    (mcod, clinic_id, fname, done)
    VALUES
    ({0}, {1}, '{2}', '{3}');
    """

    s_sql = s_sqlt.format(mcod, clinic_id, fname, sdnow)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    db.con.commit()

def st_done(db, mcod, w_month = '1402'):

    s_sqlt = """SELECT
    fname, done
    FROM
    st_done
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
    
    import os, shutil
    import time
    from dbmysql_connect import DBMY

    log.info("======================= INSB-1 ===========================================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Registering of Insurance Belonging Replies. Start {0}'.format(localtime))
    

    fnames = get_fnames()
    n_fnames = len(fnames)
    sout = "Totally {0} files has been found".format(n_fnames)
    log.info( sout )
    
    dbmy2 = DBMY()
    
    for fname in fnames:
	s_mcod  = fname[5:11]
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
	    continue

	f_fname = ST2DO_PATH + "/" + fname
	sout = "Input file: {0}".format(f_fname)
	log.info(sout)
    
	if CHECK_REGISTERED:
	    ldone, dfname, ddone = st_done(dbmy2, mcod, w_month)
	else:
	    ldone = False
	    
	if ldone:
	    sout = "On {0} hase been done. Fname: {1}".format(ddone, dfname)
	    log.warn( sout )
	else:
	    #pfile(f_fname)
	    ar = get_st(f_fname)
	    l_ar = len(ar)
	    sout = "File has got {0} lines".format(l_ar)
	    log.info( sout )
	    count_a, count_i, count_u = write_st(dbmy2, ar, UPDATE)
	    sout = "Totally {0} lines of {1} have been inserted, {2} - updated".format(count_i, count_a, count_u)
	    log.info( sout )
	    if REGISTER_FILE: register_st_done(dbmy2, mcod, clinic_id, fname)
	
	if MOVE_FILE:
	# move file
	    source = ST2DO_PATH + "/" + fname
	    destination = STDONE_PATH + "/" + fname
	    shutil.move(source, destination)
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Registering of Insurance Belonging Replies. Finish  '+localtime)  
    
    dbmy2.close()
    sys.exit(0)
    