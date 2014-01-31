#!/usr/bin/python
# -*- coding: utf-8 -*-
# insb-1.py - обработка ответов из ТФОМС (файлы ST22M*.csv)
#             ответы на ЗСП (запросы страховой принадлежности)
#
# INPUT DIRECTORY ST2DO - ответы из ТФОМС
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

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

ST2DO_PATH        = "./ST2DO"
STDONE_PATH       = "./STDONE"
CHECK_REGISTERED  = False
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

def st_done(db, mcod, w_month = '1311'):

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
    from dbmysql_connect import DBMY

    log.info("======================= INSB-1 ===========================================")

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
	    pfile(f_fname)
	    if REGISTER_FILE: register_st_done(dbmy2, mcod, clinic_id, fname)
	
	if MOVE_FILE:
	# move file
	    source = ST2DO_PATH + "/" + fname
	    destination = STDONE_PATH + "/" + fname
	    shutil.move(source, destination)
    
    dbmy2.close()
    sys.exit(0)
    