#!/usr/bin/python
# -*- coding: utf-8 -*-
# st2mira.py -  импорт данных из файлов ST (ответы ТФОМС на ЗСП)
#               в таблицу mira$peoples
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_st2mira.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

STEP = 100

ST2DO_PATH        = "./ST2DO"
STDONE_PATH       = "./STDONE"

APPEND_DATA       = False

CHECK_REGISTERED  = False
REGISTER_FILE     = False
MOVE_FILE         = False

def register_st_done(db, mcod, clinic_id, fname, lcount):
    import datetime    

    dnow = datetime.datetime.now()
    sdnow = str(dnow)
    s_sqlt = """INSERT INTO
    mira$st_done
    (mcod, clinic_id, fname, done, lcount)
    VALUES
    ({0}, {1}, '{2}', '{3}', {4});
    """

    s_sql = s_sqlt.format(mcod, clinic_id, fname, sdnow, lcount)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    db.con.commit()

def st_done(db, mcod, w_month = '1312'):

    s_sqlt = """SELECT
    fname, done
    FROM
    mira$st_done
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
    import datetime
    from people import get_fnames, get_st, put_st2mira
    from dbmysql_connect import DBMY
    from people import put_sm2mira
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')

    log.info('ST2MIRA. Start {0}'.format(localtime))

    fnames = get_fnames(path = ST2DO_PATH, file_ext = '.csv')
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
	    mname = mo.name.encode('utf-8')
	    sout = "clinic_id: {0} MO Code: {1} MO Name: {2}".format(clinic_id, mcod, mname) 
	    log.info(sout)
	except:
	    sout = "Clinic not found for mcod = {0}".format(s_mcod)
	    log.warn(sout)
	    clinic_id = 0

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
	    ar_st = get_st(f_fname, mcod)
	    l_ar = len(ar_st)
	    sout = "File has got {0} lines".format(l_ar)
	    log.info( sout )

	    count_a, count_i, count_u = put_st2mira(dbmy2, ar_st, append = APPEND_DATA)

	    sout = "{0} records have been insrted into mira$peoples table".format(count_i)
	    log.info( sout )
	    sout = "{0} records have been updated".format(count_u)
	    log.info( sout )

	    if REGISTER_FILE: register_st_done(dbmy2, mcod, clinic_id, fname, l_ar)
	
	if MOVE_FILE:
	# move file
	    source = SM2DO_PATH + "/" + fname
	    destination = SMDONE_PATH + "/" + fname
	    shutil.move(source, destination)
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('ST2MIRA. Finish  '+localtime)  
    
    dbmy2.close()
    sys.exit(0)


