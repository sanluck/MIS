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

LOG_FILENAME = '_insb5.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

STEP = 1000
PRINT_FOUND = False

MO2DO_PATH        = "./MO2DO"
MODONE_PATH       = "./MODONE"

UPDATE            = True

CHECK_REGISTERED  = True
REGISTER_FILE     = True
MOVE_FILE         = True

def register_mo_done(db, mcod, clinic_id, fname):
    import datetime

    dnow = datetime.datetime.now()
    sdnow = str(dnow)
    s_sqlt = """INSERT INTO
    mo_done
    (mcod, clinic_id, fname, done)
    VALUES
    ({0}, {1}, '{2}', '{3}');
    """

    s_sql = s_sqlt.format(mcod, clinic_id, fname, sdnow)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    db.con.commit()

def mo_done(db, mcod, w_month = '201402'):

    s_sqlt = """SELECT
    fname, done
    FROM
    mo_done
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
    from people import get_fnames, get_mo, put_mo

    log.info("======================= INSB-5 ===========================================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Registering of MO Files. Start {0}'.format(localtime))


    fnames = get_fnames(path = MO2DO_PATH, file_ext = '.csv')
    n_fnames = len(fnames)
    sout = "Totally {0} files has been found".format(n_fnames)
    log.info( sout )

    dbmy2 = DBMY()

    for fname in fnames:
	s_mcod  = fname[3:9]
	w_month = fname[9:15]
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

	f_fname = MO2DO_PATH + "/" + fname
	sout = "Input file: {0}".format(f_fname)
	log.info(sout)

	if CHECK_REGISTERED:
	    ldone, dfname, ddone = mo_done(dbmy2, mcod, w_month)
	else:
	    ldone = False

	if ldone:
	    sout = "On {0} hase been done. Fname: {1}".format(ddone, dfname)
	    log.warn( sout )
	else:
	    #pfile(f_fname)
	    ar = get_mo(f_fname)
	    l_ar = len(ar)
	    sout = "File has got {0} lines".format(l_ar)
	    log.info( sout )
	    count_a, count_i, count_u = put_mo(dbmy2, ar, UPDATE)
	    sout = "Totally {0} lines of {1} have been inserted, {2} - updated".format(count_i, count_a, count_u)
	    log.info( sout )
	    if REGISTER_FILE: register_mo_done(dbmy2, mcod, clinic_id, fname)

	if MOVE_FILE:
	# move file
	    source = MO2DO_PATH + "/" + fname
	    destination = MODONE_PATH + "/" + fname
	    shutil.move(source, destination)

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Registering of MO Files. Finish  '+localtime)

    dbmy2.close()
    sys.exit(0)
