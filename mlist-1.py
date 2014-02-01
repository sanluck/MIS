#!/usr/bin/python
# -*- coding: utf-8 -*-
# mlist-1.py - список кодов ЛПУ
#
# INPUT DIRECTORY ST2DO - ответы из ТФОМС
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insb1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

ST2DO_PATH        = "./ST2DO"

def get_fnames(path = ST2DO_PATH, file_ext = '.zip'):
    
    import os    
    
    fnames = []
    for subdir, dirs, files in os.walk(path):
        for fname in files:
            if fname.find(file_ext) > 1:
                log.info(fname)
                fnames.append(fname)
    
    return fnames    

if __name__ == "__main__":
    
    import os, shutil
    import time
    from dbmysql_connect import DBMY

    log.info("======================= MLIST-1 ===========================================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('MO List. Start {0}'.format(localtime))
    

    fnames = get_fnames()
    n_fnames = len(fnames)
    sout = "Totally {0} files has been found".format(n_fnames)
    log.info( sout )
    
    db = DBMY()
    curr = db.con.cursor()
    curw = db.con.cursor()
    
    s_sqlf = """SELECT id FROM mlist WHERE clinic_id = %s;"""
    s_sqli = """INSERT INTO mlist
    (clinic_id, mcod, clinic_name)
    VALUES
    (%s, %s, %s);"""
    
    for fname in fnames:
	s_mcod  = fname[5:11]
	w_month = fname[12:16]
	mcod = int(s_mcod)
    
	try:
	    mo = modb[mcod]
	    c_id   = mo.mis_code
	    mname = mo.name
	    sout = "clinic_id: {0} MO Code: {1} MO Name: {2}".format(c_id, mcod, mname.encode('utf-8')) 
	    log.info(sout)
	    curr.execute(s_sqlf,(c_id, ))
	    rec = curr.fetchone()
	    if rec is None:
		curw.execute(s_sqli,(c_id, mcod, mname, ))
	except:
	    sout = "Clinic not found for mcod = {0}".format(s_mcod)
	    log.warn(sout)
	    continue
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('MO List. Finish  '+localtime)  
    
    db.con.commit()
    db.close()
    sys.exit(0)
   