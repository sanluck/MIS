# -*- coding: utf-8 -*-
# r_import-1.py - импорт реестров из dbf формата
#

import logging
import sys, codecs
from datetime import datetime

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_rimport.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

PR2DO_PATH        = "./PR2DO"
PRDONE_PATH       = "./PRDONE"

STEP = 100

CHECK_REGISTERED  = False
MOVE_FILE         = False


def get_fnames(path = PR2DO_PATH, file_ext = '.dbf'):
    
    import os    
    
    fnames = []
    for subdir, dirs, files in os.walk(path):
        for fname in files:
            if fname.find(file_ext) > 1:
                log.info(fname)
                fnames.append(fname)
    
    return fnames    

def register_pr_done(db, fname):
    import datetime    

    dnow = datetime.datetime.now()
    sdnow = str(dnow)
    s_sqlt = """INSERT INTO
    pr_done
    (fname, done)
    VALUES
    ('{0}', '{1}');"""

    s_sql = s_sqlt.format(fname, sdnow)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    db.con.commit()

def pr_done(db, fname_p):

    s_sqlt = """SELECT
    id, fname, done
    FROM
    pr_done
    WHERE fname LIKE '%{0}%';
    """

    s_sql = s_sqlt.format(fname_p)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    rec = cursor.fetchone()
    if rec == None:
	return False, None, "", None
    else:
	_id   = rec[0]
	fname = rec[1]
	done  = rec[2]
	return True, _id, fname, done

def pfile(f_id, fname):

    from reestr import REESTR, get_reestr, put_reestr
    from dbmysql_connect import DBMY
        
    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('PR Import Start {0}'.format(localtime))
    
    pr_arr = get_reestr(fname)
    
    n_pr   = len(pr_arr)
    
    sout = "Input file <{0}> has got {1} lines".format(fname, n_pr)
    log.info( sout )
    
    dbmy = DBMY()
    l_ret = put_reestr(dbmy, f_id, pr_arr)
    
    dbmy.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('PR Import Finish  '+localtime)
    
    return l_ret
    

if __name__ == "__main__":
    import time
    import datetime
    from dbmis_connect2 import DBMIS
    from reestr import REESTR, get_reestr
    
    import os, shutil    
    from dbmysql_connect import DBMY

    log.info("======================= rimport-1 start ===========================================")

    fnames = get_fnames()
    n_fnames = len(fnames)
    sout = "Totally {0} files has been found".format(n_fnames)
    log.info( sout )
    
    dbmy  = DBMY()
    dbmy2 = DBMY()

    for fname in fnames:

	f_fname = PR2DO_PATH + "/" + fname
	sout = "Input file: {0}".format(f_fname)
	log.info(sout)
    
	ldone, f_id, dfname, ddone = pr_done(dbmy, fname)
	
	if not ldone:
	    register_pr_done(dbmy2, fname)
	    lnewdone, f_id, dfname, ddone = pr_done(dbmy, fname)
	
	if ldone:
	    sout = "On {0} hase been done. Fname: {1}".format(ddone, dfname)
	    log.warn( sout )
	else:
	    if not pfile(f_id, f_fname): break
	
	if MOVE_FILE:
	# move file
	    source = PR2DO_PATH + "/" + fname
	    destination = PRDONE_PATH + "/" + fname
	    shutil.move(source, destination)
    
    dbmy.close()
    dbmy2.close()
    log.info("======================= rimport-1 finish ===========================================")
    sys.exit(0)
    