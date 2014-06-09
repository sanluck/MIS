#!/usr/bin/python
# -*- coding: utf-8 -*-
# insb-1-3.py - обработка ответов из ТФОМС на ЗСП
#               (MySQL: mis.sm - файлы ST22M*.csv)
#               приписываем enp в DBMIS (peoples)
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()
from insorglist import InsorgInfoList
insorgs = InsorgInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insb1_3.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

STEP = 1000

UPDATE            = True

#SQLT_SMR = """SELECT people_id, enp
#FROM sm 
#WHERE enp_2_dbmis is Null
#LIMIT 100;"""

SQLT_SMR = """SELECT people_id, enp
FROM sm 
WHERE enp_2_dbmis is Null;"""

SQLT_SMU = """UPDATE sm
SET
enp_2_dbmis = 1
WHERE
people_id = %s;"""

CLINIC_ID = 22
HOST = "fb.ctmed.ru"
DB   = "DBMIS"

SQLT_PEOPLESU = """UPDATE peoples
SET
enp = ?
WHERE
people_id = ?;"""


if __name__ == "__main__":
    
    import os, shutil
    import time
    from dbmysql_connect import DBMY
    from dbmis_connect2 import DBMIS

    import datetime    

    dnow = datetime.datetime.now()
    sdnow = str(dnow)
    

    log.info("======================= INSB-1-3 ===========================================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Put ENP into DBMIS. Start {0}'.format(localtime))
    
    dbmy = DBMY()
    curr = dbmy.con.cursor()
    curw = dbmy.con.cursor()
    
    curr.execute(SQLT_SMR)
    results = curr.fetchall()

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    dbc = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    cur = dbc.con.cursor()

    nall = 0
    nfound = 0
    for rec in results:
        nall += 1
        p_id = rec[0]
        enp  = rec[1]
	
	if nall % STEP == 0:
	    sout = " {0}: people_id: {1} enp: {2}".format(nall, p_id, enp)
	    log.info(sout)
	    
        if enp is None:
            nfound += 1
            continue
	
	cur.execute(SQLT_PEOPLESU, (enp, p_id, ))
	dbc.con.commit()
	    
	curw.execute(SQLT_SMU,( p_id, ))
	dbmy.con.commit()
	
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Put ENP into DBMIS. Finish  '+localtime)  
    
    sout = "Totally {0} records have been processed".format(nall)
    log.info(sout)

    sout = " {0} people_id have not got ENP in the sm table".format(nfound)
    log.info(sout)

    dbc.close()
    dbmy.close()
    sys.exit(0)    
        
        
        
        
    