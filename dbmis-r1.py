#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# dbmis-r1.py - simple DBMIS response monitoring
#

import sys
import logging

CLINIC_ID = 22
MIS_HOST = 'fb.ctmed.ru'
MIS_DB   = 'DBMIS'

S_SQLT   = """SELECT current_timestamp FROM rdb$database;"""

W_SEC = 10
COUNT = 360*5

LOG_FILENAME = '_dbmisr1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

def get_exectime(cur):
    import time
    t1 = time.time()
    cur.execute(S_SQLT)
    rec = cursor.fetchone()
    t2 = time.time()
    return t2-t1
    

if __name__ == "__main__":
    from dbmis_connect2 import DBMIS
    from dbmysql_connect import DBMY
        
    import time
    import datetime

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('DBMIS Monitoring Start {0}'.format(localtime))
    
    dbc    = DBMIS(clinic_id = CLINIC_ID, mis_host = MIS_HOST, mis_db = MIS_DB)
    cursor = dbc.con.cursor()
    
    cursor.execute(S_SQLT)
    rec = cursor.fetchone()
    
    for i in range(50):
        dt = get_exectime(cursor)
        dnow = datetime.datetime.now()
        sdnow = str(dnow)        
        sout = "{0} Execution time: {1}".format(sdnow, dt)
        log.info(sout)
        time.sleep(W_SEC)
        
        
    
    dbc.close()
    sys.exit(0)