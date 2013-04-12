#!/usr/bin/python
#encoding: utf-8
#
# test DBMIS query
#
import sys
import codecs
import time
import logging

from medlib.modules.dbmis_connect import DBMIS

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_Test_DBMIS_Query.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

MIS_LPU_ID = 229

SQL_TEST1 = """SELECT * FROM VW_PEOPLES_MANY_AREAS WHERE (cnt_2 >= 0 OR cnt_1 >= 0) ORDER BY lname, fname, mname"""

if __name__ == "__main__":

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('Test Query to DBMIS Start {0}'.format(localtime))
    log.info('LPU ID: {0}'.format(MIS_LPU_ID))
  
    dbc = DBMIS(MIS_LPU_ID)
    
    ssql = SQL_TEST1
    log.info("Testing query: {0}".format(ssql))
    t1 = time.time()
    dbc.cur.execute(ssql)
    results = dbc.cur.fetchall()
    t2 = time.time()
    tdiff = t2-t1
    log.info("The query was exucuted and all the results fetched in {0} sec".format(tdiff))
    log.info("Results length: {0}".format(len(results)))
    dbc.close()
    sys.exit(0)