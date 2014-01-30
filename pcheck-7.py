#!/usr/bin/python
# -*- coding: utf-8 -*-
# pcheck-7.py - удаление записей в таблице peoples
#               для пациентов заданной клиники
#

import logging
import sys, codecs
from datetime import datetime

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_pcheck7.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

CLINIC_ID  = 123

STEP = 100

s_sqlt1 = """DELETE FROM peoples
WHERE people_id = ?"""

s_sqlt2 = """DELETE FROM area_peoples
WHERE people_id_fk = ?"""


if __name__ == "__main__":
    import time
    import datetime
    from dbmis_connect2 import DBMIS
    from people import PEOPLE, get_patients, get_people
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('Peoples Check 7 Start {0}'.format(localtime))

    sout = "clinic_id: {0} database: {1}:{2}".format(CLINIC_ID, HOST, DB)
    log.info( sout )

    dbc = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    cursor = dbc.con.cursor()
    cur2   = dbc.con.cursor()
    
    clinic_name = dbc.name
    sout = "clinic_name: {0}".format(clinic_name.encode("utf-8"))
    log.info( sout )

    people = PEOPLE()
    
    results = get_patients(dbc, CLINIC_ID)
    if results is None:
        r_count = 0
    else:
        r_count = len(results)

    sout = "Clinic has got {0} patients".format(r_count)
    log.info( sout )


    dbcw = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    conw = dbcw.con
    curw = conw.cursor()
   
    counta  = 0
    countd  = 0
    counts  = 0
    
    for rec in results:
        
        counta += 1
        people.initFromRec(rec)
        people_id = people.people_id

        u_lname  = people.lname
        u_fname  = people.fname
        u_mname  = people.mname
        lname = u_lname.encode('utf-8')
        fname = u_fname.encode('utf-8')
        if u_mname is None:
            mname = ""
        else:
            mname = u_mname.encode('utf-8')

        try:
            curw.execute(s_sqlt1,(people_id, ))
            curw.execute(s_sqlt2,(people_id, ))
            conw.commit()
            countd += 1
        except:

            counts  += 1
            mname = u_mname.encode('utf-8')
            sout = "-{0}: {1} {2} {3}".format(people_id, lname, fname, mname)
            log.info( sout )

        if counta % STEP == 0: 
            sout = "{0} {1} {2} {3} {4}".format(counta, people_id, lname, fname, mname)
            log.info( sout )    
        

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Peoples Check 7 Finish  '+localtime)   
    sout = "Totally {0} peoples of {1} have been deleted".format(countd, counta)
    log.info( sout )    

    sout = "{0} peoples have been skipped".format(counts)
    log.info( sout )    

    dbc.close()
    dbcw.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Database Connections Closed  '+localtime)
    
    sys.exit(0)