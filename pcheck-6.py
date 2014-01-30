#!/usr/bin/python
# -*- coding: utf-8 -*-
# pcheck-6.py - поиск записей в таблице peoples
#               для пациентов из файла registry.dbf
#               имеющих обрезанное отчество (registry.patron2)
#               замена на полное отчество
#

import logging
import sys, codecs
from datetime import datetime

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_pcheck6.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

CLINIC_ID  = 123
DBF_DIR    = "/home/gnv/MIS/import/{0}/".format(CLINIC_ID)
TABLE_NAME = DBF_DIR + "REGISTRY.DBF"

STEP = 100

s_sqlt1 = """UPDATE peoples
SET mname = ?
WHERE people_id = ?"""


if __name__ == "__main__":
    import time
    import datetime
    from dbmis_connect2 import DBMIS
    from people2 import PEOPLE, get_registry, get_people
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('Peoples Check 6 Start {0}'.format(localtime))

    sout = "Table Name: {0}".format(TABLE_NAME)
    log.info( sout )
    
    p_arr = get_registry(TABLE_NAME)
    l_p_arr = len(p_arr)
    sout = "Table has got {0} records".format(l_p_arr)
    log.info( sout )

    sout = "clinic_id: {0} database: {1}:{2}".format(CLINIC_ID, HOST, DB)
    log.info( sout )
    

    dbc = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    cursor = dbc.con.cursor()
    cur2   = dbc.con.cursor()
    
    clinic_name = dbc.name
    sout = "clinic_name: {0}".format(clinic_name.encode("utf-8"))
    log.info( sout )

    dbcw = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    conw = dbcw.con
    curw = conw.cursor()
   
    counta  = 0
    countu  = 0
    countn  = 0
    counte  = 0
    
    for p_dbf in p_arr:
        
        counta += 1
        
        u_lname  = p_dbf.lname
        u_fname  = p_dbf.fname
        u_mname  = p_dbf.mname
        u_mname2 = p_dbf.mname2
        birthday = p_dbf.birthday

        lname = u_lname.encode('utf-8')
        fname = u_fname.encode('utf-8')
        if u_mname is None:
            mname = ""
        else:
            mname = u_mname.encode('utf-8')

        if u_mname2 is None:
            mname2 = ""
        else:
            mname2 = u_mname2.encode('utf-8')

        if counta % STEP == 0: 
            sout = "{0} {1} {2} {3}".format(counta, lname, fname, mname2)
            log.info( sout )    
        
        if u_mname == u_mname2:
            counte += 1
            continue
        
        if birthday is None:
            sout = "{0} {1} {2} {3} - no birthday".format(counta, lname, fname, mname2)
            log.info( sout )
            pf_arr = None
            continue
        else:
            pf_arr = get_people(cursor, u_lname, u_fname, u_mname2, birthday)
            
        if (pf_arr is None) or (len(pf_arr) == 0):
            countn  += 1
        else:
            if len(u_mname) == 0: continue
            mname = u_mname[0].upper() + u_mname[1:].lower()
            for pf in pf_arr:
                people_id = pf[0]
                if counta % STEP == 0: 
                    sout = "people_id: {0}".format(people_id)
                    log.info( sout )    
                
                curw.execute(s_sqlt1,(mname, people_id))
                conw.commit()
                countu += 1
    

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Peoples Check 6 Finish  '+localtime)   
    sout = "Totally for {0} peoples of {1} have got full mname".format(countu, counta)
    log.info( sout )    

    sout = "{0} peoples have got Patron2 equal to Patronymic".format(counte)
    log.info( sout )    

    sout = "{0} peoples have not been found in the poples table".format(countn)
    log.info( sout )    
    
    dbc.close()
    dbcw.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Database Connections Closed  '+localtime)
    
    sys.exit(0)