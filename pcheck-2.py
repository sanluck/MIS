#!/usr/bin/python
# -*- coding: utf-8 -*-
# pcheck-2.py - проверка записей в таблице peoples
#               для пациентов заданной клиники
#               задание типа оплаты по возрасту
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_pcheck2.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

CLINIC_ID  = 106

STEP = 1000

s_sqlt1 = """UPDATE peoples
SET p_payment_type_id_fk = ?
WHERE people_id = ?"""

if __name__ == "__main__":
    import time
    import datetime
    from dbmis_connect2 import DBMIS
    from people import PEOPLE, SQLT_PEOPLE
    
    now = datetime.datetime.now()
    s_now = "%04d-%02d-%02d" % (now.year, now.month, now.day)    
    y_now = now.year
    y_now = 2013
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('Peoples Check 2 Start {0}'.format(localtime))
    
    
    sout = "clinic_id: {0} database: {1}:{2}".format(CLINIC_ID, HOST, DB)
    log.info( sout )

    dbc = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    
    clinic_name = dbc.name
    sout = "clinic_name: {0}".format(clinic_name.encode("utf-8"))
    log.info( sout )
    
    people = PEOPLE()
    
    s_sql = SQLT_PEOPLE.format(CLINIC_ID)
    
    cursor = dbc.con.cursor()
    cursor.execute(s_sql)
    results = cursor.fetchall()
    r_count = len(results)

    sout = "Totally {0} records has been fetched from the peoples table".format(r_count)
    log.info( sout )

    dbcw = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    conw = dbcw.con
    curw = conw.cursor()

    counta  = 0
    countg  = 0
    for rec in results:
        
        counta += 1
        people.initFromRec(rec)
        people_id = people.people_id

        if counta % STEP == 0: 
            sout = "{0} {1}".format(counta, people_id)
            log.info( sout )    
    
        payment_type = people.p_payment_type_id_fk
        
        if payment_type is not None: continue
  
        p_bd = people.birthday
        p_by = p_bd.year
        age  = y_now - p_by

        if (age >= 18) and (age <= 60):
            ppt = 1
        else:
            ppt = 2
        
        curw.execute(s_sqlt1,(ppt, people_id))
        conw.commit()
        
        countg += 1
        
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Peoples Check 2 Finish  '+localtime)    
    sout = "Totally {0} records of {1} have updated".format(countg, counta)
    log.info( sout )
    
    dbc.close()
    dbcw.close()
    sys.exit(0)    