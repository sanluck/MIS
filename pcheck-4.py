#!/usr/bin/python
# -*- coding: utf-8 -*-
# pcheck-4.py - поиск записей в таблице peoples
#               для пациентов из файла registry.dbf
#               с последующим заданием участка клиника
#               (если таковой не задан)
#

import logging
import sys, codecs
from datetime import datetime

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_pcheck4.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb.ctmed.ru"
DB   = "DBMIS"

CLINIC_ID  = 138
DBF_DIR    = "/home/gnv/MIS/import/{0}/".format(CLINIC_ID)
TABLE_NAME = DBF_DIR + "REGISTRY.DBF"

STEP = 100

s_sqlt1 = """SELECT 
area_people_id, area_id_fk, date_beg, date_end, 
motive_attach_beg_id_fk
FROM area_peoples
WHERE people_id_fk = ?"""

ap_d = {}
ap_d[138] = [3365, '2014-01-06', 1]
ap_d[106] = [3248, '2014-01-10', 1]
ap_d[132] = [3245, '2014-01-10', 1]
ap_d[117] = [3243, '2014-01-10', 1]

AREA_ID    = ap_d[CLINIC_ID][0]
DATE_BEG   = datetime.strptime(ap_d[CLINIC_ID][1], '%Y-%m-%d')
MOTIVE_ATT = ap_d[CLINIC_ID][2]

s_sqlt2 = """INSERT INTO
area_peoples
(people_id_fk, area_id_fk, date_beg, motive_attach_beg_id_fk)
VALUES
(?, ?, ?, ?);"""

s_sqlt3 = """UPDATE area_peoples
SET date_beg = ?,
motive_attach_beg_id_fk = ?
WHERE area_people_id = ?"""

s_sqlt31 = """UPDATE area_peoples
SET 
motive_attach_beg_id_fk = ?
WHERE area_people_id = ?"""


if __name__ == "__main__":
    import time
    import datetime
    from dbmis_connect2 import DBMIS
    from people import PEOPLE, get_registry, get_people
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('Peoples Check 4 Start {0}'.format(localtime))

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
    countw  = 0
    countu  = 0
    countu1 = 0
    
    for p_dbf in p_arr:
        
        counta += 1
        
        u_lname  = p_dbf.lname
        u_fname  = p_dbf.fname
        u_mname  = p_dbf.mname
        birthday = p_dbf.birthday

        lname = u_lname.encode('utf-8')
        fname = u_fname.encode('utf-8')
        if u_mname is None:
            mname = ""
        else:
            mname = u_mname.encode('utf-8')

        if counta % STEP == 0: 
            sout = "{0} {1} {2} {3}".format(counta, lname, fname, mname)
            log.info( sout )    
        
        pf_arr = get_people(cursor, u_lname, u_fname, u_mname, birthday)
        if pf_arr is None:
            sout = "Person {0} {1} {2} was not found in the DBMIS".format(lname, fname, mname)
            log.info( sout )
        else:
            for pf in pf_arr:
                people_id = pf[0]
                if counta % STEP == 0: 
                    sout = "people_id: {0}".format(people_id)
                    log.info( sout )    
                
                cur2.execute(s_sqlt1,(people_id, ))
                rec2 = cur2.fetchone()
                if rec2 is None:
                    curw.execute(s_sqlt2,(people_id, AREA_ID, DATE_BEG, MOTIVE_ATT))
                    conw.commit()
                    countw += 1
                else:
                    area_people_id = rec2[0]
                    area_id_fk     = rec2[1]
                    date_beg       = rec2[2]
                    date_end       = rec2[3] 
                    motive_att     = rec2[4]
                    
                    if date_beg is None:
                        curw.execute(s_sqlt3,(DATE_BEG, MOTIVE_ATT, area_people_id))
                        conw.commit()
                        countu  += 1
                    elif motive_att is None:
                        curw.execute(s_sqlt31,(MOTIVE_ATT, area_people_id))
                        conw.commit()
                        countu1 += 1

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Peoples Check 4 Finish  '+localtime)   
    sout = "Totally for {0} peoples of {1} assigned area_id {2} has been assigned".format(countw, counta, AREA_ID)
    log.info( sout )    

    sout = "Totally for {0} peoples date_beg {1} has been assigned".format(countu, DATE_BEG)
    log.info( sout )    

    sout = "Totally for {0} peoples motive_attach_beg_id_fk {1} has been assigned".format(countu1, MOTIVE_ATT)
    log.info( sout )    

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Peoples Check 4 Finish  '+localtime)
    
    dbc.close()
    dbcw.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Database Connections Closed  '+localtime)
    sys.exit(0)