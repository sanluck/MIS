#!/usr/bin/python
# -*- coding: utf-8 -*-
# pcheck-5.py - для всех записей в таблице area_peoples
#               которые относятся к заданной клинике
#               проверка наличая date_beg и motive_attach_beg_id_fk
#               (если None, то задать)
#

import logging
import sys, codecs
from datetime import datetime

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_pcheck5.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

CLINIC_ID  = 138

STEP = 100

s_sqlt1 = """SELECT ap.area_people_id, ap.area_id_fk, ap.date_beg, ap.date_end, 
ap.motive_attach_beg_id_fk
FROM area_peoples ap
JOIN areas ar ON ap.area_id_fk = ar.area_id
JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
WHERE ca.clinic_id_fk = {0}"""

ap_d = {}
ap_d[138] = [3365, '2014-01-06', 1]
ap_d[106] = [3248, '2014-01-10', 1]
ap_d[132] = [3245, '2014-01-10', 1]
ap_d[117] = [3243, '2014-01-10', 1]

AREA_ID    = ap_d[CLINIC_ID][0]
DATE_BEG   = datetime.strptime(ap_d[CLINIC_ID][1], '%Y-%m-%d')
MOTIVE_ATT = ap_d[CLINIC_ID][2]

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
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('Peoples Check 5 Start {0}'.format(localtime))
    
    sout = "clinic_id: {0} database: {1}:{2}".format(CLINIC_ID, HOST, DB)
    log.info( sout )

    dbc = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    
    clinic_name = dbc.name
    sout = "clinic_name: {0}".format(clinic_name.encode("utf-8"))
    log.info( sout )

    s_sql = s_sqlt1.format(CLINIC_ID)
    cursor = dbc.con.cursor()
    cursor.execute(s_sql)
    results = cursor.fetchall() 
    
    if results is None:
        r_count = 0
    else:
        r_count = len(results)

    sout = "Clinic has got {0} records in the area_peoples table".format(r_count)
    log.info( sout )

    dbcw = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    conw = dbcw.con
    curw = conw.cursor()
    
    counta  = 0
    countu  = 0
    countu1 = 0
    
    for rec in results:
        
        counta += 1
        
        area_people_id = rec[0]
        area_id_fk     = rec[1]
        date_beg       = rec[2]
        date_end       = rec[3] 
        motive_att     = rec[4]

        if counta % STEP == 0: 
            sout = "{0} area_people_id: {1}".format(counta, area_people_id)
            log.info( sout )    
  
        if date_beg is None:
            curw.execute(s_sqlt3,(DATE_BEG, MOTIVE_ATT, area_people_id))
            conw.commit()
            countu  += 1
        elif motive_att is None:
            curw.execute(s_sqlt31,(MOTIVE_ATT, area_people_id))
            conw.commit()
            countu1 += 1
            
    
    sout = "Totally for {0} peoples date_beg {1} has been assigned".format(countu, DATE_BEG)
    log.info( sout )    

    sout = "Totally for {0} peoples motive_attach_beg_id_fk {1} has been assigned".format(countu, MOTIVE_ATT)
    log.info( sout )    

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Peoples Check 5 Finish  '+localtime)
    
    dbc.close()
    dbcw.close()
    sys.exit(0)    
        
    
