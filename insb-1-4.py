#!/usr/bin/python
# -*- coding: utf-8 -*-
# insb-1-4.py - обработка ответов из ТФОМС на ЗСП
#               (MySQL: mis.tfoms_peoples)
#               приписываем
#                  - tfoms_verification_status_id_fk,
#                  - tfoms_verification_date
#               в DBMIS (area_peoples)
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

SQLT_TPR = """SELECT id, people_id, clinic_id, lpu_tfoms
FROM tfoms_peoples 
WHERE date_verify is Null;"""

SQLT_TPU = """UPDATE tfoms_peoples
SET
date_verify = %s,
tfoms_verification_status = %s
WHERE
id = %s;"""

CLINIC_ID = 22
HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

SQLT_APR = """SELECT 
ap.area_people_id, ap.area_id_fk, ap.motive_attach_beg_id_fk, 
ar.clinic_area_id_fk, 
ca.speciality_id_fk, ca.basic_speciality
FROM area_peoples ap
LEFT JOIN areas ar ON ap.area_id_fk = ar.area_id
LEFT JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
WHERE ap.people_id_fk = ?
AND ap.date_end is Null
AND ca.clinic_id_fk = ?
AND ca.basic_speciality = 1;"""

SQLT_APU = """UPDATE area_peoples
SET
tfoms_verification_status_id_fk = ?,
tfoms_verification_date = ?
WHERE
area_people_id = ?;"""


if __name__ == "__main__":
    
    import os, shutil
    import time
    from dbmysql_connect import DBMY
    from dbmis_connect2 import DBMIS

    import datetime    

    dnow = datetime.datetime.now()
    sdnow = "%04d-%02d-%02d" % (dnow.year, dnow.month, dnow.day)
    

    log.info("======================= INSB-1-4 ===========================================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Set TFOMS_VERIFIVATION_STATUS. Start {0}'.format(localtime))
    
    dbmy = DBMY()
    curr = dbmy.con.cursor()
    curw = dbmy.con.cursor()
    
    curr.execute(SQLT_TPR)
    results = curr.fetchall()

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    dbc = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    dbc_curr = dbc.con.cursor()
    dbc_curw = dbc.con.cursor()

    nall = 0
    nfound = 0
    for rec in results:
        nall += 1
        _id  = rec[0]
        p_id = rec[1]
        c_id = rec[2]
        c_tf = rec[3]
	if c_tf is None:
	    nfound += 1
	    tfoms_v_status = 4
	elif c_id == c_tf:
	    tfoms_v_status = 1
	else:
	    tfoms_v_status = 2
	
	if nall % STEP == 0:
	    sout = " {0}: people_id: {1} clinic_id: {2} tfoms_v_status: {3}".format(nall, p_id, c_id, tfoms_v_status)
	    log.info(sout)

	dbc_curr.execute(SQLT_APR, (p_id, c_id, ))
	ap_recs = dbc_curr.fetchall()
	for ap_rec in ap_recs:
	    ap_id = ap_rec[0]
	    dbc_curw.execute(SQLT_APU, (tfoms_v_status, dnow, ap_id, ))
	    dbc.con.commit()
	    
	curw.execute(SQLT_TPU,( sdnow, tfoms_v_status, _id))
	dbmy.con.commit()
	
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Set TFOMS_VERIFIVATION_STATUS. Finish  '+localtime)  
    
    sout = "Totally {0} records have been processed".format(nall)
    log.info(sout)

    sout = " {0} people_id have not got LPU_TFOMS".format(nfound)
    log.info(sout)

    dbc.close()
    dbmy.close()
    sys.exit(0)    
        