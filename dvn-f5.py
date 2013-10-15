#!/usr/bin/python
# -*- coding: utf-8 -*-
# dvn-f5.py - process all MO in the vt_done table
# check patients and check passport and birthplace
# set if None
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()
from insorglist import InsorgInfoList
insorgs = InsorgInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_dvnf5.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

REGISTER_MO = True

STEP = 100

BIRTHPLACE = u"Алтайский край"

def get_molist(db):
# get mo_list from vt_done table


    s_sqlt = """SELECT 
    id, mcod, clinic_id
    FROM vt_done
    WHERE pdone is Null;"""

    s_sql  = s_sqlt
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    recs = cursor.fetchall()
    ar = []
    for rec in recs:
	_id       = rec[0]
	mcod      = rec[1]
	clinic_id = rec[2]
	ar.append([_id, mcod, clinic_id])
	
    return ar

def get_plist(db, clinic_id):
    s_sqlt = """SELECT
    clinical_checkup_id, people_id_fk
    FROM clinical_checkups
    WHERE (clinic_id_fk = {0})
    AND (date_end_1 is Not Null)
    AND (people_status_code in (3,6));"""
    s_sql = s_sqlt.format(clinic_id)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    recs = cursor.fetchall()
    ar = []
    for rec in recs:
	cc_id     = rec[0]
	people_id = rec[1]
	ar.append([cc_id, people_id])
	
    return ar

def pmo(clinic_id):
    from dbmis_connect2 import DBMIS
    from PatientInfo2 import PatientInfo2
        
    import time
    
    birthplace_set = BIRTHPLACE.encode('cp1251')

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('Check documents and birthplace Start {0}'.format(localtime))
    
    p_obj = PatientInfo2()

    dbc = DBMIS(clinic_id)
    cname = dbc.name.encode('utf-8')

    sout = "clinic_id: {0} clinic_name: {1} ".format(clinic_id, cname)
    log.info(sout)

    ppp = get_plist(dbc, clinic_id)
    
    sout = "Totally {0} lines to be processed".format(len(ppp))
    log.info(sout)

    ncount = 0
    dbc2 = DBMIS(clinic_id)
    cur2 = dbc2.con.cursor()
    dbmy = DBMY()

    set_bplace_number   = 0
    set_doc_type_number = 0
    set_doc_number      = 0
    
    for prec in ppp:
        ncount += 1
	cc_id     = prec[0]
        people_id = prec[1]

        if ncount % STEP == 0:
            sout = " {0} people_id: {1}".format(ncount, people_id)
            log.info(sout)
	
	p_obj.initFromDb(dbc, people_id)

	bplace  = p_obj.birthplace
	if (bplace is None) or len(bplace) == 0:
	    s_sqlt = """UPDATE peoples
	    SET birthplace = '{0}'
	    WHERE people_id = {1};"""
	    
	    s_sql = s_sqlt.format(birthplace_set, people_id)
	    
	    dbc2.con.execute_immediate(s_sql)
	    dbc2.con.commit()
	    
	    set_bplace_number += 1

        doc_id     = p_obj.document_type_id_fk
        doc_series = p_obj.document_series
        doc_number = p_obj.document_number

	if (doc_id is None):
	    s_sqlt = """UPDATE peoples
	    SET document_type_id_fk = 14
	    WHERE people_id = {0};"""
	    
	    s_sql = s_sqlt.format(people_id)
	    
	    dbc2.con.execute_immediate(s_sql)
	    dbc2.con.commit()
	    set_doc_type_number += 1

	if (doc_series is None) or (doc_number is None):
	    s_sqlt = """UPDATE peoples
	    SET 
	    document_series = '01 01',
	    document_number = '111111'
	    WHERE people_id = {0};"""
	    
	    s_sql = s_sqlt.format(people_id)
	    
	    dbc2.con.execute_immediate(s_sql)
	    dbc2.con.commit()
	    set_doc_number += 1

    sout = "Set birthplace number: {0}".format(set_bplace_number)
    log.info(sout)

    sout = "Set doc_type number: {0}".format(set_doc_type_number)
    log.info(sout)

    sout = "Set doc number: {0}".format(set_doc_number)
    log.info(sout)

    dbc.close()
    dbc2.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Check documents and birthplace Finish  '+localtime)

def register_vt_pdone(db, _id):
    import datetime    

    dnow = datetime.datetime.now()
    sdnow = str(dnow)
    s_sqlt = """UPDATE vt_done
    SET pdone = '{0}'
    WHERE id = {1};"""

    s_sql = s_sqlt.format(sdnow, _id)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    db.con.commit()


if __name__ == "__main__":
    
    import os, shutil    
    from dbmysql_connect import DBMY

    log.info("======================= DVN-F5 ===========================================")

    
    dbmy2 = DBMY()

    molist = get_molist(dbmy2)
    n_mo = len(molist)
    sout = "Totally {0} MO to be processed".format(n_mo)
    log.info( sout )

    
    for mo_ar in molist:
	_id       = mo_ar[0]
	mcod      = mo_ar[1]
	clinic_id = mo_ar[2]
   
	try:
	    mo = modb[mcod]
	    clinic_id = mo.mis_code
	    sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod) 
	    log.info(sout)
	except:
	    sout = "Clinic not found for mcod = {0}".format(s_mcod)
	    log.warn(sout)
	    continue

	pmo(clinic_id)
	if REGISTER_MO: register_vt_pdone(dbmy2, _id)
    
    dbmy2.close()
    sys.exit(0)
