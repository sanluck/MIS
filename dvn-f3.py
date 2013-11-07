#!/usr/bin/python
# -*- coding: utf-8 -*-
# dvn-f3.py - проверка двойников и отсутствие места рождения
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()
from insorglist import InsorgInfoList
insorgs = InsorgInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_dvnf3.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

STEP = 100

molist = [220011]

BIRTHPLACE = u"Алтайский край"

def get_cc_list(db, clinic_id):
    s_sqlt = """SELECT 
clinical_checkup_id, people_id_fk, date_stage_1, date_end_1
FROM clinical_checkups
WHERE (clinic_id_fk = {0}) AND (people_status_code = 3)
ORDER BY clinical_checkup_id;"""
    s_sql = s_sqlt.format(clinic_id)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    records = cursor.fetchall()
    ar = []
    for rec in records:
	cc_id     = rec[0]
	people_id = rec[1]
	dt1       = rec[2]
	dt2       = rec[3]
	ar.append([cc_id, people_id, dt1, dt2])
    
    return ar

def get_cc_plist(db, people_id):
    # get list of cc_ids for the people_id
    s_sqlt = """SELECT clinical_checkup_id
    FROM clinical_checkups
    WHERE people_id_fk = {0};"""
    s_sql  = s_sqlt.format(people_id)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    recs = cursor.fetchall()
    ar = []
    for rec in recs:
	cc_id = rec[0]
	ar.append(cc_id)
	
    return ar

def person_in_cc(db, people_id):
    # check if clinical_checkups table already has got a record with current people_id
    s_sqlt = "SELECT * FROM clinical_checkups WHERE people_id_fk = {0};"
    s_sql  = s_sqlt.format(people_id)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    rec = cursor.fetchone()
    if rec == None:
        return False
    else:
        return True
   
def pclinic(clinic_id, mcod):
    from dbmis_connect2 import DBMIS
    from dbmysql_connect import DBMY
    from PatientInfo2 import PatientInfo2
        
    import time
    
    birthplace_set = BIRTHPLACE.encode('cp1251')

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    
    
    dbc = DBMIS(clinic_id)
    cname = dbc.name.encode('utf-8')

    sout = "clinic_id: {0} cod_mo: {2}\nclinic_name: {1} ".format(clinic_id, cname, mcod)
    log.info(sout)
    log.info('Check for double patient Start {0}'.format(localtime))
    
    ppp = get_cc_list(dbc, clinic_id)
    
    sout = "We have got {0} DVN cases for the clinic totally".format(len(ppp))
    log.info(sout)

    p_obj = PatientInfo2()


    wrong_clinic = 0
    wrong_insorg = 0
    ncount = 0
    dbc2 = DBMIS(clinic_id)
    cur2 = dbc2.con.cursor()
    dbmy = DBMY()

    cc_id_d_number = 0
    set_bplace_number = 0
    set_doc_type_number = 0
    set_doc_number = 0
    
    for prec in ppp:
        ncount += 1

	cc_id     = prec[0]
	people_id = prec[1]
	dt1       = prec[2]
	dt2       = prec[3]

        p_obj.initFromDb(dbc, people_id)

        if ncount % STEP == 0:
            sout = " {0} people_id: {1} cc_id: {2}".format(ncount, people_id, cc_id)
            log.info(sout)
	
	
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


	lname   = p_obj.lname
	fname   = p_obj.fname
	mname   = p_obj.mname
	bd      = p_obj.birthday
	
	p_ids = dbc.get_p_ids(lname, fname, mname, bd)
	
	if len(p_ids) > 1:
	    for p_id in p_ids:
		if p_id == people_id: continue
		cc_plist = get_cc_plist(dbc, p_id)
		for cc_id_d in cc_plist:
		    
		    cc_id_d_number += 1

		    s_sqlt = """DELETE FROM
		    clinical_checkup_results
		    WHERE clinical_checkup_id_fk = {0};"""
		    s_sql = s_sqlt.format(cc_id_d)
		    try:
			dbc2.con.execute_immediate(s_sql)
			dbc2.con.commit()
		    except:
			sout = "Delete from clinical_checkup_results error. cc_id: {0}".format(cc_id_d)
			log.warn( sout )
		    
		    s_sqlt = """DELETE FROM
		    clinical_checkups
		    WHERE clinical_checkup_id = {0};"""
		    s_sql = s_sqlt.format(cc_id_d)
		    try:
			dbc2.con.execute_immediate(s_sql)
			dbc2.con.commit()
		    except:
			sout = "Delete from clinical_checkups error. cc_id: {0}".format(cc_id_d)
			log.warn( sout )


    sout = "Deleted clinical checkups number: {0}".format(cc_id_d_number)
    log.info(sout)

    sout = "Set birthplace number: {0}".format(set_bplace_number)
    log.info(sout)

    sout = "Set doc_type number: {0}".format(set_doc_type_number)
    log.info(sout)

    sout = "Set doc number: {0}".format(set_doc_number)
    log.info(sout)
    
    dbc.close()
    dbc2.close()
    dbmy.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Check for double patient Finish  '+localtime)


if __name__ == "__main__":
    
    import os, shutil    

    log.info("======================= DVN-F3 ===========================================")
    

#    for clinic_id in clist:
#        try:
#            mcod = modb.moCodeByMisId(clinic_id)
#            sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod) 
#            log.debug(sout)
#        except:
#            sout = "Have not got MO Code for clinic_id {0}".format(clinic_id)
#            log.warn(sout)
#            mcod = 0
#
#        pclinic(clinic_id, mcod)    

    for mcod in molist:
        try:
            mo = modb[mcod]
            clinic_id = mo.mis_code
            sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod) 
            log.info(sout)
        except:
            sout = "Clinic with MO Code {0} was not found".format(mcod)
            log.warn(sout)
            continue
            

        pclinic(clinic_id, mcod)

    sys.exit(0)
