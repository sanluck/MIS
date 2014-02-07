#!/usr/bin/python
# -*- coding: utf-8 -*-
# pcheck-8.py - поиск записей в таблице peoples
#               для пациентов клиники из таблицы mis.mira$peoples
#               с последующим заданием документа и полиса ОМС
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_pcheck8.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb.ctmed.ru"
DB   = "DBMIS"

clist = [220124]

cid_list = [95,98]

STEP = 1000

CID_LIST   = False # Use cid_lis (list of clinic_id)

s_sqlt1 = """UPDATE peoples
SET 
document_type_id_fk = ?,
document_series = ?,
document_number = ?
WHERE people_id = ?"""

s_sqlt2 = """UPDATE peoples
SET 
medical_insurance_series = ?,
medical_insurance_number = ?
WHERE people_id = ?"""

s_sqlt2 = """UPDATE peoples
SET 
medical_insurance_series = ?,
medical_insurance_number = ?,
is_new_policy = ?
WHERE people_id = ?"""


s_sqlt3 = """UPDATE mira$peoples
SET id_done = 1
WHERE people_id = %s;"""
    
def pclinic(clinic_id, mcod):
    from dbmysql_connect import DBMY
    from people import get_mira_peoples
    from people import PEOPLE, get_people
    from dbmis_connect2 import DBMIS
    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('Update OMS documents data. Start {0}'.format(localtime))

    dbmy = DBMY()
    sm_ar = get_mira_peoples(dbmy, mcod)
    
    if sm_ar is None:
	sout = "There are no data for mcod = {0}".format(mcod)
	log.info( sout )
	dbmy.close()
	return
    
    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
    if dbc.ogrn == None:
        CLINIC_OGRN = u""
    else:
        CLINIC_OGRN = dbc.ogrn

    cogrn = CLINIC_OGRN.encode('utf-8')
    cname = dbc.name.encode('utf-8')
    
    sout = "clinic_id: {0} cod_mo: {1} clinic_name: {2} clinic_ogrn: {3}".format(clinic_id, mcod, cname, cogrn)
    log.info(sout)

    l_sm  = len(sm_ar)
    sout = "{0} peoples to be processed".format(l_sm)
    log.info( sout )

    curm = dbmy.con.cursor()
    
    curr = dbc.con.cursor()
    curw = dbc.con.cursor()

    counta   = 0
    
    count_nf = 0


    for sm in sm_ar:

	counta  += 1
	
	mp_id   = sm.people_id
        u_lname  = sm.lname
        u_fname  = sm.fname
        u_mname  = sm.mname
        birthday = sm.birthday

        lname = u_lname.encode('utf-8')
        fname = u_fname.encode('utf-8')
        if u_mname is None:
            mname = ""
        else:
            mname = u_mname.encode('utf-8')

	d_type_id = sm.document_type_id
	d_series  = sm.document_series
	d_number  = sm.document_number

	dpfs      = sm.dpfs
	s_oms     = sm.s_oms
	n_oms     = sm.n_oms


        if counta % STEP == 0:
	    ss_oms = s_oms.encode('utf-8')
	    nn_oms = n_oms.encode('utf-8')
	    if d_series is not None:
		d_sss = d_series.encode('utf-8')
	    else:
		d_sss = 'None'
            sout = "{0} {1} {2} {3} {4} {5} {6} {7}".format(counta, lname, fname, mname, birthday, dpfs, ss_oms, nn_oms)
            log.info( sout )
	    
        
        pf_arr = get_people(curr, u_lname, u_fname, u_mname, birthday)
	
        if (pf_arr is None) or (len(pf_arr) == 0):
	    count_nf += 1
        else:
	    
            for pf in pf_arr:
                people_id = pf[0]
		
		if (dpfs is not None) and (n_oms is not None):
		    if dpfs in (2,3):
			is_new_policy = 1
		    else:
			is_new_policy = 0
			
		    try:
			curw.execute(s_sqlt2, (s_oms, n_oms, is_new_policy, people_id))
			dbc.con.commit()
		    except Exception, e:
			r_msg = 'Ошибка записи в DBMIS: {0} {1}'.format(sys.stderr, e)
			log.error( r_msg )
			sout = "people_id: {0} dpfs: {1} s_oms: {2} n_oms: {3}".format(people_id,dpfs, s_oms, n_oms)
			log.error( sout )
		
                if counta % STEP == 0: 
                    sout = "people_id: {0}".format(people_id)
          
                    log.info( sout )    
	
	curm.execute(s_sqlt3, (mp_id))
	dbmy.con.commit()
	
    sout = "Totally {0} peoples have been checked".format(counta)
    log.info( sout )    
    
    sout = "{0} peoples have not been found in the DBMIS".format(count_nf)
    log.info( sout )

    dbmy.close()    
    dbc.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Update OMS documents data. Finish  '+localtime)



if __name__ == "__main__":

    import sys    

    log.info("======================= PCHECK-8 ===========================================")

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info( sout )

    if CID_LIST:
	for clinic_id in cid_list:
	    try:
		mcod = modb.moCodeByMisId(clinic_id)
		sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod) 
		log.debug(sout)
	    except:
		sout = "Have not got clinic for clinic_id {0}".format(clinic_id)
		log.warn(sout)
		mcod = 0
		continue
	    
	    pclinic(clinic_id, mcod)
    else:
	for mcod in clist:
	    try:
		mo = modb[mcod]
		clinic_id = mo.mis_code
		sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod) 
		log.debug(sout)
	    except:
		sout = "Have not got clinic for MO Code {0}".format(mcod)
		log.warn(sout)
		clinic_id = 0
		continue
	    
	    pclinic(clinic_id, mcod)
	
    sys.exit(0)
