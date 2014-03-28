#!/usr/bin/python
# -*- coding: utf-8 -*-
# insb-7.py - открепление от лишних участков 
#             по списку клиник из mis.mo_done.d_soof is Null
#             за основу берутся данные из mis.mo
#             
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insb7.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

STEP = 1000

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

def get_clist(db):
    
    s_sql = "SELECT DISTINCT mcod FROM mo_done WHERE d_soff is Null;"
    
    cur = db.con.cursor()
    cur.execute(s_sql)
    result = cur.fetchall()
    
    ar = []
    
    for rec in result:
	mcod = rec[0]
	ar.append(mcod)
	
    return ar

def register_soff(db, mcod):
    import datetime    

    dnow = datetime.datetime.now()
    sdnow = str(dnow)

    s_sql = """UPDATE mo_done
    SET d_soff = %s
    WHERE mcod = %s;"""
    
    cur = db.con.cursor()
    cur.execute(s_sql, (sdnow, mcod, ))

def do_clinic(clinic_id, mcod, mo_ar):
    from dbmis_connect2 import DBMIS
    from people import get_people
    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('Strike off the Register Procedure Start {0}'.format(localtime))

    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
    if dbc.ogrn == None:
        CLINIC_OGRN = u""
    else:
        CLINIC_OGRN = dbc.ogrn

    cogrn = CLINIC_OGRN.encode('utf-8')
    cname = dbc.name.encode('utf-8')
    
    sout = "clinic_id: {0} cod_mo: {1} clinic_name: {2} clinic_ogrn: {3}".format(clinic_id, mcod, cname, cogrn)
    log.info(sout)

    curr = dbc.con.cursor()
    
    for p_mo in mo_ar:
	lname = p_mo.lname
	fname = p_mo.fname
	mname = p_mo.mname
	bd    = p_mo.birthday

	p_list = get_people(curr, lname, fname, mname, bd)
	
	for p_rec in p_list:
	    p_id = p_rec[0]
	    
	

if __name__ == "__main__":
    
    import os    
    import time, datetime
    from dbmysql_connect import DBMY
    from people import get_mo_fromdb

    log.info("======================= INSB-7 START =====================================")
    
    dbmy = DBMY()
    clist = get_clist(dbmy)
    mcount = len(clist)
    sout = "Totally {0} MO to be processed".format(mcount)
    log.info( sout )
    for mcod in clist:
	try:
	    mo = modb[mcod]
	    clinic_id = mo.mis_code
	    sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod) 
	    log.info(sout)
	except:
	    sout = "Have not got clinic for MO Code {0}".format(mcod)
	    log.warn(sout)
	    clinic_id = 0
	    continue
	
	mo_ar = get_mo_fromdb(dbmy, mcod)
	l_ar  = len(mo_ar)
	sout  = "has got {0} MO lines".format(l_ar)
	log.info(sout)
	
	stime   = time.strftime("%Y%m%d")
	
	do_clinic(clinic_id, mcod)
	register_soff(dbmy, mcod)

    dbmy.close()
    log.info("----------------------- INSB-7 FINISH ------------------------------------")
    sys.exit(0)
