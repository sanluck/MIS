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

FNAME  = "STAT{0}{1}.xls" # в ТФОМС на внесение изменений
R_PATH = "./INSB7STAT"


REGISTER_SOFF = False

SET_DATE_END = True
DATE_END = "2013-12-30"
MOTIVE_ATTACH_END_ID = 2
SQLT_SOFF = """UPDATE area_peoples
SET
date_end = ?,
motive_attach_end_id_fk = ?
WHERE area_people_id = ?;"""

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
    from people import get_people, get_pclinics
    import time
    import xlwt

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('Strike off the Register Procedure Start {0}'.format(localtime))

    sout = "database: {0}:{1}".format(HOST, DB)
    log.info( sout )

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
    cur2 = dbc.con.cursor()
    curw = dbc.con.cursor()

    stime   = time.strftime("%Y%m%d")
    fname   = FNAME.format(mcod, stime)
    f_fname = R_PATH  + "/" + fname
    sout = "Output to file: {0}".format(f_fname)
    log.info(sout)	

    wb = xlwt.Workbook(encoding='cp1251')
    ws = wb.add_sheet('INSB Statistics')
    
    row = 3

    ws.write(row,0,u'people_id')
    ws.write(row,1,u'#')
    ws.write(row,2,u'FIO')
    ws.write(row,3,u'BD')
    ws.write(row,4,u'INSORG_ID')
    ws.write(row,5,u'OMS SN')
    ws.write(row,6,u'ENP')
    ws.write(row,7,u'STAT CODE')
    ws.write(row,8,u'LPU COUNT')

    row = 5
    
    n_double = 0
    n_minus  = 0
    n_many   = 0
    n_many_n = 0
    n_one_n  = 0
    
    for p_mo in mo_ar:
	lname  = p_mo.lname
	fname  = p_mo.fname
	mname  = p_mo.mname
	bd     = p_mo.birthday
	oms_sn = p_mo.oms_sn
	enp    = p_mo.enp
	

	p_list = get_people(curr, lname, fname, mname, bd)
	
	n_pid = 0
	for p_rec in p_list:
	    n_pid += 1
	    if n_pid ==2: n_double += 1
	    p_id = p_rec[0]
	    fio  = p_rec[4]
	    
	    
	    pc_arr = get_pclinics(cur2, p_id)
	    l_pc_arr = len(pc_arr)
	    if (l_pc_arr == 1):
		p_clinic = pc_arr[0]
		p_clinic_id = p_clinic.clinic_id
		if p_clinic_id == clinic_id:
		    stat_code = 0
		else:
		    stat_code = 7
		    n_one_n  += 1
	    elif l_pc_arr == 0:
		stat_code = -1
	    else:
		stat_code = 9
		for p_clinic in pc_arr:
		    p_clinic_id = p_clinic.clinic_id
		    if p_clinic_id == clinic_id:
			stat_code = 8
			continue
		    else:
			area_people_id = p_clinic.area_people_id
			if SET_DATE_END:
			    curw.execute(SQLT_SOFF, (DATE_END, MOTIVE_ATTACH_END_ID, area_people_id, ))
			    dbc.con.commit()
			
	    
	    if stat_code == -1: n_minus += 1
	    if stat_code == 9: n_many_m += 1
	    if l_pc_arr > 1: n_many += 1
	    row += 1
	    
	    ws.write(row,0,p_id)
	    ws.write(row,1,n_pid)
	    ws.write(row,2,fio)
	    ws.write(row,3,bd)
	    ws.write(row,5,oms_sn)
	    ws.write(row,6,enp)
	    ws.write(row,7,stat_code)
	    ws.write(row,8,l_pc_arr)
	    
    wb.save(f_fname)
	    
    nnn = len(mo_ar)
    sout = "Totally {0} records have been processed".format(nnn)
    log.info(sout)
    sout = " {0} peoples have got double (and more) people_id".format(n_double)
    log.info(sout)
    sout = " {0} peoples have not got MO".format(n_minus)
    log.info(sout)
    sout = " {0} peoples have got many MO".format(n_many)
    log.info(sout)
    sout = " {0} peoples have got many MO, but not equal to current MO".format(n_many_n)
    log.info(sout)
    sout = " {0} peoples have got one MO, but not equal to current MO".format(n_one_n)
    log.info(sout)
    
    dbc.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('Strike off the Register Procedure Finish {0}'.format(localtime))
	

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
	
	do_clinic(clinic_id, mcod, mo_ar)
	if REGISTER_SOFF: register_soff(dbmy, mcod)

    dbmy.close()
    log.info("----------------------- INSB-7 FINISH ------------------------------------")
    sys.exit(0)
