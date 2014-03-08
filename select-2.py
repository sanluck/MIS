#!/usr/bin/python
# -*- coding: utf-8 -*-
# select-2.py - анализ выбранных на предидущем этапе посещений (mis.a$tickets)
#

import logging
import sys, codecs

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_select2.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

CLINIC_ID = 22
HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

MGZ  = 0

d_begin = "2013-01-01"
d_end   = "2013-12-31"

d_int = [["2013-01-01", "2013-03-31"],["2013-04-01", "2013-06-30"],["2013-07-01", "2013-09-30"],["2013-10-01", "2013-12-31"]]

D_LIST = ["B35%", "B36%", "B37.2%"]

IGNORE_LIST = ["B35", "B36", "B37", "A00", "A01", "A02", "A03", "A04", "A05", "A06", "A07", "A08", "A09", "J00", "J01", "J02", "J03", "J04", "J05", "J06", "J07", "J08", "J09", "J10", "J11", "J12", "J13", "J14", "J15", "J16", "J17", "J18", "J19", "J20", "J21", "J22"]

F_PATH = "./ANALYSIS"
F_NAME = "analysis2.xls"

STEP = 1000

s_sqlp1 = """SELECT p.people_id,
p.lname, p.fname, p.mname, p.birthday,
p.p_payment_type_id_fk, p.medical_insurance_region_id_fk, p.insorg_id,
p.social_status_id_fk, p.territory_id_fk,
p.addr_jure_region_code, p.addr_jure_area_code, p.addr_jure_area_name,
p.addr_jure_town_code, p.addr_jure_town_name,
p.birthplace,
p.document_type_id_fk, p.document_series, p.document_number,
p.citizenship
FROM peoples p
WHERE p.people_id = ?;"""

s_sqld = """SELECT diagnosis_id_fk
FROM ticket_diagnosis
WHERE ticket_id_fk = ?
AND line <> 1;"""

s_sqld_all = """SELECT t.ticket_id, td.line, td.diagnosis_id_fk
FROM tickets t
LEFT JOIN ticket_diagnosis td ON t.ticket_id = td.ticket_id_fk
WHERE t.people_id_fk = ?;"""

def get_clist(mgz = MGZ):
    from constants import CMGZ_List
    
    db2 = CMGZ_List()
    db2.get_from_xlsx('cmgz.xlsx')

    n_lpu = len(db2.idxByNumber)
    n_mgz = 0
    clist = {}
    for cmgz in db2.idxByNumber:
	clinic_id = cmgz.clinic_id
	mgz_code  = cmgz.mgz_code
	if mgz_code == mgz: 
	    n_mgz += 1
	    clist[clinic_id] = [0,0,0,0]
	    
    sout = "MGZ {0} has got {1} clinics from {2}".format(mgz, n_mgz, n_lpu)
    log.info( sout )

    return clist

if __name__ == "__main__":
    import time
    import xlwt
    from datetime import datetime
    from dbmis_connect2 import DBMIS
    from dbmysql_connect import DBMY
    from people import PEOPLE
 
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('Select 2 Start {0}'.format(localtime))

    clist = get_clist(MGZ)
 
    dbc = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    curc = dbc.con.cursor()

    sout = "database: {0}:{1}".format(HOST, DB)
    log.info( sout )


    dbm = DBMY()
    curm = dbm.con.cursor()

    sout = "Begin Date: {0}".format(d_begin)
    log.info( sout )
    sout = "End   Date: {0}".format(d_end)
    log.info( sout )    

    w_count = 0
    tmgz_count = 0
    tid_arr = []

    d00 = datetime.strptime(d_int[0][0], '%Y-%m-%d').date()
    d01 = datetime.strptime(d_int[0][1], '%Y-%m-%d').date()

    d10 = datetime.strptime(d_int[1][0], '%Y-%m-%d').date()
    d11 = datetime.strptime(d_int[1][1], '%Y-%m-%d').date()

    d20 = datetime.strptime(d_int[2][0], '%Y-%m-%d').date()
    d21 = datetime.strptime(d_int[2][1], '%Y-%m-%d').date()

    d30 = datetime.strptime(d_int[3][0], '%Y-%m-%d').date()
    d31 = datetime.strptime(d_int[3][1], '%Y-%m-%d').date()


    f_fname = F_PATH + "/" + F_NAME    
    sout = "Output file: {0}".format(f_fname)
    log.info(sout)
    
    wb = xlwt.Workbook(encoding='cp1251')
    ws = wb.add_sheet('Analysis')
    
    row = 3

    ws.write(row,0,u'people_id')
    ws.write(row,1,u'FIO')
    ws.write(row,2,u'BD')
    ws.write(row,3,u'I')
    ws.write(row,4,u'II')
    ws.write(row,5,u'III')
    ws.write(row,6,u'IV')
    ws.write(row,7,u'DS main')
    ws.write(row,8,u'DS plus')

    row += 1


    s_sql = "SELECT * FROM mis.a$tickets ORDER BY people_id;"
    curm.execute(s_sql)
    recsm = curm.fetchall()

    t_count  = 0
    p_count  = 0
    p_id0    = 0
    n1       = 0
    n2       = 0
    n3       = 0
    n4       = 0
    dsm_list = u""
    dsp_list = u""

    people = PEOPLE()
    
    for recm in recsm:
	t_count += 1
	
	_id    = recm[0]
	t_id   = recm[1]
	p_id   = recm[2]
	c_id   = recm[3]
	v_date = recm[4]
	d_id   = recm[5]
	v_type = recm[6]
	
	if t_count % STEP == 0: 
	    sout = "{0} {1} {2} {3}".format(t_count, t_id, p_id, c_id)
	    log.info( sout )	

	if c_id not in clist: continue
	
	if p_id <> p_id0:
	    if p_id0 <> 0:
		p_count += 1
		row += 1
		ws.write(row,0,p_id0)
		ws.write(row,1,people.f1io)
		bd = people.birthday
		if bd is None:
		    s_db = u""
		else:
		    s_bd = "%04d-%02d-%02d" % (bd.year, bd.month, bd.day)
		ws.write(row,2,s_bd)
		ws.write(row,3,n1)
		ws.write(row,4,n2)
		ws.write(row,5,n3)
		ws.write(row,6,n4)
		ws.write(row,7,dsm_list)

		curc.execute(s_sqld_all,(p_id0,))
		reccs = curc.fetchall()
		dsp_list == u""
		for recc in reccs:
		    ds  = recc[2]
		    if ds is None: continue
		    ds2 = ds[:2]
		    if ds2 in IGNORE_LIST: continue
		    if len(dsp_list) == 0:
			dsp_list = ds
		    else:
			dsp_list += u"; " + ds
		
		ws.write(row,8,dsp_list)
	    
	    p_id0 = p_id
	    curc.execute(s_sqlp1,(p_id,))
	    recc = curc.fetchone()
	    if recc is None:
		sout = "not found people_id: {0}".format(p_id)
		log.warn( sout )
		continue
		
	    people.initFromRec(recc)
	    n1       = 0
	    n2       = 0
	    n3       = 0
	    n4       = 0
	    dsm_list = u""
	    dsp_list = u""
	
	if len(dsm_list) == 0:
	    dsm_list = d_id
	else:
	    dsm_list += u", " + d_id
	
	if (v_date >= d00) and (v_date <= d01):
	    n1 += 1
	elif (v_date >= d10) and (v_date <= d11):
	    n2 += 1
	elif  (v_date >= d20) and (v_date <= d21):
	    n3 += 1
	elif  (v_date >= d30) and (v_date <= d31):
	    n4 += 1
	else:
	    w_count += 1

    wb.save(f_fname)
    
    sout = "Totally {0} tickets, {1} peoples have been processed".format(t_count, p_count)
    log.info( sout )
    
    sout = "Totally {0} tickets have got date outside of date intervals".format(w_count)
    log.info( sout )
    
    
    dbc.close()
    dbm.close()
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Select 2 Finish  '+localtime)    
    
    sys.exit(0)