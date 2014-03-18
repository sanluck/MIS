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

D_LIST = ["B35%", "B36%", "B37.2%"]

F_PATH    = "./ANALYSIS"
F_NAME    = "analysis2.xls"
F_DS_MAIN = "ds_main_list.xls"
F_DS_PLUS = "ds_plus_list.xls"

STEP = 1000

s_sqlp1 = """SELECT p.people_id,
p.lname, p.fname, p.mname, p.birthday,
p.p_payment_type_id_fk, p.medical_insurance_region_id_fk, p.insorg_id,
p.social_status_id_fk, p.territory_id_fk,
p.addr_jure_region_code, p.addr_jure_area_code, p.addr_jure_area_name,
p.addr_jure_town_code, p.addr_jure_town_name,
p.birthplace,
p.document_type_id_fk, p.document_series, p.document_number,
p.citizenship,
p.sex, p.work_place,
p.addr_fact_region_code, p.addr_fact_area_code, p.addr_fact_area_name, p.addr_fact_area_socr,
p.addr_fact_town_code, p.addr_fact_town_name, p.addr_fact_town_socr,
p.addr_fact_country_code, p.addr_fact_country_name, p.addr_fact_country_socr
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

def get_ds_main_list():
    import xlrd
    
    f_fname = F_PATH + "/" + F_DS_MAIN
    sout = "Get ds_main_list from {0}".format(f_fname)
    log.info(sout)
    
    workbook = xlrd.open_workbook(f_fname)
    
    worksheets = workbook.sheet_names()
    wshn0 = worksheets[0]
    worksheet = workbook.sheet_by_name(wshn0)

    num_rows = worksheet.nrows - 1
    arr = []
    
    curr_row = 0
    while curr_row <= num_rows:
	# Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
	c1_type = worksheet.cell_type(curr_row, 0)
	if c1_type != 1: continue
	ds = worksheet.cell_value(curr_row, 0)
	arr.append(ds)
	curr_row += 1
    
    l_arr = len(arr)
    sout = "Totally {0} main diagnosis have been imported".format(l_arr)
    log.info(sout)
    return arr

def get_ds_plus_list():
    import xlrd
    
    f_fname = F_PATH + "/" + F_DS_PLUS
    sout = "Get ds_plus_list from {0}".format(f_fname)
    log.info(sout)
    
    workbook = xlrd.open_workbook(f_fname)
    
    worksheets = workbook.sheet_names()
    wshn0 = worksheets[0]
    worksheet = workbook.sheet_by_name(wshn0)

    num_rows = worksheet.nrows - 1
    arr = []
    
    curr_row = 0
    while curr_row <= num_rows:
	# Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
	c1_type = worksheet.cell_type(curr_row, 0)
	if c1_type != 1: continue
	ds1 = worksheet.cell_value(curr_row, 0)
	ds2 = worksheet.cell_value(curr_row, 1)
	arr.append([ds1, ds2])
	curr_row += 1
    
    l_arr = len(arr)
    sout = "Totally {0} associated diagnosis intervals have been imported".format(l_arr)
    log.info(sout)
    return arr
    

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
    
    ds_main_list = get_ds_main_list()
    ds_plus_list = get_ds_plus_list()
 
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

    tmgz_count = 0
    tid_arr = []

    f_fname = F_PATH + "/" + F_NAME    
    sout = "Output file: {0}".format(f_fname)
    log.info(sout)
    
    wb = xlwt.Workbook(encoding='cp1251')
    ws = wb.add_sheet('Analysis')
    
    row = 3

    ws.write(row,0,u'people_id')
    ws.write(row,1,u'FIO')
    ws.write(row,2,u'BD')
    ws.write(row,3,u'SEX')
    ws.write(row,4,u'WORK_PLACE')
    ws.write(row,5,u'ADRR FACT')
    
    col = 6
    for ds_main in ds_main_list:
	ws.write(row,col,ds_main)
	col += 1
	for n_month in range(1,13):
	    ws.write(row,col,n_month)
	    col += 1
	    
    for ds_plus in ds_plus_list:
	ds1 = ds_plus[0]
	ds2 = ds_plus[1]
	s_ds = u"(" + ds1 + "-" + ds2 + u")"
	ws.write(row,col,s_ds)
	col += 1

    row += 1

    s_sql = "SELECT * FROM mis.a$tickets ORDER BY people_id;"
    curm.execute(s_sql)
    recsm = curm.fetchall()

    t_count  = 0
    p_count  = 0
    p_id0    = 0

    dsm_list  = []
    dsmd_list = []
    dsmm_list = []
    dsp_list  = []

    people = PEOPLE()
    
    for recm in recsm:
	t_count += 1
	
	_id     = recm[0]
	t_id    = recm[1]
	p_id    = recm[2]
	c_id    = recm[3]
	v_date  = recm[4]
	v_month = v_date.month
	d_id    = recm[5]
	v_type  = recm[6]
	
	if t_count % STEP == 0: 
	    sout = "{0} {1} {2} {3}".format(t_count, t_id, p_id, c_id)
	    log.info( sout )	

	if c_id not in clist: continue
	
	if p_id <> p_id0:
	    if (p_id0 <> 0):
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
		ws.write(row,3,people.sex)
		ws.write(row,4,people.work_place)
		ws.write(row,5,people.addr_fact)

		col = 6
		for ds_main in ds_main_list:
		    
		    i_out = 0
		    for dsm in dsm_list:
			if dsm == ds_main:
			    i_out = 1
			    break
		    ws.write(row,col,i_out)
		    col += 1
		    
		    for n_month in range(1,13):
			i_out = 0
			iii = 0
			for dsm in dsm_list:
			    dsmm = dsmm_list[iii]
			    iii += 1
			    if dsm != ds_main: continue
			    if dsmm == n_month: i_out = 1

			ws.write(row,col,i_out)
			col += 1
    
		curc.execute(s_sqld_all,(p_id0,))
		reccs = curc.fetchall()
		dsp_list = []
		for recc in reccs:
		    ds  = recc[2]
		    if ds is None: continue
		    dsp_list.append(ds)
		

		for ds_plus in ds_plus_list:
		    ds1 = ds_plus[0]
		    ds2 = ds_plus[1]
		    i_out = 0
		    
		    for dsp in dsp_list:
			dsp2 = dsp[:3]
			if (dsp2 >= ds1) and (dsp2 <= ds2): i_out = 1
		    
		    ws.write(row,col,i_out)
		    col += 1
		

	    p_id0 = p_id
	    curc.execute(s_sqlp1,(p_id,))
	    recc = curc.fetchone()
	    if recc is None:
		sout = "not found people_id: {0}".format(p_id)
		log.warn( sout )
		continue
		
	    people.initFromRec(recc)
	    dsm_list  = []
	    dsmd_list = []
	    dsmm_list = []
	
	dsm_list.append(d_id.strip())
	dsmd_list.append(v_date)
	dsmm_list.append(v_month)
	

    wb.save(f_fname)
    
    sout = "Totally {0} tickets, {1} peoples have been processed".format(t_count, p_count)
    log.info( sout )
    
    dbc.close()
    dbm.close()
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Select 2 Finish  '+localtime)    
    
    sys.exit(0)