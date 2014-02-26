#!/usr/bin/python
# -*- coding: utf-8 -*-
# select-1.py - выборка пациентов с заданными диагнозами
#               по МГЗ
#               за указанный период времени
#               вывод в xls файл
#

import logging
import sys, codecs

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_select1.out'
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

F_PATH = "./ANALYSIS"
F_NAME = "analysis1.xls"

STEP = 1000

s_sqlt1 = """SELECT 
t.ticket_id, t.people_id_fk, t.clinic_id_fk, t.visit_date,
td.diagnosis_id_fk, visit_type_id_fk
FROM tickets t
LEFT JOIN ticket_diagnosis td ON t.ticket_id = td.ticket_id_fk
WHERE  t.visit_date >= ?
AND t.visit_date <= ?
AND td.line = 1
AND ((td.diagnosis_id_fk LIKE 'B35%') OR (td.diagnosis_id_fk LIKE 'B36%') OR (td.diagnosis_id_fk LIKE 'B37.2%'))
ORDER BY t.visit_date;"""

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

def write_clist(clist):
    import xlwt
    from constants import CMGZ_List
    
    db2 = CMGZ_List()
    db2.get_from_xlsx('cmgz.xlsx')
    
    f_fname = F_PATH + "/" + F_NAME    
    sout = "Output file: {0}".format(f_fname)
    log.info(sout)
    
    wb = xlwt.Workbook(encoding='cp1251')
    ws = wb.add_sheet('Analysis')
    
    row = 3

    ws.write(row,0,u'clinic_id')
    ws.write(row,1,u'clinic_name')
    ws.write(row,2,u'I')
    ws.write(row,3,u'II')
    ws.write(row,4,u'III')
    ws.write(row,5,u'IV')    

    row += 1

    for c_id in clist.keys():
	row += 1
	cmgz = db2[c_id]
	c_name = cmgz.clinic_name
	c0 = clist[c_id][0]
	c1 = clist[c_id][1]
	c2 = clist[c_id][2]
	c3 = clist[c_id][3]
	ws.write(row,0,c_id)
	ws.write(row,1,c_name)
	ws.write(row,2,c0)
	ws.write(row,3,c1)
	ws.write(row,4,c2)
	ws.write(row,5,c3)    
    
    wb.save(f_fname)

if __name__ == "__main__":
    import time
    from datetime import datetime
    from dbmis_connect2 import DBMIS
 
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('Select 1 Start {0}'.format(localtime))

    clist = get_clist(MGZ)
 
    dbc = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)

    sout = "database: {0}:{1}".format(HOST, DB)
    log.info( sout )
 
    sout = "Begin Date: {0}".format(d_begin)
    log.info( sout )
    sout = "End   Date: {0}".format(d_end)
    log.info( sout )    

    curr = dbc.con.cursor()
    curr.execute(s_sqlt1, (d_begin, d_end, ))
    recs = curr.fetchall()
    
    t_count = 0
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
    
    
    for rec in recs:
	t_count += 1
	
	t_id   = rec[0]
	p_id   = rec[1]
	c_id   = rec[2]
	v_date = rec[3]
	
	if t_count % STEP == 0: 
	    sout = "{0} {1} {2} {3}".format(t_count, t_id, p_id, c_id)
	    log.info( sout )	

	if c_id not in clist: continue
	
	
	tid_arr.append(t_id)
	tmgz_count += 1
	
	if (v_date >= d00) and (v_date <= d01):
	    clist[c_id][0] += 1
	elif (v_date >= d10) and (v_date <= d11):
	    clist[c_id][1] += 1
	elif  (v_date >= d20) and (v_date <= d21):
	    clist[c_id][2] += 1
	elif  (v_date >= d30) and (v_date <= d31):
	    clist[c_id][3] += 1
	else:
	    w_count += 1
    
    sout = "Totally {0} ticket of {1} have been chosen".format(tmgz_count, t_count)
    log.info( sout )
    
    sout = "Totally {0} tickets have got date outside of date intervals".format(w_count)
    log.info( sout )
    
    
    dbc.close()
    
    write_clist(clist)
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Select 1 Finish  '+localtime)    
    
    sys.exit(0)