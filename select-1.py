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

D_LIST = ["B35%", "B36%", "B37.2%"]

OUT_PATH = "./ANALYSIS"

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
    clist = []
    for cmgz in db2.idxByNumber:
	clinic_id = cmgz.clinic_id
	mgz_code  = cmgz.mgz_code
	if mgz_code == mgz: 
	    n_mgz += 1
	    clist.append(clinic_id)
	    
    sout = "MGZ {0} has got {1} clinics from {2}".format(mgz, n_mgz, n_lpu)
    log.info( sout )

    return clist


if __name__ == "__main__":
    import time
    import datetime
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
    tmgz_count = 0
    tid_arr = []
    
    for rec in recs:
	t_count += 1
	
	t_id = rec[0]
	p_id = rec[1]
	c_id = rec[2]
	
	if t_count % STEP == 0: 
	    sout = "{0} {1} {2} {3}".format(t_count, t_id, p_id, c_id)
	    log.info( sout )	

	if c_id not in clist: continue
	
	
	tid_arr.append(t_id)
	tmgz_count += 1
    
    sout = "Totally {0} ticket of {1} have been chosen".format(tmgz_count, t_count)
    log.info( sout )
    
    dbc.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Select 1 Finish  '+localtime)    
    
    sys.exit(0)