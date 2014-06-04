#!/usr/bin/python
# -*- coding: utf-8 -*-
# em-1.py - выборка пациентов с заданными диагнозами
#               по списку ЛПУ
#               за указанный период времени
#               вывод в таблицу em$tickets
#

import logging
import sys, codecs

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_em1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

CLINIC_ID = 22
HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

MGZ  = 0

d_begin = "2011-01-01"
d_end   = "2011-12-31"
APPEND  = True

STEP = 1000

s_sqlt1 = """SELECT 
t.ticket_id, t.people_id_fk, t.clinic_id_fk, t.visit_date,
td.diagnosis_id_fk, td.visit_type_id_fk, td.disease_type_id_fk, td.diagnosis_state_id_fk
FROM tickets t
LEFT JOIN ticket_diagnosis td ON t.ticket_id = td.ticket_id_fk
WHERE  t.visit_date >= ?
AND t.visit_date <= ?
AND td.line = 1
AND (
(td.diagnosis_id_fk LIKE 'A08.0%') 
OR (td.diagnosis_id_fk LIKE 'A05.9%') 
OR (td.diagnosis_id_fk LIKE 'A08.4%')
OR (td.diagnosis_id_fk LIKE 'A09%')
OR (td.diagnosis_id_fk LIKE 'A77.2%')
OR ((td.diagnosis_id_fk >= 'J10') AND (td.diagnosis_id_fk <= 'J11.9'))
OR ((td.diagnosis_id_fk >= 'J12') AND (td.diagnosis_id_fk <= 'J16.9'))
OR (td.diagnosis_id_fk LIKE 'J18%')
)
ORDER BY t.visit_date;"""

s_sqli = """INSERT INTO em$tickets
(ticket_id, people_id, clinic_id, visit_date, diagnosis_id, visit_type_id, disease_type_id, diagnosis_state_id)
VALUES
(%s, %s, %s, %s, %s, %s, %s, %s);"""

if __name__ == "__main__":
    import time
    from datetime import datetime
    from dbmis_connect2 import DBMIS
    from dbmysql_connect import DBMY
    from em_clist import get_clist_from_xls
 
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('EM 1 Start {0}'.format(localtime))

    clist = get_clist_from_xls()
    nclist = len(clist)
    sout = "Totally {0} clinics will be selected".format(nclist)
    log.info(sout)
 
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


    dbmy = DBMY()
    curw = dbmy.con.cursor()
    if not APPEND:
    # clear em$tickets table
	s_sql = "TRUNCATE TABLE em$tickets;"
	curw.execute(s_sql)
    
    t_count = 0
    em_count = 0
    
    for rec in recs:
	t_count += 1
	
	t_id    = rec[0]
	p_id    = rec[1]
	c_id    = rec[2]
	v_date  = rec[3]
	d_id    = rec[4]
	v_type  = rec[5]
	d_type  = rec[6]
	d_state = rec[7]
	
	if t_count % STEP == 0: 
	    sout = "{0} {1} {2} {3}".format(t_count, t_id, p_id, c_id)
	    log.info( sout )	

	if c_id not in clist: continue
	
	
	curw.execute(s_sqli,(t_id, p_id, c_id, v_date, d_id, v_type, d_type, d_state))
	em_count += 1
    
    sout = "Totally {0} tickets of {1} have been chosen".format(em_count, t_count)
    log.info( sout )
    
    
    dbc.close()
    dbmy.close()
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('EM 1 Finish  '+localtime)    
    
    sys.exit(0)