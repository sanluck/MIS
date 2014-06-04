#!/usr/bin/python
# -*- coding: utf-8 -*-
# em-3g.py - выборка пациентов с заданными диагнозами
#               по списку ЛПУ
#               за указанный период времени
#               отображение по дням на графиках
#

import logging
import sys, codecs

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_em3g.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

CLINIC_ID = 22
HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

d_begin = "2014-05-01"
d_end   = "2014-06-30"

STEP = 1000

D_LIST = ['A01%', 'A02%', 'A03%', 'A04%', 'A08%', 'A09%', 'B15%', 'J03%', 'J06%']
C_LIST = [132, 106, 139, 149, 150, 162, 217, 171, 202, 173, 127, 181]

F_PATH = "./EM3"
F_NAME = "em3.xls"

s_sqlt1 = """SELECT 
t.ticket_id, t.people_id_fk, t.clinic_id_fk, t.visit_date,
td.diagnosis_id_fk, td.visit_type_id_fk, td.disease_type_id_fk, td.diagnosis_state_id_fk
FROM tickets t
LEFT JOIN ticket_diagnosis td ON t.ticket_id = td.ticket_id_fk
WHERE  t.visit_date >= ?
AND t.visit_date <= ?
AND td.line = 1
AND td.diagnosis_id_fk LIKE ?
AND t.clinic_id_fk = ?
ORDER BY t.visit_date;"""

if __name__ == "__main__":
    import time
    from datetime import datetime
    from dbmis_connect2 import DBMIS
    import xlwt
    import matplotlib.pyplot as plt
 
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('EM 3 Start {0}'.format(localtime))

    clist = C_LIST
    nclist = len(clist)
    sout = "Totally {0} clinics will be selected".format(nclist)
    log.info(sout)
 
    sout = "database: {0}:{1}".format(HOST, DB)
    log.info( sout )

    wb = xlwt.Workbook(encoding='cp1251')
    ws = wb.add_sheet('Analysis')
 
    sout = "Begin Date: {0}".format(d_begin)
    log.info( sout )
    ws.write(0,0,sout)
    
    sout = "End   Date: {0}".format(d_end)
    log.info( sout )    
    ws.write(1,0,sout)

    row = 3

    col = 1
    for ddd in D_LIST:
	col += 1
	ws.write(row,col,ddd)

    NNN = 0
    for clinic_id in clist:
	row += 1
	dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
	cname = dbc.name.encode('utf-8')
	sout = "clinic_id: {0} clinic_name: {1} ".format(clinic_id, cname)
	log.info(sout)
	ws.write(row,0,clinic_id)
	ws.write(row,1,dbc.name)
	
	curr = dbc.con.cursor()
	nnn = 0
	col = 1
	for ddd in D_LIST:
	    col += 1
	    curr.execute(s_sqlt1, (d_begin, d_end, ddd, clinic_id, ))
	    recs = curr.fetchall()
	    nddd = len(recs)
	    sout = "Count[{0}]: {1}".format(ddd, nddd)
	    log.info(sout)
	    ws.write(row,col,nddd)
	    arr_x = []
	    arr_y = []
	    d_count = 0
	    visit_date0 = None
	    for rec in recs:
		visit_date = rec[3]
		if visit_date0 is None:
		    visit_date0 = visit_date
		    d_count += 1
		elif visit_date == visit_date0:
		    d_count += 1
		else:
		    month = visit_date0.month
		    day = visit_date0.day
		    n_date = month+day*0.01
		    arr_x.append(n_date)
		    arr_y.append(d_count)
		    d_count = 0
		    visit_date0 = visit_date
	    
	    if nddd > 0:
		fig = plt.figure(figsize=(9, 7))
		plt.plot(arr_x, arr_y)
		plt.ylabel(ddd)
		plt.xlabel('Date')
		f_name = F_PATH + "/" + "em3-{0}-{1}.png".format(clinic_id, ddd)
		plt.savefig(f_name)
	    
	    nnn += nddd
	sout = "Subtotal: {0}".format(nnn)
	log.info(sout)
	col += 1
	ws.write(row,col,nnn)
	NNN += nnn
	dbc.close()
	
    sout = "TOTAL: {0}".format(NNN)
    log.info(sout)
    row += 1
    ws.write(row,col,NNN)
    f_fname = F_PATH + "/" + F_NAME
    wb.save(f_fname)
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('EM 3 Finish  '+localtime)    
    
    sys.exit(0)