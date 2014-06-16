#!/usr/bin/python
# -*- coding: utf-8 -*-
# em-2b.py - построение графика средне-месячной заболеваемости
#               по результатам выборки (em$tickets)
#               за указанный период времени
#

import logging
import sys, codecs

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_em2b.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

ds_begin = "2012-01-01"
ds_end   = "2012-12-31"
d_begin  = "2013-01-01"
d_end    = "2013-12-31"
labels = ('2012', '2013')

n_peoples = 621700
factor = 100000.0/n_peoples

STEP = 1000

s_sql1 = """SELECT count(id), MONTH(visit_date) FROM em$tickets 
WHERE visit_date >= %s AND visit_date <= %s
GROUP BY MONTH(visit_date);"""


if __name__ == "__main__":
    import time
    from datetime import datetime
    import matplotlib.pyplot as plt
    from dbmysql_connect import DBMY
 
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('EM 2b Start {0}'.format(localtime))

    sout = "Begin Date: {0}".format(d_begin)
    log.info( sout )
    sout = "End   Date: {0}".format(d_end)
    log.info( sout )    

    dbmy = DBMY()
    curr = dbmy.con.cursor()

    # Расчет среднего показателя за указанный период
    curr.execute(s_sql1, (ds_begin, ds_end, ))
    recs = curr.fetchall()
    _sum = 0
    nnn = 0
    for rec in recs:
	nnn += 1
	n = rec[0]
	f = n*factor
	_sum += f
    
    IM = _sum/nnn
    
    arrs_x = []
    arrs_y = []
    for i in range(1,13):
	arrs_x.append(i)
	arrs_y.append(IM)
    
    curr.execute(s_sql1, (d_begin, d_end, ))

    recs = curr.fetchall()
    
    d_count = 0
    arr_x = []
    arr_y = []
    for rec in recs:
	d_count += 1
	n = rec[0]
	f = n*factor
	d = rec[1]
	arr_x.append(d)
	arr_y.append(f)
    
    sout = "Totally {0} dates have been selected".format(d_count)
    log.info( sout )
    
    dbmy.close()

    fig = plt.figure(figsize=(15, 7))
    plt.plot(arrs_x, arrs_y)
    plt.plot(arr_x, arr_y)
    plt.ylabel('I')
    plt.xlabel('Month')
    legend = plt.legend(labels, loc=(0.85, .1), labelspacing=0.1)
    plt.savefig('em-2b.png')
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('EM 2b Finish  '+localtime)    
    
    sys.exit(0)