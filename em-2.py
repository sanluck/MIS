#!/usr/bin/python
# -*- coding: utf-8 -*-
# em-2.py - построение графика заболеваемости
#               по результатам выборки (em$tickets)
#               за указанный период времени
#

import logging
import sys, codecs

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_em2.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

d_begin = "2013-01-01"
d_end   = "2013-12-31"
n_peoples = 621700
factor = 100000.0/n_peoples

STEP = 1000

s_sql1 = """SELECT count(id), visit_date FROM em$tickets 
WHERE visit_date >= %s AND visit_date <= %s
GROUP BY visit_date;"""


if __name__ == "__main__":
    import time
    from datetime import datetime
    import matplotlib.pyplot as plt
    from dbmysql_connect import DBMY
 
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('EM 2 Start {0}'.format(localtime))

    sout = "Begin Date: {0}".format(d_begin)
    log.info( sout )
    sout = "End   Date: {0}".format(d_end)
    log.info( sout )    

    dbmy = DBMY()
    curr = dbmy.con.cursor()
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
    plt.plot(arr_x, arr_y)
    plt.ylabel('I')
    plt.xlabel('Date')
    plt.savefig('em-2.png')
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('EM 2 Finish  '+localtime)    
    
    sys.exit(0)