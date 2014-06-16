#!/usr/bin/python
# -*- coding: utf-8 -*-
# em-2b.py - построение графика пороговой заболеваемости
#               по результатам выборки (em$tickets)
#               за указанный период времени
#

import logging
import sys, codecs

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_em2c.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

ysig_list = [2011, 2012, 2013]
d_begin   = "2014-01-01"
d_end     = "2014-05-31"

labels = ('2011-2013', '2014')

n_peoples = 621700
factor = 100000.0/n_peoples

STEP = 1000

s_sql1 = """SELECT count(id), MONTH(visit_date) FROM em$tickets 
WHERE visit_date >= %s AND visit_date <= %s
GROUP BY MONTH(visit_date);"""


if __name__ == "__main__":
    import time
    import math
    from datetime import datetime
    import matplotlib.pyplot as plt
    from dbmysql_connect import DBMY
 
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('EM 2c Start {0}'.format(localtime))

    sout = "Begin Date: {0}".format(d_begin)
    log.info( sout )
    sout = "End   Date: {0}".format(d_end)
    log.info( sout )    

    dbmy = DBMY()
    curr = dbmy.con.cursor()

    # Расчет средних показателей за указанные годы
    
    IY = {}
    for ysig in ysig_list:
	d1 = datetime(ysig,1,1)
	d2 = datetime(ysig,12,31)
	curr.execute(s_sql1, (d1, d2, ))
	recs = curr.fetchall()
	_sum = 0.0
	nnn = 0
	i_arr = []
	for rec in recs:
	    nnn += 1
	    n = rec[0]
	    f = n*factor
	    i_arr.append(f)
	    _sum += f
    
	IM = _sum/nnn
	IY[ysig] = [IM, i_arr]
    
    arrs_x = []
    arrs_y = []
    arrs_s = []
    for i in range(1,13):
	arrs_x.append(i)
	_sum = 0.0
	_sss = 0.0
	nnn = 0
	for ysig in ysig_list:
	    nnn += 1
	    Im = IY[ysig][0]
	    i_arr = IY[ysig][1]
	    f = i_arr[i-1]
	    delta = Im - f
	    _sum += f
	    _sss += delta*delta
	    
	arrs_y.append(_sum/nnn)
	arrs_s.append(math.sqrt(_sss/nnn))
    
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
    plt.errorbar(arrs_x, arrs_y, yerr = arrs_s, fmt = '-o')
    plt.plot(arr_x, arr_y)
    plt.ylabel('I')
    plt.xlabel('Month')
    legend = plt.legend(labels, loc=(0.80, .1), labelspacing=0.1)
    plt.savefig('em-2c.png')
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('EM 2c Finish  '+localtime)    
    
    sys.exit(0)