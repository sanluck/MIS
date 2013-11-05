#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# rrd-1.py - get DBMIS monitoring results (dbmis-r1.py) from the MysQL
#            and plot it using rrdtool
#

import sys
import logging
import datetime

FNAME  = 'test.rrd'
GFNAME = 'test.png'

LOG_FILENAME = '_rrd1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

ddd = datetime.datetime.now()
ddd_d = ddd.day
ddd_m = ddd.month
ddd_y = ddd.year
ddd_s = "%04d-%02d-%02d" % (ddd_y, ddd_m, ddd_d)

def get_exectimes(gdate = ddd_s):
    
    import time
    from dbmysql_connect import DBMY
    
    sout = "Get monitoring data for {0}".format(gdate)
    log.info(sout)
    
    s_sqlt = """SELECT dt, exectime FROM dbmisr1 WHERE dt like '{0}%';"""
    s_sql  = s_sqlt.format(gdate)
    
    db = DBMY()
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    recs = cursor.fetchall()
    ar = []
    etime_min = 0
    etime_max = 0
    
    i = 0
    for rec in recs:
        dt       = rec[0]
        exectime = rec[1]
        if i == 0:
            etime_min = exectime
            etime_max = exectime
        else:
            if exectime < etime_min: etime_min = exectime
            if exectime > etime_max: etime_max = exectime
        
        # 
        # http://stackoverflow.com/questions/2775864/python-datetime-to-unix-timestamp
        #
        unixtime = int(time.mktime(dt.timetuple()))
        ar.append([unixtime, exectime])
        i += 1
        
    
    db.close()
    return ar, etime_min, etime_max
    

if __name__ == "__main__":

    import time
    import datetime
    
    import rrdtool

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('Graphing of DBMIS Monitoring Start {0}'.format(localtime))
    
    ar, etime_min, etime_max = get_exectimes()
    
    starttime = ar[0][0] - 10
    dpoints = len(ar)
    endtime = ar[dpoints-1][0]
    
    ds_string = "DS:rtime:GAUGE:60:{0}:{1}".format(etime_min, etime_max)
    
    rrdtool.create(FNAME ,
             '--step', '10',
             '--start' , str(starttime) ,
             ds_string ,
             'RRA:MAX:0.5:1:3600'
    )    
    
    for rec in ar:
        unixtime = rec[0]
        exectime = rec[1]
        rrdtool.update(FNAME , '%d:%d' % (unixtime , exectime))


    def_string = "DEF:rtime={0}:rtime:MAX".format(FNAME)
    area_string = "AREA:rtime#990033:rtime"
    
    rrdtool.graph(GFNAME ,
         '--start' , str(starttime) ,
         '--end' , str(endtime) ,
         '--vertical-label' , 'Exec Time, msec' ,
         '--imgformat' , 'PNG' ,
         '--title' , 'Etalon Execution Time' ,
         '--lower-limit' , '0' ,
         def_string,
         area_string
    )