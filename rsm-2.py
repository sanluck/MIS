#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# rsm-2.py - get DBMIS monitoring results from DBMIS
#            for certain period of time
#            and find out stations which have got sql_exec_time > SQL_EXEC_TIME_MIN
#            in the same time
#

import sys
import logging
import datetime


LOG_FILENAME = '_rsm2.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

CLINIC_ID = 22
DB_HOST   = "fb.ctmed.ru"
DB_NAME   = "DBMIS"

DT_START  = "2013-11-21 12:30"
DT_FINISH = "2013-11-21 12:50"
DT_FORMAT = "%Y-%m-%d %H:%M"

SQL_EXEC_TIME_MIN = 200


S_SQLT = """SELECT
station_id, sql_exec_time
FROM station_mon 
WHERE datetime > '{0}'
AND datetime < '{1}' 
ORDER BY station_id;
"""

S_SQLT_STATION = """SELECT 
s.station_id, s.local_ip, s.remote_ip, s.user_id,
u.lpu_id_fk, u.lname, u.fname, u.mname
FROM station s
LEFT JOIN users u ON s.user_id = u.user_id
WHERE s.station_id = {0};
"""

if __name__ == "__main__":

    import time
    import xlwt
    from dbmis_connect2 import DBMIS

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('DBMIS Monitoring Analysis (rsm-2) Start {0}'.format(localtime))

    book = xlwt.Workbook()
    sheet1 = book.add_sheet("DBMIS")
    sheet1.write(0, 0, "DBMIS Monitoring Analysis")
    
    sout = "DB_HOST: {0} DB_NAME: {1}".format(DB_HOST, DB_NAME)
    log.info(sout)
    iout = 1
    sheet1.write(iout, 0, sout)
    
    dbc = DBMIS(CLINIC_ID, mis_host = DB_HOST, mis_db = DB_NAME)
    
    s_sql = S_SQLT.format(DT_START, DT_FINISH)
    
    cursor = dbc.con.cursor()
    cursor.execute(s_sql)
    results = cursor.fetchall()

    station_id = 0
    id_count   = 0
    etime_max  = 0
    etime_sum  = 0
    sm_ar = []
    
    for rec in results:
        s_id  = rec[0]
        etime = rec[1]
        if s_id != station_id:
            if station_id != 0:
              etime_avg = etime_sum/id_count
              sm_ar.append([station_id, id_count, etime_max, etime_avg])

            station_id = s_id
            id_count = 1
            etime_max  = 0
            etime_sum  = 0
        else:
            id_count += 1
          
        etime_sum += etime  
        if etime > etime_max: etime_max = etime
        
    # http://stackoverflow.com/questions/2788871/python-date-difference-in-minutes
    
    d1 = datetime.datetime.strptime(DT_START,  DT_FORMAT)
    d2 = datetime.datetime.strptime(DT_FINISH, DT_FORMAT)
    
    # convert to unix timestamp
    d1_ts = time.mktime(d1.timetuple())
    d2_ts = time.mktime(d2.timetuple())
    
    dt_min = int(d2_ts-d1_ts) / 60
    n_count = int(dt_min / 3)
    
    sout = "dt_start: {0}".format(d1)
    log.info( sout )
    iout += 1
    sheet1.write(iout, 0, sout)

    sout = "dt_finish: {0}".format(d2)
    log.info( sout )
    iout += 1
    sheet1.write(iout, 0, sout)

    sout = "Time difference in minutes: {0}".format(dt_min)
    log.info( sout )
    sout = "DBMIS requests number: {0}".format(n_count)
    log.info( sout )
    iout += 1
    sheet1.write(iout, 0, sout)
    
    sout = "st_id count  max   avg       local_ip        remote_ip   user clinic user_fio"
    log.info(sout)

    iout += 1
    iout += 1
    sheet1.write(iout, 0, u'st_id')
    sheet1.write(iout, 1, u'count')
    sheet1.write(iout, 2, u'etime_max')
    sheet1.write(iout, 3, u'etime_avg')
    sheet1.write(iout, 4, u'local_ip')
    sheet1.write(iout, 5, u'remote_ip')
    sheet1.write(iout, 6, u'user')
    sheet1.write(iout, 7, u'clinic')
    sheet1.write(iout, 8, u'user_fio')
    
    for rec in sm_ar:
        if rec[2] > SQL_EXEC_TIME_MIN:
            st_id     = rec[0]
            st_count  = rec[1]
            etime_max = rec[2]
            etime_avg = rec[3]
            
            s_sql = S_SQLT_STATION.format(st_id)
            cursor.execute(s_sql)
            rec1 = cursor.fetchone()
            if rec1 is not None:
                local_ip  = rec1[1]
                remote_ip = rec1[2]
                user_id   = rec1[3]
                clinic_id = rec1[4]
                
                if rec1[5] is None:
                    lname = u""
                    fio = u""
                else:
                    lname = rec1[5]
                    fio = lname

                if rec1[6] is None:
                    fname = u""
                else:
                    fname = rec1[6]
                    fio += u" " + fname
                    
                if rec1[7] is None:
                    mname = u""
                else:
                    mname = rec1[6]
                    fio += u" " + mname

            else:
                local_ip  = u"---"
                remote_ip = u"---"
                user_id   = 0
                clinic_id = 0
                fio = u"---"
            
            fio_u8 = fio.encode('utf-8')
            lip_u8 = local_ip.encode('utf-8')
            rip_u8 = remote_ip.encode('utf-8')
            
            sout = "%5d %5d %5d %5d %15s %15s %5d %5d %s" % (st_id, st_count, etime_max, etime_avg, lip_u8, rip_u8, user_id, clinic_id, fio_u8)
            log.info(sout)

            iout += 1
            sheet1.write(iout, 0, st_id)
            sheet1.write(iout, 1, st_count)
            sheet1.write(iout, 2, etime_max)
            sheet1.write(iout, 3, etime_avg)
            sheet1.write(iout, 4, local_ip)
            sheet1.write(iout, 5, remote_ip)
            sheet1.write(iout, 6, user_id)
            sheet1.write(iout, 7, clinic_id)
            sheet1.write(iout, 8, fio)
            
        
    book.save("rsm2.xls")    
    dbc.close()
    sys.exit(0)
    
