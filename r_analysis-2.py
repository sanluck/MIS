#!/usr/bin/python
# -*- coding: utf-8 -*-
# r_analysis-1.py - анализ реестров
#                   посещения <-> обращения
#                   ДЮЮ (обращения = 2 посещения специалиста)
#

import logging
import sys, codecs
from datetime import datetime

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_ranalysis2.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

F_ID  = 2

S_O1  = 167.30
S_O1F = 104.00
S_O2  = 167.30
S_O2F = 104.00

STEP = 100

s_sqlt1 = """SELECT pr.spolis, pr.npolis, pr.spec, pr.uet, pr.data_p, 
pr.perv_povt, pr.cel_posobr,
pr$tarif.t1, pr$tarif.t2, pr$tarif.t3, pr$tarif.t4
FROM mis.pr
LEFT JOIN mis.pr$tarif ON pr.spec = pr$tarif.spec 
WHERE pr.f_id = {0} 
ORDER BY spolis, npolis, spec, data_p;"""

s_sqlts = """SELECT DISTINCT pr.spec,
pr$tarif.t3, pr$tarif.t4
FROM mis.pr
LEFT JOIN mis.pr$tarif ON pr.spec = pr$tarif.spec 
WHERE pr.f_id = {0} 
ORDER BY pr.spec;"""

s_sqlt0 = """SELECT
fname
FROM pr_done
WHERE id = {0};"""

def ra1(db, f_id = F_ID):
    
    cur   = db.con.cursor()

    s_sql = s_sqlts.format(f_id)
    cur.execute(s_sql)
    result = cur.fetchall()
    
    s_count = {}
    ttt     = {}
    for rec in result:
        spec = rec[0]
        t3   = rec[1]
        t4   = rec[2]
        s_count[spec] = 0
        ttt[spec] = [t3, t4]
    
    s_sql = s_sqlt1.format(f_id)
    cur.execute(s_sql)
    result = cur.fetchall()
    ncount = 0
    sum1   = 0.0
    sum2   = 0.0
    sum1o  = 0.0
    sum2o  = 0.0
    
    sum3   = 0.0
    sum4   = 0.0

    sum29  = 0.0
    n29    = 0
    
    n_posobr3 = 0
    
    t_polis = ""
    t_spec  = 0
    n_spec  = 0
    
    for rec in result:

        ncount += 1
        spolis     = rec[0]
        if spolis is None: spolis = ""
        npolis     = rec[1]
        spec       = rec[2]
        uet        = rec[3]
        data_p     = rec[4] 
        perv_povt  = rec[5] 
        cel_posobr = rec[6] 
        t1         = rec[7]
        t2         = rec[8]
        t3         = rec[9]
        t4         = rec[10]
        

        if cel_posobr == 3:
            n_posobr3 += 1
            if spec == 51:
                sum1o += S_O1F
                sum2o += S_O2F
            else:
                sum1o += S_O1
                sum2o += S_O2
        else:
            if spec == 29:
                sum1 += uet*float(t1)
                sum2 += uet*float(t2)
                sum3 += uet*float(t3)
                sum4 += uet*float(t4)
                n29  += 1
                sum29+= uet*float(t1)
            else:
                sum1 += float(t1)
                sum2 += float(t2)

        s_polis = spolis + npolis
    
        if t_polis == s_polis:
            if t_spec == spec:
                if cel_posobr == 3: continue
                n_spec  += 1
                if n_spec == 2:
                    n_spec = 0
                    s_count[spec] += 1
                    if spec != 29:
                        sum3 += float(t3)
                        sum4 += float(t4)
            else:
                t_spec = spec
                if cel_posobr == 3: 
                    n_spec = 0
                    continue
                n_spec = 1
        else:
            t_polis = s_polis
            t_spec  = spec
            if cel_posobr == 3:
                n_spec = 0
                continue
            n_spec = 1
        
        
    sout = "Count of cel_posob = 3: {0} Sum: {1} {2}".format(n_posobr3, sum1o, sum2o)
    log.info( sout )

    sout = "s1: {0} {1}  s2: {2} {3}".format(sum1, sum2, sum3, sum4)
    log.info( sout )
    
    sout = "n29: {0} sum29: {1}".format(n29, sum29)
    log.info( sout )
    
    sum_s2_1 = 0
    sum_s2_2 = 0
    
    for spec in s_count.keys():
        n2 = s_count[spec]
        t3 = ttt[spec][0]
        t4 = ttt[spec][1]

        s2_1 = n2*t3
        s2_2 = n2*t4
        
        if spec != 29:
            sum_s2_1 += s2_1
            sum_s2_2 += s2_2


        sout = "spec: {0} case count: {1}".format(spec, n2)
        log.info( sout )
        sout = "          s2_1: {0} s2_2: {1}".format(s2_1, s2_2)
        log.info( sout )

    log.info( "TOTAL" )
    sout = "          s2_1: {0} s2_2: {1}".format(sum_s2_1, sum_s2_2)
    
    log.info( sout )
    
    return ncount, sum1, sum2

if __name__ == "__main__":
    import time
    import datetime
    from dbmysql_connect import DBMY

    dbmy  = DBMY()
    cur   = dbmy.con.cursor()
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('Reestr Analysis Start {0}'.format(localtime))
    
    s_sql = s_sqlt0.format(F_ID)
    cur.execute(s_sql)
    rec = cur.fetchone()
    if rec is not None:
        fname = rec[0]
        sout = "f_id: {0} fname: {1}".format(F_ID, fname)
        log.info( sout )
        ncount, s1, s2 = ra1(dbmy, F_ID)
        sout = "Totally {0} records. s1: {1} s2: {2}".format(ncount, s1, s2)
        log.info( sout )
        
    else:
        sout = "f_id: {0} was not found in the <pr_done> table".format(F_ID, fname)
        log.info( sout )
        
    

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Reestr Analysis Finish  '+localtime)
    dbmy.close()
    sys.exit(0)
