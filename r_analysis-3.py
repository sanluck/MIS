#!/usr/bin/python
# -*- coding: utf-8 -*-
# r_analysis-3.py - анализ реестров
#                   посещения <-> обращения
#                   Недосеко (обращения = 2 посещения специалиста, 
#                             остатки как обращения с другой целью)
#

import logging
import sys, codecs
from datetime import datetime

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_ranalysis3.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

F_ID     = 1
LPU_TYPE = 2

STEP = 100

if LPU_TYPE == 1:
    s_sqlt1 = """SELECT pr.spolis, pr.npolis, pr.spec, pr.uet, pr.data_p, 
    pr.perv_povt, pr.cel_posobr,
    pr$tarif.t1, pr$tarif.t3
    FROM mis.pr
    LEFT JOIN mis.pr$tarif ON pr.spec = pr$tarif.spec 
    WHERE pr.f_id = {0} 
    ORDER BY spolis, npolis, spec, data_p;"""

    S_O  = 167.30
    S_OF = 104.00

    s_sqlts = """SELECT DISTINCT pr.spec,
    pr$tarif.t3
    FROM mis.pr
    LEFT JOIN mis.pr$tarif ON pr.spec = pr$tarif.spec 
    WHERE pr.f_id = {0} 
    ORDER BY pr.spec;"""
    
else:
    s_sqlt1 = """SELECT pr.spolis, pr.npolis, pr.spec, pr.uet, pr.data_p, 
    pr.perv_povt, pr.cel_posobr,
    pr$tarif.t2, pr$tarif.t4
    FROM mis.pr
    LEFT JOIN mis.pr$tarif ON pr.spec = pr$tarif.spec 
    WHERE pr.f_id = {0} 
    ORDER BY spolis, npolis, spec, data_p;"""

    S_O  = 167.30
    S_OF = 104.00

    s_sqlts = """SELECT DISTINCT pr.spec,
    pr$tarif.t4
    FROM mis.pr
    LEFT JOIN mis.pr$tarif ON pr.spec = pr$tarif.spec 
    WHERE pr.f_id = {0} 
    ORDER BY pr.spec;"""


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
        t    = rec[1] # цена по обращениям
        s_count[spec] = 0
        ttt[spec]     = t
    
    s_sql = s_sqlt1.format(f_id)
    cur.execute(s_sql)
    result = cur.fetchall()
    ncount = 0
    sum1   = 0.0 # visit sum
    sum2   = 0.0 # case  sum
    sumo   = 0.0 # other visit sum

    sum29  = 0.0
    n29    = 0
    
    n_posobr3 = 0
    
    t_polis = ""
    t_spec  = 0
    n_spec  = 0

    n_other = 0
    s_other = 0.0
    
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
        t1         = rec[7] # visit cost
        t2         = rec[8] # case cost
        

        if cel_posobr == 3:
            n_posobr3 += 1
            if spec == 51:
                sumo += S_OF
            else:
                sumo += S_O
        else:
            if spec == 29:
                sum1 += uet*float(t1)
                sum2 += uet*float(t2)
                n29  += 1
                sum29+= uet*float(t1)
            else:
                sum1 += float(t1)

        s_polis = spolis + npolis
    
        if t_polis == s_polis:
            if t_spec == spec:
                if cel_posobr == 3: continue
                n_spec  += 1
                if n_spec == 2:
                    n_spec = 0
                    s_count[spec] += 1
                    if spec != 29:
                        sum2 += float(t2)
            else:
                if n_spec == 1:
                    if t_spec == 51:
                        s_other += S_OF
                    else:
                        s_other += S_O
                    n_other += 1

                t_spec = spec
                if cel_posobr == 3: 
                    n_spec = 0
                else:
                    n_spec = 1
        else:

            if n_spec == 1:
                if t_spec == 51:
                    s_other += S_OF
                else:
                    s_other += S_O
                n_other += 1

            t_polis = s_polis
            t_spec  = spec
            if cel_posobr == 3:
                n_spec = 0
            else:
                n_spec = 1
        
        
    sout = "Count of cel_posob = 3: {0} Sum: {1}".format(n_posobr3, sumo)
    log.info( sout )
    
    sout = "n_other: {0} s_other: {1}".format(n_other, s_other)
    log.info( sout )

    sout = "n29: {0} sum29: {1}".format(n29, sum29)
    log.info( sout )
    

    sout = "sum1: {0} sum2: {1}".format(sum1, sum2)
    log.info( sout )
    
    sum_s2 = 0.0
    
    for spec in s_count.keys():
        n2 = s_count[spec]
        t  = ttt[spec]

        s2 = n2*t
        
        if spec != 29:
            sum_s2 += float(s2)

            sout = "spec: {0} case count: {1} sum: {2}".format(spec, n2, s2)
            log.info( sout )

    log.info( "TOTAL: {0}".format(sum_s2) )
    
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
        sout = "f_id: {0} fname: {1} lpu_type: {2}".format(F_ID, fname, LPU_TYPE)
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
