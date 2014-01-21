#!/usr/bin/python
# -*- coding: utf-8 -*-
# r_analysis-1.py - анализ реестров
#                   посещения <-> обращения
#

import logging
import sys, codecs
from datetime import datetime

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_ranalysis.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

F_ID  = 2

S_O1  = 167.3
S_O2  = 167.3

STEP = 100

s_sqlt1 = """SELECT pr.spolis, pr.npolis, pr.spec, pr.data_p, 
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
        s_count[spec] = [0, 0]
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
    
    n_posobr3 = 0
    
    t_polis = ""
    t_spec  = 0
    l_perv  = False
    for rec in result:

        ncount += 1
        spolis     = rec[0]
        if spolis is None: spolis = ""
        npolis     = rec[1]
        spec       = rec[2]
        data_p     = rec[3] 
        perv_povt  = rec[4] 
        cel_posobr = rec[5] 
        t1         = rec[6]
        t2         = rec[7]
        t3         = rec[8]
        t4         = rec[9]
        

        if cel_posobr == 3:
            n_posobr3 += 1
            sum1o += S_O1
            sum2o += S_O2
        else:
            sum1 += float(t1)
            sum2 += float(t2)

        s_polis = spolis + npolis
    
        if t_polis == s_polis:
            if t_spec == spec:
                if cel_posobr == 3: continue
                if perv_povt == 1:
                    l_perv = True
                    s_count[spec][0] += 1
                else:
                    if l_perv: 
                        sum3 += float(t3)
                        sum4 += float(t4)
                        s_count[spec][1] += 1
                    
                    l_perv = False
            else:
                t_spec = spec
                if (cel_posobr != 3) and (perv_povt == 1) :
                    l_perv = True
                    s_count[spec][0] += 1
                else:
                    l_perv = False
        else:
            t_polis = s_polis
            t_spec  = spec
            if cel_posobr == 3: continue
            if (cel_posobr != 3) and (perv_povt == 1):
                l_perv = True
                s_count[spec][0] += 1
            else:
                l_perv = False
        
        
    sout = "Count of cel_posob = 3: {0} Sum: {1} {2}".format(n_posobr3, sum1o, sum2o)
    log.info( sout )
    
    sum_s1_1 = 0
    sum_s1_2 = 0
    sum_s2_1 = 0
    sum_s2_2 = 0
    
    for spec in s_count.keys():
        c1 = s_count[spec][0]
        c2 = s_count[spec][1]
        t3 = ttt[spec][0]
        t4 = ttt[spec][1]

        s1_1 = c1*t3
        s1_2 = c1*t4
        s2_1 = c2*t3
        s2_2 = c2*t4

        sum_s1_1 += s1_1
        sum_s1_2 += s1_2
        sum_s2_1 += s2_1
        sum_s2_2 += s2_2


        sout = "spec: {0} count1: {1} count2: {2}".format(spec, c1, c2)
        log.info( sout )
        sout = "          s1_1: {0} s1_2: {1} s2_1: {2} s2_2: {3}".format(s1_1, s1_2, s2_1, s2_2)
        log.info( sout )

    log.info( "TOTAL" )
    sout = "s1_1: {0} s1_2: {1} s2_1: {2} s2_2: {3}".format(sum_s1_1, sum_s1_2, sum_s2_1, sum_s2_2)
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
