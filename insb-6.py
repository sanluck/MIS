#!/usr/bin/python
# -*- coding: utf-8 -*-
# insb-6.py - формирование файлов MO
#             из базы DBMY (mis.mo)
#             по списку клиник из mis.mo_done
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insb6.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

FNAME  = "MO2{0}{1}.csv" # в ТФОМС на внесение изменений
R_PATH = "./RMO"

STEP = 1000

OCATO      = '01000'

PRINT2     = False

# режим вывода
# 1 - все записи
# 2 - только те, у которых tfoms_verification_status != 1
MODE = 2

def get_clist(db):

    s_sql = "SELECT DISTINCT mcod FROM mo_done WHERE d_out is Null;"

    cur = db.con.cursor()
    cur.execute(s_sql)
    result = cur.fetchall()

    ar = []

    for rec in result:
        mcod = rec[0]
        ar.append(mcod)

    return ar

def register_cdone(db, mcod, l_out):
    import datetime

    dnow = datetime.datetime.now()
    sdnow = str(dnow)

    s_sql = """UPDATE mo_done
    SET d_out = %s,
    l_out = %s
    WHERE mcod = %s;"""

    cur = db.con.cursor()
    cur.execute(s_sql, (sdnow, l_out, mcod, ))

if __name__ == "__main__":

    import os
    import time, datetime
    from dbmysql_connect import DBMY
    from people import get_mo_fromdb, write_mo

    log.info("======================= INSB-6 START =====================================")

    dbmy = DBMY()
    clist = get_clist(dbmy)
    mcount = len(clist)
    sout = "Totally {0} MO to be processed".format(mcount)
    log.info( sout )
    for mcod in clist:
        try:
            mo = modb[mcod]
            clinic_id = mo.mis_code
            sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod)
            log.info(sout)
        except:
            sout = "Have not got clinic for MO Code {0}".format(mcod)
            log.warn(sout)
            clinic_id = 0
            continue

        mo_ar = get_mo_fromdb(dbmy, mcod, MODE)
        l_ar  = len(mo_ar)
        sout  = "has got {0} MO lines".format(l_ar)
        log.info(sout)

        stime   = time.strftime("%Y%m%d")
        fname   = FNAME.format(mcod, stime)
        f_fname = R_PATH  + "/" + fname
        sout = "Output to file: {0}".format(f_fname)
        log.info(sout)

        l_out = write_mo(mo_ar, f_fname)
        register_cdone(dbmy, mcod, l_out)

    dbmy.close()
    log.info("----------------------- INSB-6 FINISH ------------------------------------")
    sys.exit(0)
