#!/usr/bin/python
# -*- coding: utf-8 -*-
# insb-1-2-2.py - повторная обработка ответов из ТФОМС на ЗСП
#                (MySQL: mis.sm - файлы ST22M*.csv)
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()
from insorglist import InsorgInfoList
insorgs = InsorgInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insb1_2_2.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

STEP = 1000

UPDATE = True

SQLT_TPR = """SELECT tp.id, tp.people_id, tp.clinic_id,
sm.enp, sm.mcod
FROM tfoms_peoples tp
LEFT JOIN sm ON tp.people_id = sm.people_id
WHERE sm.enp_2_dbmis is Null;"""


SQLT_TPU = """UPDATE tfoms_peoples
SET
date_verify = Null,
tfoms_verification_status = Null,
date_from_tfoms = %s,
enp = %s,
lpu_tfoms = %s
WHERE
id = %s;"""

if __name__ == "__main__":

    import os, shutil
    import time
    from dbmysql_connect import DBMY

    import datetime

    dnow = datetime.datetime.now()
    sdnow = str(dnow)


    log.info("======================= INSB-1-2-2 ===========================================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Filling out TFOMS_PEOPLES table. Start {0}'.format(localtime))

    dbmy = DBMY()
    curr = dbmy.con.cursor()
    curw = dbmy.con.cursor()

    curr.execute(SQLT_TPR)
    results = curr.fetchall()


    nall = 0
    nfound = 0
    nomcod = 0
    for rec in results:
        nall += 1
        _id  = rec[0]
        p_id = rec[1]
        c_id = rec[2]
        enp  = rec[3]
        mcod = rec[4]

        if nall % STEP == 0:
            sout = " {0}: people_id: {1} enp: {2} mcod: {3}".format(nall, p_id, enp, mcod)
            log.info(sout)

        if enp is None:
            nfound += 1
            continue
        if mcod is not None:
            try:
                mo = modb[mcod]
                lpu_tfoms = mo.mis_code
            except:
                sout = "id: {0} people_id: {1}. Clinic not found for mcod = {2}".format(_id, p_id, mcod)
                log.warn(sout)
                lpu_tfoms = None
                nomcod += 1
        else:
            lpu_tfoms = None
            nomcod += 1

        curw.execute(SQLT_TPU,(dnow, enp, lpu_tfoms, _id, ))
        dbmy.con.commit()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Filling out TFOMS_PEOPLES table. Finish  '+localtime)

    sout = "Totally {0} records have been processed".format(nall)
    log.info(sout)

    sout = " {0} people_id have not been found in the sm table".format(nfound)
    log.info(sout)

    sout = " {0} people_id have not got lpu_tfoms".format(nomcod)
    log.info(sout)

    dbmy.close()
    sys.exit(0)
