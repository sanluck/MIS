#!/usr/bin/python
# -*- coding: utf-8 -*-
# insb-cad-5.py - проверка файлов MO_CAD
#
# INPUT DIRECTORY MO2DO
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()
from insorglist import InsorgInfoList
insorgs = InsorgInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insb_cad5_check.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

STEP = 1000
PRINT_FOUND = False

MO2DO_PATH        = "./MO2DO"

if __name__ == "__main__":

    import os, shutil
    import time
    from dbmysql_connect import DBMY
    from people import get_fnames, get_mo_cad, put_mo_cad

    log.info("======================= INSB-CAD-5-CHECK ===================================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Testing of MO_CAD Files. Start {0}'.format(localtime))


    fnames = get_fnames(path = MO2DO_PATH, file_ext = '.csv')
    n_fnames = len(fnames)
    sout = "Totally {0} files has been found".format(n_fnames)
    log.info( sout )

    for fname in fnames:
        s_mcod  = fname[3:9]
        w_month = fname[9:15]
        mcod = int(s_mcod)

        try:
            mo = modb[mcod]
            clinic_id = mo.mis_code
            sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod)
            log.info(sout)
        except:
            sout = "Clinic not found for mcod = {0}".format(s_mcod)
            log.warn(sout)
            continue

        f_fname = MO2DO_PATH + "/" + fname
        sout = "Input file: {0}".format(f_fname)
        log.info(sout)

        ar = get_mo_cad(f_fname)
        l_ar = len(ar)
        sout = "File has got {0} lines".format(l_ar)
        log.info( sout )
        
        # Check values
        for p_ar in ar:
            doc_snils = p_ar.doc_snils
            if len(doc_snils) > 11:
                sout = "Wrang doc_snils ({0}) has been found".format(doc_snils)
                log.warn(sout)

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Testing of MO_CAD Files. Finish  '+localtime)

    sys.exit(0)
