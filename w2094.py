#!/usr/bin/python
# coding: utf-8
# w2094.py - задача 2094
#            читать МО файлы, 
#            проставить прикрепление из DBMIS,
#            записать новые МО файлы
#
# INPUT DIRECTORY FIN
# OUTPUT DIRECTORY FOUT
#

import os, sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()
from insorglist import InsorgInfoList
insorgs = InsorgInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

import logging

LOG_FILENAME = '_w2094.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

STEP = 1000
PRINT_FOUND = False

MO2DO_PATH        = "./FIN"
MODONE_PATH       = "./FOUT"

if __name__ == "__main__":

    import shutil
    import time
    from people import get_fnames, get_mo_cad, put_mo_cad

    log.info("======================= W2094 ===========================================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Updating MO_CAD Files. Start {0}'.format(localtime))

    fnames = get_fnames(path = MO2DO_PATH, file_ext = '.csv')
    n_fnames = len(fnames)
    sout = "Totally {0} files has been found".format(n_fnames)
    log.info( sout )

    for fname in fnames:

        f_fname = MO2DO_PATH + "/" + fname
        sout = "Input file: {0}".format(f_fname)
        log.info(sout)

        ar = get_mo_cad(f_fname)
        l_ar = len(ar)
        sout = "File has got {0} lines".format(l_ar)
        log.info( sout )

    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Updating MO_CAD Files. Finish  '+localtime)

    sys.exit(0)