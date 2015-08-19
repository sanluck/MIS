#!/usr/bin/python
# -*- coding: utf-8 -*-
# all_tfoms.py -  проверка выгрузки ТФОМС
#                 по базе DBMIS
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_alltfoms.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

STEP = 100

F2DO_PATH        = "./ALL_TFOMS_IN"
FDONE_PATH       = "./ALL_TFOMS_OUT"

MOVE_FILE         = True

class PTFOMS:
    def __init__(self):
        self.ddd = None
        self.enp = None
        self.tdpfs = None
        self.oms_ser = None
        self.oms_num = None
        self.oms_ddd = None
        self.addr1 = None
        self.addr2 = None
        self.addr3 = None
        self.addr4 = None
        self.addr5 = None
        self.birthday = None
        self.clinic_key = None
        self.mcod = None

        self.people_id = None
        self.clinic_id = None
        self.d_begin = None
        self.docsnils = None

def get_ptfomss(fname):
    ins = open( fname, "r" )

    array = []
    for line in ins:
        u_line = line.decode('cp1251')
        a_line = u_line.split(";")
        if len(a_line) < 14:
            sout = "Wrang line: {0}".format(u_line.encode('utf-8'))
            log.warn( sout )
            continue

        ptfoms = PTFOMS()

        ddd = a_line[0]
        try:
            ptfoms.ddd = datetime.strptime(ddd, '%Y-%m-%d')
        except:
            ptfoms.ddd = None

        ptfoms.enp = a_line[1]

        tdpfs = a_line[2]
        if len(tdpfs) == 0:
            ptfoms.tdpfs = None
        else:
            ptfoms.tdpfs = int(tdpfs)

        ptfoms.oms_ser = a_line[3]
        ptfoms.oms_num = a_line[4]
        ptfoms.oms_ddd = a_line[5]
        ptfoms.addr1 = a_line[6]
        ptfoms.addr2 = a_line[7]
        ptfoms.addr3 = a_line[8]
        ptfoms.addr4 = a_line[9]
        ptfoms.addr5 = a_line[10]
        s_bd = a_line[11]
        try:
            ptfoms.birthday = datetime.strptime(s_bd, '%Y-%m-%d')
        except:
            ptfoms.birthday = None

        clinic_key = a_line[12]
        if len(clinic_key) == 0:
            ptfoms.clinic_key = None
        else:
            ptfoms.clinic_key = int(clinic_key)

        try:
            ptfoms.mcod = int(a_line[13])
        except:
            ptfoms.mcod = None

        array.append( ptfoms )

    ins.close()

    return array


if __name__ == "__main__":
    import os, shutil
    import time
    import datetime
    from people import get_fnames

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')

    log.info('ALL TFOMS. Start {0}'.format(localtime))

    fnames = get_fnames(path = F2DO_PATH, file_ext = '.csv')
    n_fnames = len(fnames)
    sout = "Totally {0} files has been found".format(n_fnames)
    log.info( sout )

    for fname in fnames:
        f_fname = F2DO_PATH + "/" + fname
        sout = "Input file: {0}".format(f_fname)
        log.info(sout)

        ar_ptfoms = get_ptfomss(f_fname)
        l_ar = len(ar_ptfoms)
        sout = "File has got {0} lines".format(l_ar)
        log.info( sout )

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('ALL TFOMS. Finish  '+localtime)

    sys.exit(0)
