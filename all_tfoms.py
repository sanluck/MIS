#!/usr/bin/python
# -*- coding: utf-8 -*-
# all_tfoms.py -  проверка выгрузки ТФОМС
#                 по базе DBMIS
#

import logging
import sys, codecs

import fdb
from dbmis_connect2 import DBMIS
from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_alltfoms.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB = "DBMIS"

STEP = 100

F2DO_PATH        = "./ALL_TFOMS_IN"
FDONE_PATH       = "./ALL_TFOMS_OUT"

MOVE_FILE         = True

SQLT_GET_PEOPLES = """SELECT
people_id, birthday, enp,
medical_insurance_series,
medical_insurance_number
FROM peoples;"""

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

class PEOPLE:
    def __init__(self):
        self.people_id = None
        self.birthday = None
        self.enp = None
        self.oms_ser = None
        self.oms_num = None

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

def get_peoples(cur):

    cur.execute(SQLT_GET_PEOPLES)
    results = cur.fetchall()
    p_enp = {}
    p_oms = {}
    for rec in results:
        people = PEOPLE()
        people.people_id = rec[0]
        people.birthday = rec[1]
        people.enp = rec[2]
        people.oms_ser = rec[3]
        people.oms_num = rec[4]

        if people.enp: p_enp[people.enp] = people
        if people.oms_ser:
            oms_sn = people.oms_ser
        else:
            oms_sn = u""

        if people.oms_num:
            oms_sn += people.oms_num

        if len(oms_sn) > 0: p_oms[oms_sn] = people

    return p_enp, p_oms

def identify_ptfoms(ar_pf, d_enp, d_oms):
    
    l = len(ar_pf)
    lenp = 0
    loms = 0
    for i in range(l):
        pf = ar_pf[i]
        enp = pf.enp
        oms_ser = pf.oms_ser
        oms_num = pf.oms_num
        if oms_ser:
            oms_sn = oms_ser
        else:
            oms_sn = u""

        if oms_num:
            oms_sn += oms_num
        
        if enp:
            if d_enp.has_key(enp):
                p = d_enp[enp]
                people_id = p.people_id
                lenp += 1
                pf.people_id = people_id
                ar_pf[i] = pf
        elif len(oms_sn) > 0:
            if d_oms.has_key(oms_sn):
                p = d_oms[oms_sn]
                people_id = p.people_id
                loms += 1
                pf.people_id = people_id
                ar_pf[i] = pf
    
    return lenp, loms

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

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    dbc = DBMIS(mis_host = HOST, mis_db = DB)
    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = dbc.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    ro_cur = ro_transaction.cursor()

    p_enp, p_oms = get_peoples(ro_cur)

    l_enp = len(p_enp)
    l_oms = len(p_oms)

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('ALL TFOMS. PEOPLES TABLE LOADED {0}'.format(localtime))
    
    sout = "ENP count: {0}".format(l_enp)
    log.info(sout)

    sout = "OMS count: {0}".format(l_oms)
    log.info(sout)

    for fname in fnames:
        f_fname = F2DO_PATH + "/" + fname
        sout = "Input file: {0}".format(f_fname)
        log.info(sout)

        ar_ptfoms = get_ptfomss(f_fname)
        l_ar = len(ar_ptfoms)
        sout = "File has got {0} lines".format(l_ar)
        log.info( sout )
        
        lenp, loms = identify_ptfoms(ar_ptfoms, p_enp, p_oms)
        
        sout = "{0} patients have been identified using ENP".format(lenp)
        log.info(sout)
        
        sout = "{0} patients have been identified using OMS".format(loms)
        log.info(sout)


    dbc.con.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('ALL TFOMS. Finish  '+localtime)

    sys.exit(0)
