#!/usr/bin/python
# -*- coding: utf-8 -*-
# all_tfoms.py -  проверка выгрузки ТФОМС
#                 по базе DBMIS
#

import logging
import sys, codecs
from datetime import datetime


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

SQLT_AP = """SELECT
ap.area_people_id, ap.area_id_fk, ap.date_beg, ap.motive_attach_beg_id_fk,
ca.clinic_id_fk,
ar.area_number,
ca.speciality_id_fk,
ap.docum_attach_beg_number
FROM area_peoples ap
LEFT JOIN areas ar ON ap.area_id_fk = ar.area_id
LEFT JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
WHERE ap.people_id_fk = ?
AND ca.basic_speciality = 1
AND ca.speciality_id_fk IN (1,7,38,51)
AND ap.date_end is Null
ORDER BY ap.date_beg DESC;"""

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
        try:
            ptfoms.oms_ddd = datetime.strptime(a_line[5], '%Y-%m-%d')
        except:
            ptfoms.oms_ddd = None
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
        oms_ser = rec[3]
        if oms_ser:
            people.oms_ser = oms_ser.strip()
        else:
            people.oms_ser = None
        oms_num = rec[4]
        if oms_num:
            people.oms_num = oms_num.strip()
            if (not people.enp) and len(people.oms_num) == 16:
                people.enp = people.oms_num
        else:
            people.oms_num = None

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
        
        if (not pf.people_id) and (len(oms_sn) > 0):
            if d_oms.has_key(oms_sn):
                p = d_oms[oms_sn]
                people_id = p.people_id
                loms += 1
                pf.people_id = people_id
                ar_pf[i] = pf
    
    return lenp, loms

def set_ap(ar_pf):
    from clinic_areas_doctors import get_cad, get_d

    try:
        dbmis = DBMIS(mis_host = HOST, mis_db = DB)
    except:
        return
        
    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = dbmis.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    cur = ro_transaction.cursor()
    
    t_clinic_id = 0

    l = len(ar_pf)
    for i in range(l):
        pf = ar_pf[i]
        people_id = pf.people_id
        if not people_id: continue
        
        cur.execute(SQLT_AP, (people_id, ))
        aprec = cur.fetchone()
        dbmis.con.commit()
        if not aprec: continue
        area_id = aprec[1]
        date_beg = aprec[2]
        clinic_id = aprec[4]
        mcod = modb.moCodeByMisId(clinic_id)

        area_number   = aprec[5]
        speciality_id = aprec[6]

        pf.clinic_id = clinic_id
        pf.d_begin = date_beg

        if clinic_id != t_clinic_id:
            cad = get_cad(dbmis, clinic_id)
            d1,d7,d38,d51 = get_d(dbc, clinic_id)
            t_clinic_id = clinic_id

        docsnils = None
        if cad.has_key(area_id):
            docsnils = cad[area_id][2]

        pf.docsnils = docsnils
        
        ar_pf[i] = pf
    
    dbmis.con.close()
    
def save_ptfomss(pf_arr, fout):
    
    fo = open(fout, "wb")

    for pf in pf_arr:
        line = u""
        
        if pf.ddd:
            line += pf.ddd.strftime("%Y-%m-%d")
        line += ";"
        
        if pf.enp:
            line += pf.enp
        line += ";"

        if pf.tdpfs:
            line += str(pf.tdpfs)
        line += ";"

        if pf.oms_ser:
            line += pf.oms_ser
        line += ";"

        if pf.oms_num:
            line += pf.oms_num
        line += ";"

        if pf.oms_ddd:
            line += pf.oms_ddd.strftime("%Y-%m-%d")
        line += ";"

        if pf.addr1:
            line += pf.addr1
        line += ";"

        if pf.addr2:
            line += pf.addr2
        line += ";"

        if pf.addr3:
            line += pf.addr3
        line += ";"

        if pf.addr4:
            line += pf.addr4
        line += ";"

        if pf.addr5:
            line += pf.addr5
        line += ";"

        if pf.birthday:
            line += pf.birthday.strftime("%Y-%m-%d")
        line += ";"

        if pf.clinic_key:
            line += str(pf.clinic_key)
        line += ";"

        if pf.mcod:
            line += str(pf.mcod)
        line += ";"

        if pf.people_id:
            line += str(pf.people_id)
        line += ";"

        if pf.clinic_id:
            try:
                dbmis_mcod = modb.moCodeByMisId(pf.clinic_id)
                line += str(dbmis_mcod)
            except:
                pass
#            line += str(pf.clinic_id)
        line += ";"

        if pf.d_begin:
            line += pf.d_begin.strftime("%Y-%m-%d")
        line += ";"

        if pf.docsnils:
            line += pf.docsnils
        
        lout = line.upper().encode('cp1251')+"\r\n"
        fo.write(lout)
        
    fo.close()

if __name__ == "__main__":
    import os, shutil
    import time
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

    try:
        dbc = DBMIS(mis_host = HOST, mis_db = DB)
    except:
        sys.exit(1)
        
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
        
        l_enp, l_oms = identify_ptfoms(ar_ptfoms, p_enp, p_oms)
        
        sout = "{0} patients have been identified using ENP".format(l_enp)
        log.info(sout)
        
        sout = "{0} patients have been identified using OMS".format(l_oms)
        log.info(sout)
        
        set_ap(ar_ptfoms)

        f_fname = FDONE_PATH + "/" + fname
        sout = "Output file: {0}".format(f_fname)
        log.info(sout)
        
        save_ptfomss(ar_ptfoms, f_fname)


    dbc.con.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('ALL TFOMS. Finish  '+localtime)

    sys.exit(0)
