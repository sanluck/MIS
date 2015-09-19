#!/usr/bin/python
# -*- coding: utf-8 -*-
# all_tfoms.py -  проверка выгрузки ТФОМС
#                 по базе DBMIS
#                 обработка в соответствии с задачей 2412
#

import logging
import sys, codecs
from datetime import datetime


import fdb
from dbmis_connect2 import DBMIS
from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_alltfoms2412.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB = "DBMIS"

STEP = 100

NFIELDS = 23
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
        self.s1 = ""
        self.s2 = ""
        self.s3 = ""
        self.enp = ""
        self.s5 = ""
        self.s6 = ""
        self.s7 = ""
        self.sbd = ""
        self.s9 = ""
        self.s10 = ""
        self.s11 = ""
        self.s12 = ""
        self.s13 = ""
        self.s14 = ""
        self.mcod = ""
        self.s16 = ""
        self.s17 = ""
        self.sdp = ""
        self.s19 = ""
        self.s20 = ""
        self.s21 = ""
        self.s22 = ""
        self.s23 = ""

        self.people_id = None
        self.m = None
        self.clinic_id = None
        self.motive_att = None
        self.d_begin = None
        self.docsnils = None

class PEOPLE:
    def __init__(self):
        self.people_id = None
        self.birthday = None
        self.enp = None
        self.oms_ser = None
        self.oms_num = None

def drop_q(s):
    if s[0] == '"':
        s = s[1:]
    if s[-1] == '"':
        s = s[:-1]
    elif s[-1] == '\n':
        s = s[:-2]
        if s[-1] == '"':
            s = s[:-1]        
        
    return s

def get_ptfomss(fname):
    ins = open( fname, "r" )

    array = []
    for line in ins:
        u_line = line.decode('cp1251')
        a_line = u_line.split(";")
        if len(a_line) < NFIELDS:
            sout = "Wrang line: {0}".format(u_line.encode('utf-8'))
            log.warn( sout )
            continue

        pf = PTFOMS()

        pf.s1 = drop_q(a_line[0])
        pf.s2 = drop_q(a_line[1])
        pf.s3 = drop_q(a_line[2])
        pf.enp = drop_q(a_line[3])
        pf.s5 = drop_q(a_line[4])
        pf.s6 = drop_q(a_line[5])
        pf.s7 = drop_q(a_line[6])
        pf.sbd = drop_q(a_line[7])
        pf.s9 = drop_q(a_line[8])
        pf.s10 = drop_q(a_line[9])
        pf.s11 = drop_q(a_line[10])
        pf.s12 = drop_q(a_line[11])
        pf.s13 = drop_q(a_line[12])
        pf.s14 = drop_q(a_line[13])
        pf.mcod = drop_q(a_line[14])
        pf.s16 = drop_q(a_line[15])
        pf.s17 = drop_q(a_line[16])
        pf.sdp = drop_q(a_line[17])
        pf.s19 = drop_q(a_line[18])
        pf.s20 = drop_q(a_line[19])
        pf.s21 = drop_q(a_line[20])
        pf.s22 = drop_q(a_line[21])
        pf.s23 = drop_q(a_line[22])

        array.append( pf )

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

def identify_ptfoms(ar_pf, d_enp):
    
    l = len(ar_pf)
    lenp = 0
    for i in range(l):
        pf = ar_pf[i]
        enp = pf.enp
        
        if enp:
            if d_enp.has_key(enp):
                p = d_enp[enp]
                people_id = p.people_id
                lenp += 1
                pf.people_id = people_id
                ar_pf[i] = pf
    
    return lenp

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
        motive_att = aprec[3]
        clinic_id = aprec[4]
        mcod = modb.moCodeByMisId(clinic_id)

        area_number   = aprec[5]
        speciality_id = aprec[6]

        pf.clinic_id = clinic_id
        pf.d_begin = date_beg
        pf.motive_att = motive_att

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
        
        line += '"' + pf.s1 + '";'
        line += '"' + pf.s2 + '";'
        line += '"' + pf.s3 + '";'
        line += '"' + pf.enp + '";'
        line += '"' + pf.s5 + '";'
        line += '"' + pf.s6 + '";'
        line += '"' + pf.s7 + '";'
        line += '"' + pf.sbd + '";'
        line += '"' + pf.s9 + '";'
        line += '"' + pf.s10 + '";'
        line += '"' + pf.s11 + '";'
        line += '"' + pf.s12 + '";'
        line += '"' + pf.s13 + '";'
        line += '"' + pf.s14 + '";'
        
        #line += '"' + pf.mcod + '";'
        if pf.clinic_id:
            try:
                dbmis_mcod = str(modb.moCodeByMisId(pf.clinic_id))
            except:
                dbmis_mcod = pf.mcod
        else:
            dbmis_mcod = pf.mcod
        line += '"' + dbmis_mcod + '";'

        line += '"' + pf.s16 + '";'
        if pf.motive_att:
            line += '"' + str(pf.motive_att) + '";'
        else:
            line += '"' + pf.s17 + '";'
        line += '"' + pf.sdp + '";'
        line += '"' + pf.s19 + '";'
        line += '"' + pf.s20 + '";'
        line += '"' + pf.s21 + '";'
        line += '"' + pf.s22 + '";'
        if pf.docsnils:
            line += '"' + pf.docsnils + '"'
        else:
            line += '"' + pf.s23 + '"'
        
        lout = line.upper().encode('cp1251')+"\r\n"
        fo.write(lout)
        
    fo.close()

if __name__ == "__main__":
    import os, shutil
    import time
    from people import get_fnames

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')

    log.info('ALL TFOMS 2412. Start {0}'.format(localtime))

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
        
        l_enp = identify_ptfoms(ar_ptfoms, p_enp)
        
        sout = "{0} patients have been identified using ENP".format(l_enp)
        log.info(sout)
        
        set_ap(ar_ptfoms)

        f_fname = FDONE_PATH + "/" + fname
        sout = "Output file: {0}".format(f_fname)
        log.info(sout)
        
        save_ptfomss(ar_ptfoms, f_fname)


    dbc.con.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('ALL TFOMS 2412. Finish  '+localtime)

    sys.exit(0)
