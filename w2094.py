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
import random

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
DATE_ATT = "20150601"

HOST = "fb2.ctmed.ru"
DB = "DBMIS"

SQLT_FP_ENP = "SELECT people_id FROM peoples WHERE enp = ?;"

SQLT_AP = """SELECT
ap.area_people_id, ap.area_id_fk, ap.date_beg, ap.motive_attach_beg_id_fk,
ca.clinic_id_fk,
ar.area_number,
ca.speciality_id_fk
FROM area_peoples ap
LEFT JOIN areas ar ON ap.area_id_fk = ar.area_id
LEFT JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
WHERE ap.people_id_fk = ?
AND ca.basic_speciality = 1
AND ca.speciality_id_fk IN (1,7,38,51)
AND ap.date_end is Null
ORDER BY ap.date_beg DESC;"""

def t2094(ar):
    import fdb
    from dbmis_connect2 import DBMIS
    from people import get_people
    from clinic_areas_doctors import get_cad, get_d
    
    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    dbc = DBMIS(mis_host = HOST, mis_db = DB)

    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = dbc.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    ro_cur = ro_transaction.cursor()
    
    ar_result = []
    t_clinic_id = 0
    i = 0
    for mo in ar:
        i += 1
        
        mo = ar[i]
        lname = mo.lname
        fname = mo.fname
        mname = mo.mname
        bd    = mo.birthday
        enp   = mo.enp
        
        if i % STEP == 0:
            sout = "{0}: ENP {1}".format(i, enp)
            log.info(sout)
            
        precs = get_people(ro_cur, lname, fname, mname, bd)
        if precs:
            people_id = precs[0][0]
        elif enp:
            ro_cur.execute(SQLT_FP_ENP, (enp, ))
            erec = ro_cur.fetchone()
            if erec:
                people_id = erec[0]
            else:
                continue
        else:
            continue
        
        ro_cur.execute(SQLT_AP, (people_id, ))
        aprec = ro_cur.fetchone()
        
        if not aprec: continue
        area_id = aprec[1]
        motive_att = aprec[3]
        if not (motive_att in (1,2)): motive_att = 1
        clinic_id = aprec[4]
        mcod = modb.moCodeByMisId(clinic_id)
        if not mcod: continue
        area_number   = aprec[5]
        speciality_id = aprec[6]
        
        if clinic_id != t_clinic_id:
            cad = get_cad(dbc, clinic_id)
            d1,d7,d38,d51 = get_d(dbc, clinic_id)
            t_clinic_id = clinic_id
            
        mo.mcod = mcod
        mo.motive_att = motive_att
        mo.date_att = DATE_ATT
        mo.area_number = area_number
        d_snils = None
        if cad.has_key(area_id):
            d_snils = cad[area_id][2]
        else:
            if speciality_id == 1:
                if len(d1) > 0: d_snils = random.choice(d1)
            elif speciality_id == 7:
                if len(d7) > 0: d_snils = random.choice(d7)
            elif speciality_id == 38:
                if len(d38) > 0: d_snils = random.choice(d38)
            elif speciality_id == 51:
                if len(d51) > 0: d_snils = random.choice(d51)
        if d_snils: mo.doc_snils = d_snils
        ar_result.append(mo)

        if i % STEP == 0:
            sout = "{0}: people_id {1} doctor {2}".format(i, people_id, d_snils)
            log.info(sout)
        
    dbc.close()
    return ar_result

if __name__ == "__main__":

    import shutil
    import time
    from people import get_fnames, get_mo_cad, write_mo_cad

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

        arr = t2094(ar)
        l_arr = len(arr)
        sout = "Result has got {0} lines".format(l_ar)
        log.info( sout )
        
        f_fname = MODONE_PATH + "/" + fname
        sout = "Output file: {0}".format(f_fname)
        log.info(sout)
        write_mo_cad(arr, f_fname)
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Updating MO_CAD Files. Finish  '+localtime)

    sys.exit(0)