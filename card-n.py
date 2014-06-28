#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# card-n.py - формирования файла ПН для выгрузки в Минздрав
#

import os
import sys
import logging

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

from card_1 import getCard

HOST      = "fb2.ctmed.ru"
DB        = "DBMIS"

CLINIC_ID = 268

D_START  = "2014-03-18"
D_FINISH = "2014-03-20"

STEP = 100

FNAME = "PN{0}_{1}.xml"
FPATH = "./PN"

SQLT_CL = """SELECT
prof_exam_id, people_id_fk, date_begin
FROM prof_exam_minor
WHERE clinic_id_fk = ?
AND date_end is not Null
AND type_exam_code = 1
AND status_code = 2
AND date_end >= ?
AND date_end <= ?
AND date_begin is not Null
ORDER by date_begin;"""

SQLT_RL = """SELECT * FROM prof_exam_results WHERE prof_exam_id_fk = ?;"""

SQLT_SNILS = """SELECT insurance_certificate, document_type_id_fk FROM peoples WHERE people_id = ?;"""

if __name__ == "__main__":
    LOG_FILENAME = '_cardn.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

def getC_list(dbc, clinic_id = CLINIC_ID, d_start = D_START, d_finish = D_FINISH):
    cur  = dbc.con.cursor()
    cur2 = dbc.con.cursor()
    
    cur.execute(SQLT_CL, (clinic_id, d_start, d_finish, ))
    recs = cur.fetchall()

    sout = "d_start: {0} d_finish: {1}".format(d_start, d_finish)
    log.info(sout)
    
    arr = []
    arr_arr = []
    nnn = 0
    for rec in recs:
        prof_exam_id = rec[0]
        cur2.execute(SQLT_RL, (prof_exam_id, ))
        rec2 = cur2.fetchone()
        if rec2 is None: continue
        people_id    = rec[1]
        cur2.execute(SQLT_SNILS, (people_id, ))
        rec2 = cur2.fetchone()
        if rec2 is None: continue
        if rec2[0] is None: continue
        doc_type = rec2[1]
        if doc_type not in (3, 14): continue
        date_begin   = rec[2]
        nnn += 1
        arr.append([prof_exam_id, people_id, date_begin])
        if nnn % STEP == 0:
            arr_arr.append(arr)
            arr = []
            
    if len(arr) > 0: arr_arr.append(arr)

    nn2 = len(arr_arr)
    sout = "Totally {0} cards {1} files to be processed".format(nnn, nn2)
    log.info(sout)
    return arr_arr

if __name__ == "__main__":
    from dbmis_connect2 import DBMIS
    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('-----------------------------------------------------------------------------------')
    log.info('Prof Exam Export Start {0}'.format(localtime))
    
    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    clinic_id = CLINIC_ID
    mcod = modb.moCodeByMisId(clinic_id)

    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)

    cname = dbc.name.encode('utf-8')
    caddr = dbc.addr_jure.encode('utf-8')
    
    sout = "clinic_id: {0} mcod: {1} clinic_name: {2}".format(clinic_id, mcod, cname)
    log.info(sout)
    sout = "address: {0}".format(caddr)
    log.info(sout)

    arr_arr = getC_list(dbc)
    nnn = 0
    for arr in arr_arr:
        nnn += 1
        c_list = arr
        f_fname = FPATH + "/" + FNAME.format(mcod, nnn)
        sout = "Output to file: {0}".format(f_fname)
        log.info(sout)
    
        fo = open(f_fname, "wb")
    
        sout = """<?xml version="1.0" encoding="UTF-8"?>
        <children>"""
        fo.write(sout)

    
        iii = 0
        for ccc in c_list:
            iii += 1
            e_id = ccc[0]
            p_id = ccc[1]
            d_bg = ccc[2]
        
            docTXT = getCard(dbc, e_id, p_id)
            fo.write(docTXT)
            fo.flush()
            os.fsync(fo.fileno())
        
        sout = '</children>'
        fo.write(sout)
        fo.close()

    dbc.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Prof Exam Export Finish  '+localtime)
    sys.exit(0)
    