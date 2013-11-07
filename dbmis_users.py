#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# dbmis_users.py - calculate users by clinics
# 
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_dbmis_users.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST      = "fb2.ctmed.ru"
DB        = "DBMIS"
CLINIC_ID = 22

FIN  = "110_CLINICS_USERS.xls"
FOUT = "110_CLINICS_USERS_R.xls"
I_START = 1
I_END   = 110

SQLT_CLINIC = "SELECT clinic_key FROM clinics WHERE clinic_id = {0};"
SQLT_USER1  = "SELECT user_id FROM users WHERE lpu_id_fk = {0} AND clinics_catalog_lpu_id_fk = {1};"
SQLT_USER2  = "SELECT DISTINCT clinic_id FROM clinic_users WHERE user_id = {0};"


if __name__ == "__main__":
    from xlrd import open_workbook
    from xlutils.save import save
    from dbmis_connect2 import DBMIS
    
    dbc = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    cursor  = dbc.con.cursor()
    cursor2 = dbc.con.cursor()
    
    
    rb = open_workbook(FIN,formatting_info=True)
    r_sheet = rb.sheet_by_index(0)
    
    for i in range(I_START, I_END+1):
        clinic_id = int(r_sheet.cell(i,3).value)
        cell1 = r_sheet.cell(i,4)

        s_sql = SQLT_CLINIC.format(clinic_id)
        cursor.execute(s_sql)
        rec = cursor.fetchone()
        if rec is None:
            sout = "Clinic {0} has not got clinic_key".format(clinic_id)
            log.warn(sout)
            continue

        key = rec[0]
        
        s_sql = SQLT_USER1.format(clinic_id, key)
        cursor.execute(s_sql)
        recs = cursor.fetchall()
        
        n1 = 0
        for rec in recs:
            user_id = rec[0]
            s_sql = SQLT_USER2.format(user_id)
            cursor2.execute(s_sql)
            results = cursor2.fetchall()
            nn = len(results)
            if nn == 1: n1 += 1
        
        r_sheet.put_cell(i,4,2,n1,cell1.xf_index)
        sout = "{0} key: {1} id: {2} n1: {3}".format(i, key, clinic_id, n1)
        log.info(sout)
        
    
    save(rb,FOUT)
    dbc.close()
    sys.exit(0)