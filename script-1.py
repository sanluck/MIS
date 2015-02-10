#!/usr/bin/python
# -*- coding: utf-8 -*-
# script-1.py - выполнить скрипт на подключение пользователя
#               ко всем ЛПУ в DBMIS
#

import os
import sys, codecs
import logging

from dbmis_connect2 import DBMIS

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_script1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB = "DBMIS"

SQLT_CLIST = """SELECT clinic_id, clinic_name 
FROM clinics;"""

SQLT_FCUSERS = """SELECT parent_user_id
FROM clinic_users
WHERE
clinic_id = ?
AND user_id = ?;"""

SQLT_CUSERS = """INSERT INTO clinic_users
(clinic_id, user_id, parent_user_id) 
VALUES
(?, ?, 0);"""

CLINIC_ID = 22
USER_ID = 175

def get_c_list():

    clinic_id = CLINIC_ID
    
    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
    cur = dbc.con.cursor()
    cur.execute(SQLT_CLIST)
    
    result = cur.fetchall()
    
    c_arr = []
    for rec in result:
        clinic_id = rec[0]
        c_arr.append(clinic_id)
        
    dbc.close()
    
    return c_arr

if __name__ == "__main__":


    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    c_arr = get_c_list()
    nclinics = len(c_arr)
    sout = "{0} clinics to be processed".format(nclinics)
    log.info(sout)

    clinic_id = CLINIC_ID
    
    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
    crr = dbc.con.cursor()
    cur = dbc.con.cursor()
    
    for clinic in c_arr:
        
        clinic_id = clinic
        crr.execute(SQLT_FCUSERS, (clinic_id, USER_ID, ))
        rec = crr.fetchone()
        if rec is None:
            cur.execute(SQLT_CUSERS, (clinic_id, USER_ID, ))
        
   
    dbc.con.commit()
    dbc.close()
    
    sout = "Scritp processing finished"
    log.info(sout)
    
    sys.exit(0)
