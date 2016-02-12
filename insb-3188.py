#!/usr/bin/python
# -*- coding: utf-8 -*-
# insb-3188.py - формирование фалов на прикрепление
#                по списку ЕНП полученных из ТФОМС
#                в виде фала ошибок (задача 3188)
#

import os
import datetime
import time
import random

import sys, codecs
import ConfigParser
import logging

from dbmysql_connect import DBMY

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insb_3188.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

Config = ConfigParser.ConfigParser()
PATH = os.getcwd()
FINI = PATH + "/" + "insr.ini"

log.info("INI File: {0}".format(FINI))

from ConfigSection import ConfigSectionMap
# read INI data
Config.read(FINI)
# [DBMIS]
Config1 = ConfigSectionMap(Config, "DBMIS")
HOST = Config1['host']
DB = Config1['db']

# [Insb]
Config2 = ConfigSectionMap(Config, "Insb")
ADATE_ATT = Config2['adate_att']
SET_ADATE = Config2['set_adate']
if SET_ADATE == "1":
    ASSIGN_ATT = True
else:
    ASSIGN_ATT = False

# [Cad]
Config2 = ConfigSectionMap(Config, "Cad")
ANYDOCTOR = Config2['anydoctor']
if ANYDOCTOR == "1":
    FIND_DOCTOR = True
else:
    FIND_DOCTOR = False

EMPTYDOCTOR = Config2['emptydoctor']
if EMPTYDOCTOR == "1":
    EMPTY_DOCTOR = True
else:
    EMPTY_DOCTOR = False

action_u8 = Config2['action']
ACTION = action_u8.decode('utf-8')

SET_DOC = Config2['set_doc_category']
if SET_DOC == "1":
    SET_DOC_CATEGORY = True
else:
    SET_DOC_CATEGORY = False

CLINIC_OGRN = u""

FNAMEb = "MO2{0}{1}.csv" # в ТФОМС

IN_PATH  = "./ERR_TFOMS_IN"
OUT_PATH = "./ERR_TFOMS_OUT"

STEP = 1000

OCATO      = '01000'

SQLT_GETP = """
"""

def get_fnames(path = IN_PATH, file_ext = '.csv'):
# get file names

    import os

    fnames = []
    for subdir, dirs, files in os.walk(path):
        for fname in files:
            if fname.find(file_ext) > 1:
                log.info(fname)
                fnames.append(fname)

    return fnames

def get_plist(f_fname):
# get patients list
    import fdb
    from dbmis_connect2 import DBMIS
    from PatientInfo import PatientInfo
    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Get Patients List on the Base of {0}'.format(f_fname))

    # read file line by line into arry
    
    ins = open( f_fname, "r" )
    
    array = []
    nn = 0
    for line in ins:
        nn += 1
        u_line = line.decode('cp1251')
        data = u_line.split(';')
        if len(data) < 2: continue
        
        enp = data[1]
        if len(enp) != 16: continue
        
        array.append( enp )
    
    ins.close()
    
    log.info("Have got {0} ENPs from {1} lines".format(len(array), nn))
    
    log.info('Use database: {0}:{1}'.format(HOST, DB))

    dbc = DBMIS(mis_host = HOST, mis_db = DB)
    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = dbc.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    ro_cur = ro_transaction.cursor()
    
    for enp in array:
        


if __name__ == "__main__":

    import shutil
    import time

    log.info("======================= INSB-3188 ===========================================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Getting patients and processing data. Start {0}'.format(localtime))

    fnames = get_fnames()
    n_fnames = len(fnames)
    sout = "Totally {0} files has been found".format(n_fnames)
    log.info( sout )

    for fname in fnames:
        f_fname = IN_PATH + "/" + fname
        sout = "Input file: {0}".format(f_fname)
        log.info(sout)
        
        plist = get_plist(f_fname)
        
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Getting patients and processing data. Finish  '+localtime)

    sys.exit(0)