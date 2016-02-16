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

import fdb
from dbmis_connect2 import DBMIS
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

SQLT_GETP0 = """SELECT
people_id, fname, lname, mname, birthday, enp,
medical_insurance_series,
medical_insurance_number
FROM peoples
WHERE enp = ?;"""

SQLT_GETP1 = """SELECT
medical_insurance_series,
medical_insurance_number,
enp, 
lname, fname, mname, birthday, 
enp, birthplace, 
document_type_id_fk, document_series, document_number,
document_when, document_who,
insurance_certificate
FROM peoples
WHERE people_id = ?;"""


class PEOPLE:
    def __init__(self):
        self.people_id = None
        self.fname = None
        self.lname = None
        self.mname = None
        self.birthday = None
        self.enp = None
        self.oms_ser = None
        self.oms_num = None
        
    def init_from_rec(self, rrr):
        self.people_id = rrr[0]
        self.fname = rrr[1]
        self.lname = rrr[2]
        self.mname = rrr[3]
        self.birthday = rrr[4]
        self.enp = rrr[5]
        self.oms_ser = rrr[6]
        self.oms_num = rrr[7]

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

def get_peoples():
    
    log.info('GET ALL PEOPLES. Start {0}'.format(localtime))
    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    p_enp = {}
    p_oms = {}


    try:
        dbc = DBMIS(mis_host = HOST, mis_db = DB)
    except:
        log.warn("Can't connect to DBMIS")
        return p_enp, p_oms

    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = dbc.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    cur = ro_transaction.cursor()

    cur.execute(SQLT_GET_PEOPLES)
    results = cur.fetchall()
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
        
    dbc.con.commit()
    dbc.close()
    
    log.info('GET ALL PEOPLES. Finish {0}'.format(localtime))
    log.info('len(p_enp): {0} len(p_oms): {1}'.format(len(p_enp), len(p_oms)))
    return p_enp, p_oms

def get_plist(f_fname, p_enp, o_oms):
# get patients list
    from people import MO_CAD_PEOPLE
    from PatientInfo import PatientInfo
    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Get Patients List on the Base of {0}. Start {1} '.format(f_fname, localtime))

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
    
    nnn = 0
    ppp_arr = []
    for enp in array:
        
        if p_enp.has_key(enp):
            p = p_enp[enp]
            people_id = p.people_id
            
        else:
            continue
        
        ro_transaction.begin()
        ro_cur.execute(SQLT_GETP1, (people_id, ))
        rec = ro_cur.fetchone()
        if not rec: continue
        
        ppp = MO_CAD_PEOPLE()
        ppp.people_id = people_id
        ppp.enp = enp
        
        medical_insurance_series = rec[0]
        medical_insurance_number = rec[1]
        enp = rec[2]
        lname = rec[3]
        fname = rec[4]
        mname = rec[5]
        birthday = rec[6]
        # enp = rec[7]
        birthplace = rec[8]
        document_type_id_fk = rec[9]
        document_series = rec[10]
        document_number = rec[11]
        document_when = rec[12]
        document_who  = rec[13]
        insurance_certificate = rec[14]
        
        ro_transaction.commit()
        
        # medical_insurance_series (s_mis) & medical_insurance_number (s_min)
        sss = medical_insurance_series
        if sss == None:
            s_mis = u""
            oms_sn = s_mis
        else:
            s_mis = u"{0}".format(sss)
            oms_sn = s_mis + u" "
    
        sss = medical_insurance_number
        if sss == None:
            s_min = u""
        else:
            s_min = u"{0}".format(sss)
            oms_sn += s_min
    
        oms_enp = u""
        if len(s_mis) == 0:
            tdpfs = u"3" # Полис ОМС единого образца
            oms_enp = s_min
        elif s_mis[0] in (u"0", u"1", u"2", u"3", u"4", u"5", u"6", u"7", u"8", u"9"):
            tdpfs = u"2" # Временное свидетельство, ....
        else:
            tdpfs = u"1" # Полис ОМС старого образца
            
        ppp.dpfs = tdpfs
        ppp.oms_sn = oms_sn

        ppp.lname = lname
        ppp.fname = fname
        ppp.mname = mname
        

        ppp.birthday = birthday
        if birthplace:
            ppp.birthplace = birthplace
        else:
            ppp.birthplace = u""
            
        if document_type_id_fk is None:
            sdt = u"14"
        else:
            sdt = str(document_type_id_fk)
        ppp.doc_type_id = sdt

        if document_series is None:
            dsn = None
        else:
            dsn = document_series
    
        if document_number is not None:
            if dsn is None:
                dsn = document_number
            else:
                dsn += u" № " + document_number
        
        ppp.doc_sn = dsn

        if document_when is None:
            ppp.doc_when = None
        else:
            dw = document_when
            sdr = u"%04d%02d%02d" % (dw.year, dw.month, dw.day)
            ppp.doc_when = sdr
    
        if document_who is None:
            ppp.doc_who = None
        else:
            ppp.doc_who = document_who.strip().upper()
    
        if insurance_certificate is None:
            ppp.snils = None
        else:
            ppp.snils = insurance_certificate.replace('-','').replace(' ','')
        
        ppp_arr.append(ppp)
        nnn += 1
        
    dbc.con.commit()
    dbc.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Get Patients List. Finish {0}'.format(localtime))
    
    log.info('Identified {0} patients'.format(nnn))
    
    return ppp_arr

if __name__ == "__main__":

    import shutil
    import time

    log.info("======================= INSB-3188 ===========================================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Getting patients and processing data. Start {0}'.format(localtime))

    p_enp, p_oms = get_peoples()

    fnames = get_fnames()
    n_fnames = len(fnames)
    sout = "Totally {0} files has been found".format(n_fnames)
    log.info( sout )

    for fname in fnames:
        f_fname = IN_PATH + "/" + fname
        sout = "Input file: {0}".format(f_fname)
        log.info(sout)
        
        plist = get_plist(f_fname, p_enp, p_oms)
        
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Getting patients and processing data. Finish  '+localtime)

    sys.exit(0)