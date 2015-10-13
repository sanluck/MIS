#!/usr/bin/python
# -*- coding: utf-8 -*-
# insb-cad.py - выгрузка пациент-врач
#               задача 1936
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

LOG_FILENAME = '_insb_cad.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

Config = ConfigParser.ConfigParser()
#PATH = os.path.dirname(sys.argv[0])
#PATH = os.path.realpath(__file__)
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

# [Insr]
Config2 = ConfigSectionMap(Config, "Insr")
D_START = Config2['d_start']
D_FINISH = Config2['d_finish']
S_USE_DATERANGE = Config2['use_daterange']
if S_USE_DATERANGE == "1":
    USE_DATE_RANGE = True
    DATE_RANGE = [D_START,D_FINISH]
else:
    USE_DATE_RANGE = False
    DATE_RANGE = None

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

IN_PATH    = "./FIN"

STEP = 1000

OCATO      = '01000'

PRINT2     = False

PRINT_ALL  = True # include all patients into MO files

def plist(dbc, clinic_id, mcod, patient_list):
    from clinic_areas_doctors import get_cad, get_d
    from PatientInfo import p1, p2, p3
    from insorglist import InsorgInfoList

    cad = get_cad(dbc, clinic_id)
    d1,d7,d38,d51 = get_d(dbc, clinic_id)

    cad1 = {}
    cad7 = {}
    cad38 = {}
    cad51 = {}

    for a_id in cad.keys():
        cad_item = cad[a_id]
        speciality_id = cad_item[0]
        area_number = cad_item[1]
        snils = cad_item[2]
        if speciality_id == 1:
            cad1[a_id] = snils
        elif speciality_id == 7:
            cad7[a_id] = snils
        elif speciality_id == 38:
            cad38[a_id] = snils
        elif speciality_id == 51:
            cad51[a_id] = snils

    dnumber = len(cad)
    d1number = len(cad1)
    d7number = len(cad7)
    d38number = len(cad38)
    d51number = len(cad51)

    sout = "Totally we have got {0} ({1} + {2} + {3} + {4}) areas having doctors".format(dnumber, d1number, d7number, d38number, d51number)
    log.info(sout)

    d1number = len(d1)
    d7number = len(d7)
    d38number = len(d38)
    d51number = len(d51)

    sout = "Active doctors: {0} + {1} + {2} + {3}".format(d1number, d7number, d38number, d51number)
    log.info(sout)

    cur = dbc.con.cursor()

    s_sql_enp = """SELECT enp FROM peoples WHERE people_id = ?;"""

    s_sql_ap = """SELECT
ap.area_people_id, ap.area_id_fk, ap.date_beg, ap.motive_attach_beg_id_fk,
ca.clinic_id_fk,
ar.area_number, ar.area_id,
ca.speciality_id_fk
FROM area_peoples ap
LEFT JOIN areas ar ON ap.area_id_fk = ar.area_id
LEFT JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
WHERE ap.people_id_fk = ?
AND ca.clinic_id_fk = ?
AND ca.basic_speciality = 1
AND ca.speciality_id_fk IN (1,7,38,51)
AND ap.date_end is Null
ORDER BY ap.date_beg DESC;"""

    insorgs = InsorgInfoList()

    stime  = time.strftime("%Y%m%d")
    fnameb = FNAMEb.format(mcod, stime)
    sout = "Output to file: {0}".format(fnameb)
    log.info(sout)

    f_fnameb = IN_PATH + "/" + fnameb
    fob = open(f_fnameb, "wb")

    count    = 0
    count_p  = 0
    count_m  = 0
    noicc    = 0

    p_ids    = []
    for p_obj in patient_list:

        p_id = p_obj.people_id

        count += 1

        insorg_id   = p_obj.insorg_id
        try:
            insorg = insorgs[insorg_id]
        except:
            sout = "People_id: {0}. Have not got insorg_id: {1}".format(p_id, insorg_id)
            log.debug(sout)
            insorg = insorgs[0]
            noicc += 1

        if count % STEP == 0:
            sout = " {0} people_id: {1}".format(count, p_id)
            log.info(sout)

        cur.execute(s_sql_enp,(p_id, ))
        rec = cur.fetchone()
        if rec is not None:
            enp = rec[0]
        else:
            enp = None

        if enp is None:
            if (p_obj.medical_insurance_series is None) and \
               (p_obj.medical_insurance_number is not None) and \
               (len(p_obj.medical_insurance_number) == 16):
                p_obj.enp = p_obj.medical_insurance_number
        else:
            p_obj.enp = enp

        f_oms_series = p_obj.medical_insurance_series
        f_oms_number = p_obj.medical_insurance_number

        f_enp        = p_obj.enp
        f_mcod       = mcod
        f_ocato      = OCATO
        if f_enp is None: continue

        cur.execute(s_sql_ap,(p_id, clinic_id, ))
        rec = cur.fetchone()

        l_print = False
        if rec is not None:
            date_beg = rec[2]
            motive_attach = rec[3]
            if motive_attach == 1:
                matt = 1
            else:
                matt = 2

            area_number = rec[5]
            area_id = rec[6]
            speciality_id = rec[7]
            if cad.has_key(area_id):
                d_snils = cad[area_id][2]
            elif FIND_DOCTOR:
                # http://stackoverflow.com/questions/4859292/how-to-get-a-random-value-in-python-dictionary
                # http://stackoverflow.com/questions/1058712/how-do-i-select-a-random-element-from-an-array-in-python
                if speciality_id == 1:
                    d_snils = random.choice(d1)
                elif speciality_id == 7:
                    d_snils = random.choice(d7)
                elif speciality_id == 38:
                    d_snils = random.choice(d38)
                elif speciality_id == 51:
                    d_snils = random.choice(d51)
            else:
                if EMPTY_DOCTOR:
                    d_snils = u"11111111111"
                else:
                    continue

            if speciality_id == 51:
                doc_category = 2
            else:
                doc_category = 1

            if not SET_DOC_CATEGORY:
                doc_category = None

            sss = p3(p_obj, mcod, matt, date_beg, ADATE_ATT, ASSIGN_ATT, \
                     area_number, d_snils, doc_category, ACTION) + "\r\n"

            ps = sss.encode('windows-1251')

            fob.write(ps)
            fob.flush()
            os.fsync(fob.fileno())
            count_p += 1

    fob.close()

    sout = "Totally {0} of {1} patients have been printed".format(count_p, count)
    log.info( sout )

def pclinic(clinic_id, mcod):
    from dbmis_connect2 import DBMIS
    from PatientInfo import get_patient_list
    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('Patient - Doctor Start {0}'.format(localtime))

    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
    if dbc.ogrn == None:
        CLINIC_OGRN = u""
    else:
        CLINIC_OGRN = dbc.ogrn

    cogrn = CLINIC_OGRN.encode('utf-8')
    cname = dbc.name.encode('utf-8')

    sout = "clinic_id: {0} cod_mo: {1} clinic_name: {2} clinic_ogrn: {3}".format(clinic_id, mcod, cname, cogrn)
    log.info(sout)

    if DATE_RANGE is None:
        s_sqlt = """SELECT * FROM vw_peoples p
JOIN area_peoples ap ON p.people_id = ap.people_id_fk
JOIN areas ar ON ap.area_id_fk = ar.area_id
JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
WHERE ca.clinic_id_fk = {0}
AND ca.basic_speciality = 1
AND ca.speciality_id_fk IN (1,7,38,51)
AND ap.date_end is Null;"""
        s_sql = s_sqlt.format(clinic_id)
        sout = "Don't use date_range"
        log.info(sout)
    else:
        D1 = DATE_RANGE[0]
        D2 = DATE_RANGE[1]
        s_sqlt = """SELECT * FROM vw_peoples p
JOIN area_peoples ap ON p.people_id = ap.people_id_fk
JOIN areas ar ON ap.area_id_fk = ar.area_id
JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
WHERE ca.clinic_id_fk = {0}
AND ca.basic_speciality = 1
AND ca.speciality_id_fk IN (1,7,38,51)
AND ap.date_beg >= '{1}'
AND ap.date_beg <= '{2}'
AND ap.date_end is Null;"""
        s_sql = s_sqlt.format(clinic_id, D1, D2)
        sout = "date_range: [{0}] - [{1}]".format(D1, D2)
        log.info(sout)


    if ASSIGN_ATT:
        sout = "ADATE_ATT: {0}".format(ADATE_ATT)
        log.info(sout)
    else:
        sout = "Set actual ATTACH DATE"
        log.info(sout)

    if FIND_DOCTOR:
        sout = "Find random doctor if area has not got assigned doctor"
        log.info(sout)
    else:
        sout = "If area has not got assigned doctor then do not print"
        log.info(sout)

    if EMPTY_DOCTOR:
        sout = "Print patients not having doctors"
        log.info(sout)
    else:
        sout = "Do not print patients not having doctors"
        log.info(sout)


    sout = "Action: '{0}'".format(ACTION.encode('utf-8'))
    log.info(sout)

    if SET_DOC_CATEGORY:
        sout = "Print doctor's category"
        log.info(sout)
    else:
        sout = "Do not print doctor's category"
        log.info(sout)

    cursor = dbc.con.cursor()
    cursor.execute(s_sql)
    results = cursor.fetchall()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Patients have been selected on {0}'.format(localtime))

    patient_list = get_patient_list(results)

    npatients = len(patient_list)
    sout = "clinic has got {0} unique patients".format(npatients)
    log.info( sout )

    plist(dbc, clinic_id, mcod, patient_list)

    dbc.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Patient - Doctor Finish  '+localtime)

def get_1clinic_lock(id_unlock = None):

    dnow = datetime.datetime.now()

    dbmy = DBMY()
    curm = dbmy.con.cursor()

    if id_unlock is not None:
        ssql = "UPDATE insr_list SET done = %s, c_lock = Null WHERE id = %s;"
        curm.execute(ssql, (dnow, id_unlock, ))
        dbmy.con.commit()

    ssql = "SELECT id, clinic_id, mcod FROM insr_list WHERE (done is Null) AND (c_lock is Null);"
    curm.execute(ssql)
    rec = curm.fetchone()

    if rec is not None:
        _id  = rec[0]
        c_id = rec[1]
        mcod = rec[2]
        c_rec = [_id, c_id, mcod]
        ssql = "UPDATE insr_list SET c_lock = 1 WHERE id = %s;"
        curm.execute(ssql, (_id, ))
        dbmy.con.commit()
    else:
        c_rec = None

    dbmy.close()
    return c_rec

if __name__ == "__main__":

    log.info("======================= INSB-CAD ===========================================")

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info( sout )

    c_rec  = get_1clinic_lock()
    while c_rec is not None:
        _id = c_rec[0]
        clinic_id = c_rec[1]
        mcod = c_rec[2]
        sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod)
        log.debug(sout)
        if clinic_id is not None:
            pclinic(clinic_id, mcod)
        c_rec  = get_1clinic_lock(_id)

    sys.exit(0)
