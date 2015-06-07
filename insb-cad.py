#!/usr/bin/python
# -*- coding: utf-8 -*-
# insb-cad.py - выгрузка пациент-врач
#               задача 1936
#

import os
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
S_USE_DATE_RANGE = Config2['use_date_range']
if S_USE_DATE_RANGE == "1":
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

CLINIC_OGRN = u""

FNAMEb = "MO2{0}{1}.csv" # в ТФОМС

IN_PATH    = "./FIN"

STEP = 1000

OCATO      = '01000'

PRINT2     = False

PRINT_ALL  = True # include all patients into MO files

def plist(dbc, clinic_id, mcod, patient_list):
    from PatientInfo import p1, p2
    from insorglist import InsorgInfoList

    import os
    import datetime
    import time

    cur = dbc.con.cursor()

    s_sql_ap = """SELECT
ap.area_people_id, ap.area_id_fk, ap.date_beg, ap.motive_attach_beg_id_fk,
ca.clinic_id_fk
FROM area_peoples ap
LEFT JOIN areas ar ON ap.area_id_fk = ar.area_id
LEFT JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
WHERE ap.people_id_fk = ? AND ca.basic_speciality = 1
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

        if (p_obj.medical_insurance_series is None) and \
           (p_obj.medical_insurance_number is not None) and \
           (len(p_obj.medical_insurance_number) == 16):
            p_obj.enp = p_obj.medical_insurance_number

        f_oms_series = p_obj.medical_insurance_series
        f_oms_number = p_obj.medical_insurance_number
        f_enp        = p_obj.enp
        f_mcod       = mcod
        f_ocato      = OCATO
        if f_enp is None: continue

        cur.execute(s_sql_ap,(p_id, ))
        recs_ap = cur.fetchall()

        l_print = False
        if (len(recs_ap) == 1):
            if (f_ocato == OCATO):
                date_beg = recs_ap[0][2]
                motive_attach = recs_ap[0][3]
                if motive_attach == 1:
                    matt = 1
                else:
                    matt = 2
                sss = p2(p_obj, mcod, matt, date_beg, ADATE_ATT, ASSIGN_ATT) + "\r\n"
                ps = sss.encode('windows-1251')
                l_print = True

        else:
            count_m += 1
            for rec_ap in recs_ap:
                area_people_id = rec_ap[0]
                area_id_fk     = rec_ap[1]
                date_beg       = rec_ap[2]
                motive_attach  = rec_ap[3]
                clinic_id_fk   = rec_ap[4]
                if PRINT2:
                    sout = "people_id: {0} date_beg: {1} motive_attach: {2} clinic_id: {3}".format(p_id, date_beg, motive_attach, clinic_id_fk)
                    log.info( sout )

                ws_row += 1
                ws.write(ws_row,0,p_id)
                if date_beg is None:
                    s_date_beg = u"None"
                else:
                    s_date_beg = u"%04d-%02d-%02d" % (date_beg.year, date_beg.month, date_beg.day)
                ws.write(ws_row,1,s_date_beg)
                ws.write(ws_row,2,motive_attach)
                ws.write(ws_row,3,clinic_id_fk)

                if motive_attach in (None, 3, 9):
                    motive_attach = 2

                if (motive_attach in (2,3)) and (clinic_id == clinic_id_fk) and (not l_print) and (f_ocato == OCATO):
                    sss = p2(p_obj, mcod, 2, date_beg, ADATE_ATT, ASSIGN_ATT) + "\r\n"
                    ps = sss.encode('windows-1251')
                    l_print = True


        if l_print:
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
    log.info('Insurance Belongings Analysis Start {0}'.format(localtime))

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
WHERE ca.clinic_id_fk = {0} AND ca.basic_speciality = 1
AND ap.date_end is Null;"""
        s_sql = s_sqlt.format(clinic_id)
    else:
        D1 = DATE_RANGE[0]
        D2 = DATE_RANGE[1]
        s_sqlt = """SELECT * FROM vw_peoples p
JOIN area_peoples ap ON p.people_id = ap.people_id_fk
JOIN areas ar ON ap.area_id_fk = ar.area_id
JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
WHERE ca.clinic_id_fk = {0} AND ca.basic_speciality = 1
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
    log.info('Insurance Belongings Analysis Finish  '+localtime)

def get_1clinic_lock(id_unlock = None):
    import datetime
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

    import os
    import datetime

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
