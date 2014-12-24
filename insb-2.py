#!/usr/bin/python
# -*- coding: utf-8 -*-
# insb-2.py - обработка страховой пренадлежности
#             Insurance Belongins Request (IBR)
#             сравнение данных DBMIS и MySQL(mis.sm)
#             использовать ENP, серию и номер пролисов ОМС из ответов ТФОМС
#

import os
import sys, codecs
import ConfigParser
import logging

from dbmysql_connect import DBMY

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insb2.out'
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

# [Insb]
Config2 = ConfigSectionMap(Config, "Insb")
ADATE_ATT = Config2['adate_att']

#clist = [220021, 220022, 220034, 220036, 220037, 220040, 220042, 220043, 220045, 220048, 220051, 220059, 220060, 220062, 220063, 220064, 220068, 220073, 220074, 220078, 220079, 220080, 220081, 220083, 220085, 220091, 220093, 220094, 220097, 220138, 220140, 220152, 220041]
clist = [220011, 220001, 220014]

cid_list = [95,98,101,105,110,119,121,124,125,127,128,131,133,134,140,141,142,145,146,147,148,150,151,152,157,159,160,161,162,163,165,166,167,168,169,170,174,175,176,177,178,180,181,182,186,192,198,199,200,205,206,208,210,213,215,220,222,223,224,226,227,230,232,233,234,235,236,237,238,239,240,330,381]

CLINIC_OGRN = u""

FNAMEa = "AM{0}{1}.csv" # нет среди ответов фонда
FNAMEb = "MO2{0}{1}.csv" # в ТФОМС на внесение изменений
FNAMEx = "SD{0}{1}.xls" # паценты с несколькими прикреплениями

SD2DO_PATH = "./SD2DO"
IN_PATH    = "./FIN"
R_PATH     = "./RESULTS"

STEP = 1000

CID_LIST   = False # Use cid_lis (list of clinic_id)

MLIST      = False # Use mis.mlist table (MySQL)
ILIST      = True  # Use mis.insr_list table (MySQL)

OCATO      = '01000'

PRINT2     = False

#DATE_RANGE = None
#DATE_RANGE = ['2014-12-01','2014-12-31']
DATE_RANGE = [D_START,D_FINISH]
PRINT_ALL  = True # include all patients into MO files

def get_clist(db):

    if MLIST:
	s_sql = "SELECT DISTINCT mcod FROM mlist WHERE done is Null;"
    else:
	s_sql = "SELECT DISTINCT mcod FROM insr_list WHERE done is Null;"

    cur = db.con.cursor()
    cur.execute(s_sql)
    result = cur.fetchall()

    ar = []

    for rec in result:
        mcod = rec[0]
        ar.append(mcod)

    return ar

def register_cdone(db, clinic_id):
    import datetime

    dnow = datetime.datetime.now()
    sdnow = str(dnow)

    if MLIST:
        s_sql = """UPDATE mlist
        SET done = %s
        WHERE clinic_id = %s;"""
    else:
        s_sql = """UPDATE insr_list
        SET done = %s
        WHERE clinic_id = %s;"""


    cur = db.con.cursor()
    cur.execute(s_sql, (sdnow, clinic_id))


def plist(dbc, clinic_id, mcod, patient_list):
    import xlwt
    from PatientInfo import p1, p2
    from insorglist import InsorgInfoList

    import os
    import datetime
    import time

    dbmy = DBMY()
    curr = dbmy.con.cursor()

    s_sqlf = """SELECT oms_series, oms_number, enp, mcod, ocato, smo_code
    FROM
    sm
    WHERE people_id = %s"""

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
    fnamea = FNAMEa.format(mcod, stime)
    fnameb = FNAMEb.format(mcod, stime)
    fnamex = FNAMEx.format(mcod, stime)
    sout = "Output to files: {0} | {1} | {2}".format(fnamea, fnameb, fnamex)
    log.info(sout)

    f_fnamea = R_PATH  + "/" + fnamea
    foa = open(f_fnamea, "wb")
    f_fnameb = IN_PATH + "/" + fnameb
    fob = open(f_fnameb, "wb")

    wb = xlwt.Workbook(encoding='cp1251')
    ws = wb.add_sheet('Patients')

    sout = u"clinic_id: {0}".format(clinic_id)
    ws.write(0,0,sout)
    ws.write(2,0,u"id")
    ws.write(2,1,u"date_beg")
    ws.write(2,2,u"motive")
    ws.write(2,3,u"clinic_id")

    ws_row = 3

    count    = 0
    count_e  = 0
    count_a  = 0
    count_b  = 0
    count_m  = 0
    count_np = 0
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

        curr.execute(s_sqlf,(p_id,))
        rec = curr.fetchone()
        if rec is None:
            count_a += 1
            if (p_obj.medical_insurance_series is None) and \
               (p_obj.medical_insurance_number is not None) and \
               (len(p_obj.medical_insurance_number) == 16):
		p_obj.enp = p_obj.medical_insurance_number

            sss = p1(p_obj, insorg) + "|\n"
            ps = sss.encode('windows-1251')

            foa.write(ps)

            foa.flush()
            os.fsync(foa.fileno())

            f_oms_series = p_obj.medical_insurance_series
            f_oms_number = p_obj.medical_insurance_number
            f_enp        = p_obj.enp
            f_mcod       = mcod
            f_ocato      = OCATO
            if f_enp is None: continue
        else:
            f_oms_series = rec[0]
            f_oms_number = rec[1]
            f_enp        = rec[2]
            f_mcod       = rec[3]
            f_ocato      = rec[4]

            p_obj.enp = f_enp
            p_obj.medical_insurance_series = f_oms_series
            p_obj.medical_insurance_number = f_oms_number

            if mcod == f_mcod:
                count_e += 1
                if not PRINT_ALL: continue
            else:
                count_b += 1

        cur.execute(s_sql_ap,(p_id, ))
        recs_ap = cur.fetchall()

        l_print = False
        if (len(recs_ap) == 1):
            if (f_ocato == OCATO):
                date_beg = recs_ap[0][2]
                sss = p2(p_obj, mcod, 2, date_beg, ADATE_ATT) + "\r\n"
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
                    sss = p2(p_obj, mcod, 2, date_beg, ADATE_ATT) + "\r\n"
                    ps = sss.encode('windows-1251')
                    l_print = True


        if l_print:
            fob.write(ps)

            fob.flush()
            os.fsync(fob.fileno())
        else:
            count_np += 1

    foa.close()
    fob.close()
    f_fname = SD2DO_PATH + "/" + fnamex
    wb.save(f_fname)

    sout = "Totally {0} of {1} patients have got mcod equal to TFOMS".format(count_e, count)
    log.info( sout )
    sout = "{0} patients have not been identified by TFOMS".format(count_a)
    log.info( sout )
    sout = "{0} patients have not got mcod equal to TFOMS".format(count_b)
    log.info( sout )
    sout = "{0} patients have got few attachments".format(count_m)
    log.info( sout )
    sout = "{0} patients have not been printed out".format(count_np)
    log.info( sout )

    # write results into MySQL DB
    dnow = datetime.datetime.now()
    sdnow = str(dnow)

    s_sql = """SELECT id FROM iba WHERE clinic_id = %s;"""
    curr.execute(s_sql,(clinic_id,))
    rec = curr.fetchone()
    if rec is None:
        s_sql = """INSERT INTO iba
        (clinic_id, mcod, done, count, count_e, count_a, count_m, count_np)
        VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s);"""
        curr.execute(s_sql,(clinic_id, mcod, sdnow, count, count_e, count_a, count_m, count_np, ))
        dbmy.con.commit()
    else:
        _id = rec[0]
        s_sql = """UPDATE iba
        SET
        done = %s,
        count = %s,
        count_e = %s,
        count_a = %s,
        count_m = %s,
        count_np  = %s
        WHERE id = %s;"""
        curr.execute(s_sql,(sdnow, count, count_e, count_a, count_m, count_np, _id, ))
        dbmy.con.commit()

    dbmy.close()

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


    sout = "ADATE_ATT: {0}".format(ADATE_ATT)
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

    log.info("======================= INSB-2 ===========================================")

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info( sout )

    if CID_LIST:
        for clinic_id in cid_list:
            try:
                mcod = modb.moCodeByMisId(clinic_id)
                sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod)
                log.debug(sout)
            except:
                sout = "Have not got clinic for clinic_id {0}".format(clinic_id)
                log.warn(sout)
                mcod = 0
                continue

            pclinic(clinic_id, mcod)
    elif ILIST:
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
    else:
        if MLIST:
            dbmy = DBMY()
            clist = get_clist(dbmy)
            mcount = len(clist)
            sout = "Totally {0} MO to be processed".format(mcount)
            log.info( sout )
        for mcod in clist:
            try:
                mo = modb[mcod]
                clinic_id = mo.mis_code
                sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod)
                log.debug(sout)
            except:
                sout = "Have not got clinic for MO Code {0}".format(mcod)
                log.warn(sout)
                clinic_id = 0
                continue

            pclinic(clinic_id, mcod)
            if MLIST:
                register_cdone(dbmy, clinic_id)

    if MLIST:
        dbmy.close()
    sys.exit(0)
