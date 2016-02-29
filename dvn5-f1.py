#!/usr/bin/python
# -*- coding: utf-8 -*-
# dvn5-f1.py - формирование карт ДВН
# для всех лиц, прикрепленных к ЛПУ,
# подходящим по возрасту для ДВН
# статус прикрепления, которых равен 1 (соответствует данным ТФОМС)
#
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()
from insorglist import InsorgInfoList
insorgs = InsorgInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_dvn5f1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
#DB   = "DBMIS"
DB   = "DVN5"

CLINIC_OGRN = u""

STEP = 100

SKIP_OGRN = True

DS_WHITE_LIST = []
DS_WHITE_COUNT = 395

HEALTH_GROUP_1 = 1
RESULT_1 = 317
DS_1 = 'Z00.0'
PEOPLE_STATUS_CODE = 3

from dvn_config import D_DATE_STAGE_1, D_DATE_END_1
from dvn_config import mo_list

def get_wlist(fname="ds_white_list.xls"):
    import xlrd
    workbook = xlrd.open_workbook(fname)
    worksheets = workbook.sheet_names()
    wshn0 = worksheets[0]
    worksheet = workbook.sheet_by_name(wshn0)
    curr_cell = 0
    for curr_row in range(DS_WHITE_COUNT):
        cell_value = worksheet.cell_value(curr_row, curr_cell)
        DS_WHITE_LIST.append(cell_value)

    workbook.release_resources()

def check_person(db, people_id):
    import datetime
    s_sqlt = """SELECT t.ticket_id, td.diagnosis_id_fk
FROM tickets t
LEFT JOIN ticket_diagnosis td ON t.ticket_id = td.ticket_id_fk
WHERE t.people_id_fk = {0} and visit_date > '2013-01-01';"""
    s_sql = s_sqlt.format(people_id)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    records = cursor.fetchall()
    for rec in records:
        dss = rec[1]
        if dss == None: continue
        ds = dss.strip()
        if ds not in DS_WHITE_LIST:
            return False
    return True

def person_in_cc(db, people_id):
    # check if clinical_checkups table already has got a record with current people_id
    s_sqlt = "SELECT * FROM clinical_checkups WHERE people_id_fk = {0};"
    s_sql  = s_sqlt.format(people_id)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    rec = cursor.fetchone()
    if rec == None:
        return False
    else:
        return True

def add_cc(db, clinic_id, people_id):
    # create record in the clinical_checkups table
    import sys
    from dbmis_connect2 import dset
    from datetime import datetime, timedelta
    d1 = D_DATE_STAGE_1[0]
    d2 = D_DATE_STAGE_1[1]
    DATE_STAGE_1 = dset(d1, d2)
    if D_DATE_END_1[0] > DATE_STAGE_1:
        d1 = D_DATE_END_1[0]
    else:
        d1 = DATE_STAGE_1
        dd1 = datetime.strptime(d1, "%Y-%m-%d").date()
        dd2 = dd1 + timedelta(days=1)
        if dd2.weekday() == 5: dd2 = dd1 + timedelta(days=3)
        d1 = "%04d-%02d-%02d" % (dd2.year, dd2.month, dd2.day)
    d2 = D_DATE_END_1[1]
    DATE_END_1 = dset(d1, d2)
    s_sqlt = """INSERT INTO clinical_checkups
    (clinic_id_fk, people_id_fk, date_stage_1, date_end_1,
    health_group_1, result_1, ds_1,
    people_status_code)
    VALUES
    ({0}, {1}, '{2}', '{3}', {4}, {5}, '{6}', {7});"""
    s_sql = s_sqlt.format(clinic_id, people_id, DATE_STAGE_1, DATE_END_1, HEALTH_GROUP_1, RESULT_1, DS_1, PEOPLE_STATUS_CODE)
    cursor = db.con.cursor()
    try:
        cursor.execute(s_sql)
        db.con.commit()
    except:
        log.warn('Create record in the clinical_checkups table Error.\SQL Code:\n')
        log.warn(s_sql)
    s_sqlt = """SELECT
    clinical_checkup_id
    FROM clinical_checkups
    WHERE people_id_fk = {0};"""
    s_sql = s_sqlt.format(people_id)
    cursor.execute(s_sql)
    rec = cursor.fetchone()
    if rec == None:
        return 0
    else:
        return rec[0]

def set_document(db, people_id):
    s_sqlt = """UPDATE peoples
    SET
    document_type_id_fk = 14,
    document_series = '01 01',
    document_number = '111111'
    WHERE
    people_id = {0};"""
    s_sql = s_sqlt.format(people_id)
    cursor = db.con.cursor()
    try:
        cursor.execute(s_sql)
        db.con.commit()
    except:
        log.warn('Set document error.\nSQL Code:\n')
        log.warn(s_sql)

def register_cc(dbmy, cc_id, people_id, clinic_id):
    import datetime
    today = datetime.datetime.today()
    d_now = "%04d-%02d-%02d" % (today.year, today.month, today.day)

    # look for people_id, clinic_id
    s_sqlt = """SELECT id FROM clinical_checkups
    WHERE people_id = {0};"""
    s_sql = s_sqlt.format(people_id)
    cursor = dbmy.con.cursor()
    cursor.execute(s_sql)
    rec = cursor.fetchone()
    if rec == None:
        # register clinical_checkup in the MySQL database (ct208.ctmed.ru:mis)
	s_sqlt = """INSERT INTO clinical_checkups
	(cc_id, people_id, cc_dcreated, clinic_id)
	VALUES
	({0}, {1}, '{2}', {3})
	"""
	s_sql = s_sqlt.format(cc_id, people_id, d_now, clinic_id)
    else:
	t_id = rec[0]
	s_sqlt = """UPDATE clinical_checkups
	SET
	cc_id = {0},
	cc_dcreated = '{1}',
	clinic_id ={2}
	WHERE id = {3};"""
	s_sql = s_sqlt.format(cc_id, d_now, clinic_id, t_id)

    cursor = dbmy.con.cursor()
    cursor.execute(s_sql)
    dbmy.con.commit()

def get_p_list(clinic_id):
    import fdb
    from datetime import datetime
    from dbmis_connect2 import DBMIS

    sd1 = D_DATE_STAGE_1[0]
    d1  = datetime.strptime(sd1, '%Y-%m-%d')
    y1  = d1.year


    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
    cur = dbc.con.cursor()
    cur2= dbc.con.cursor()


    s_sqlt = """SELECT DISTINCT p.people_id, p.birthday,
    p.insorg_id, p.medical_insurance_series, p.medical_insurance_number,
    p.document_series, p.document_number,
    p.document_type_id_fk
FROM peoples p
JOIN area_peoples ap ON p.people_id = ap.people_id_fk
JOIN areas ar ON ap.area_id_fk = ar.area_id
JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
WHERE ca.clinic_id_fk = ? AND ca.basic_speciality = 1
AND ap.date_end is Null;"""

    SQLT_APR = """SELECT
    ap.tfoms_verification_status_id_fk, ap.tfoms_verification_date,
    ap.area_people_id, ap.area_id_fk, ap.motive_attach_beg_id_fk,
    ar.clinic_area_id_fk,
    ca.speciality_id_fk, ca.basic_speciality
    FROM area_peoples ap
    LEFT JOIN areas ar ON ap.area_id_fk = ar.area_id
    LEFT JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
    WHERE ap.people_id_fk = ?
    AND ap.date_end is Null
    AND ca.clinic_id_fk = ?
    AND ca.basic_speciality = 1;"""

    dbc.con.begin(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    cur.execute(s_sqlt, (clinic_id, ))
    results = cur.fetchall()
    dbc.con.commit()

    p_arr = []
    for rec in results:
	people_id = rec[0]
	birthday  = rec[1]
	insorg_id = rec[2]
	oms_s     = rec[3]
	oms_n     = rec[4]
	doc_s     = rec[5]
	doc_n     = rec[6]
	doc_t     = rec[7]
	p_by = birthday.year
	age = y1 - p_by
	agem = age % 3
	if (age <= 20) or (agem !=0): continue

	cur2.execute(SQLT_APR, (people_id, clinic_id, ))
	rec2 = cur2.fetchone()
	if rec2 is None: continue
	v_status = rec2[0]
	# Статус проверки прикрепления ТФОМС (соответствует данным ТФОМС)
	if v_status != 1: continue
	# Тип документа - Паспорт РФ
	if doc_t != 14: continue
	if doc_n is None or len(doc_n) == 0: continue
	if oms_n is None or len(oms_n) == 0: continue
	if insorg_id is None: continue

	p_arr.append([people_id, insorg_id, oms_s, oms_n, doc_s, doc_n, doc_t])

    dbc.close()

    return p_arr

def process_p_list(clinic_id, p_list):
    from dbmis_connect2 import DBMIS
    from dbmysql_connect import DBMY
    from PatientInfo2 import PatientInfo2

    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('DVN List Processing Start {0}'.format(localtime))


    ppp = p_list

    p_obj = PatientInfo2()

    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
    if dbc.ogrn == None:
        CLINIC_OGRN = u""
    else:
        CLINIC_OGRN = dbc.ogrn

    cogrn = CLINIC_OGRN.encode('utf-8')
    cname = dbc.name.encode('utf-8')

    if SKIP_OGRN: CLINIC_OGRN = u""

    if dbc.clinic_areas == None:
        sout = "Clinic has not got any areas"
        log.warn(sout)
        dbc.close()
        return
    else:
        nareas = len(dbc.clinic_areas)
        area_id = dbc.clinic_areas[0][0]
        area_nu = dbc.clinic_areas[0][1]
        sout = "Clinic has got {0} areas".format(nareas)
        log.info(sout)
        sout = "Using area_id: {0} area_number: {1}".format(area_id, area_nu)
        log.info(sout)

    wrong_clinic = 0
    wrong_insorg = 0
    ncount = 0
    dbc2 = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
    cur2 = dbc2.con.cursor()
    dbmy = DBMY()

    dvn_number = 0

    for prec in ppp:
        ncount += 1
        people_id = prec[0]
        insorg_id = prec[1]
        if insorg_id is None:
            insorg_id = 0

        p_obj.initFromDb(dbc, people_id)

        if ncount % STEP == 0:
            sout = " {0} people_id: {1} clinic_id: {2} dvn_number: {3}".format(ncount, people_id, p_obj.clinic_id, dvn_number)
            log.info(sout)

	# check if person already
	# has got a record in the clinical_checkups table
	if person_in_cc(dbc, people_id): continue

	lname = p_obj.lname
	fname = p_obj.fname
	mname = p_obj.mname
	bd    = p_obj.birthday

        if check_person(dbc, people_id):

            # check if clinical_checkups table

            cc_id = add_cc(dbc2, clinic_id, people_id)

            register_cc(dbmy, cc_id, people_id, clinic_id)

            dvn_number += 1

    sout = "DVN cases number: {0}".format(dvn_number)
    log.info(sout)

    dbc.close()
    dbc2.close()
    dbmy.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('DVN List Processing Finish  '+localtime)


if __name__ == "__main__":

    import os, shutil
    from dbmysql_connect import DBMY

    log.info("======================= DVN5-F1 ===========================================")
    sout = "HOST: {0} DB: {1}".format(HOST, DB)
    log.info(sout)

    get_wlist()

    for mcod in mo_list:
	try:
	    mo = modb[mcod]
	    clinic_id = mo.mis_code
	    clinic_name = mo.name
	    sout = "clinic_id: {0} mcod: {1} Clinic Name: {2}".format(clinic_id, mcod, clinic_name.encode("utf-8"))
	    log.info(sout)
	except:
	    sout = "Clinic not found for mcod = {0}".format(s_mcod)
	    log.warn(sout)
	    continue

	p_list = get_p_list(clinic_id)

	nnn = len(p_list)
	sout = "Clinic has got {0} patients having appropriate age".format(nnn)
	log.info(sout)

	process_p_list(clinic_id, p_list)

	#pfile(f_fname)


    sys.exit(0)
