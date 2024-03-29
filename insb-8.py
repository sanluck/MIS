#!/usr/bin/python
# -*- coding: utf-8 -*-
# insb-8.py - Сформировать файлы МО
#             для пациентов, прикрепленных в период времени
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insb8.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

#clist = [220021, 220022, 220034, 220036, 220037, 220040, 220042, 220043, 220045, 220048, 220051, 220059, 220060, 220062, 220063, 220064, 220068, 220073, 220074, 220078, 220079, 220080, 220081, 220083, 220085, 220091, 220093, 220094, 220097, 220138, 220140, 220152, 220041]
clist = [220011, 220001, 220014]

cid_list = [95,98,101,105,110,119,121,124,125,127,128,131,133,134,140,141,142,145,146,147,148,150,151,152,157,159,160,161,162,163,165,166,167,168,169,170,174,175,176,177,178,180,181,182,186,192,198,199,200,205,206,208,210,213,215,220,222,223,224,226,227,230,232,233,234,235,236,237,238,239,240,330,381]

CLINIC_OGRN = u""

FNAMEb = "MO2{0}{1}.csv" # в ТФОМС на внесение изменений
FNAMEx = "SD{0}{1}.xls" # паценты с несколькими прикреплениями

IN_PATH    = "./FIN"
SD2DO_PATH = "./SD2DO"

STEP = 1000

CID_LIST   = False # Use cid_lis (list of clinic_id)

ILIST      = True  # Use mis.insr_list table (MySQL)

OCATO      = '01000'

PRINT2     = False

#DATE_RANGE = None
DATE_RANGE = ['2014-05-01','2014-05-31']


def get_clist(db):
    
    s_sql = "SELECT DISTINCT mcod FROM insr_list WHERE ddone is Null;"
    
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
    
    s_sql = """UPDATE insr_list
    SET ddone = %s
    WHERE clinic_id = %s;"""
    
    cur = db.con.cursor()
    cur.execute(s_sql, (sdnow, clinic_id))


def plist(dbc, clinic_id, mcod, patient_list):
    import xlwt
    from dbmysql_connect import DBMY
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
    
    s_sql_enp = """SELECT enp FROM peoples WHERE people_id = ?;"""
    
    insorgs = InsorgInfoList()

    stime  = time.strftime("%Y%m%d")
    fnameb = FNAMEb.format(mcod, stime)
    fnamex = FNAMEx.format(mcod, stime)
    sout = "Output to files: {0} | {1}".format( fnameb, fnamex )
    log.info(sout)

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
    count_a  = 0
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
	    cur.execute(s_sql_enp,(p_id, ))
	    rec_enp = cur.fetchone()
	    if rec_enp is None:
		p_obj.enp = None
		count_np += 1
		continue
	    if rec_enp[0] is None:
		p_obj.enp = None
		count_np += 1
		continue
	    p_obj.enp = rec_enp[0]
	else:
	    f_oms_series = rec[0]
	    f_oms_number = rec[1]
	    f_enp        = rec[2]
	    f_mcod       = rec[3]
	    f_ocato      = rec[4]
	    f_smo_code   = rec[5]

	    p_obj.enp = f_enp
	    p_obj.medical_insurance_series = f_oms_series
	    p_obj.medical_insurance_number = f_oms_number

	cur.execute(s_sql_ap,(p_id, ))
	recs_ap = cur.fetchall()

	l_print = False
	if (len(recs_ap) == 1):
	    date_beg = recs_ap[0][2]
	    motive_attach  = recs_ap[0][3]
	    if motive_attach not in (0, 1, 2):
		motive_attach = 2
	    sss = p2(p_obj, insorg, mcod, motive_attach, date_beg) + "\r\n"
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
			
		if motive_attach not in (0, 1, 2):
		    motive_attach = 2
			
		if (clinic_id == clinic_id_fk) and (not l_print):
		    sss = p2(p_obj, insorg, mcod, motive_attach, date_beg) + "\r\n"
		    ps = sss.encode('windows-1251')
		    l_print = True
			    
		
		if l_print:
		    fob.write(ps)
		    fob.flush()
		    os.fsync(fob.fileno())
		else:
		    count_np += 1

    fob.close()
    f_fname = SD2DO_PATH + "/" + fnamex
    wb.save(f_fname)

    sout = "Totally {0} patients have been processed".format(count)
    log.info( sout )
    sout = "{0} patients have not been identified by TFOMS".format(count_a)
    log.info( sout )
    sout = "{0} patients have got few attachments".format(count_m)
    log.info( sout )
    sout = "{0} patients have not been printed out".format(count_np)
    log.info( sout )

    dbmy.close()

def pclinic(clinic_id, mcod):
    from dbmis_connect2 import DBMIS
    from PatientInfo import get_patient_list
    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('Create MO File. Start {0}'.format(localtime))

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
	
    cursor = dbc.con.cursor()
    cursor.execute(s_sql)
    results = cursor.fetchall()
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Patients has been selected {0}'.format(localtime))
    
    patient_list = get_patient_list(results)
    
    npatients = len(patient_list)
    sout = "clinic has got {0} unique patients".format(npatients)
    log.info( sout )
    
    plist(dbc, clinic_id, mcod, patient_list)
    
    dbc.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Create MO File. Finish  '+localtime)


if __name__ == "__main__":
    
    import os    
    import datetime
    from dbmysql_connect import DBMY

    log.info("======================= INSB-8 ===========================================")

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info( sout )
    
    if DATE_RANGE is not None:
	sout = "Date Range: ['{0}','{1}']".format(DATE_RANGE[0], DATE_RANGE[1])
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
    else:
	if ILIST: 
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
	    if ILIST:
		register_cdone(dbmy, clinic_id)
	
    if ILIST:
	dbmy.close()
    sys.exit(0)
