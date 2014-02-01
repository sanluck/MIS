#!/usr/bin/python
# -*- coding: utf-8 -*-
# insb-2.py - обработка страховой пренадлежности
#             Insurance Belongins Request (IBR)
#             сравнение данных DBMIS и MySQL(mis.sm)
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insb2.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb.ctmed.ru"
DB   = "DBMIS"

#clist = [220021, 220022, 220034, 220036, 220037, 220040, 220042, 220043, 220045, 220048, 220051, 220059, 220060, 220062, 220063, 220064, 220068, 220073, 220074, 220078, 220079, 220080, 220081, 220083, 220085, 220091, 220093, 220094, 220097, 220138, 220140, 220152, 220041]
clist = [220011, 220001, 220014]

cid_list = [95,98,101,105,110,119,121,124,125,127,128,131,133,134,140,141,142,145,146,147,148,150,151,152,157,159,160,161,162,163,165,166,167,168,169,170,174,175,176,177,178,180,181,182,186,192,198,199,200,205,206,208,210,213,215,220,222,223,224,226,227,230,232,233,234,235,236,237,238,239,240,330,381]

CLINIC_OGRN = u""

FNAMEa = "AM{0}T22_14021.csv" # нет среди ответов фонда
FNAMEb = "SM{0}T22_14021.csv" # в ТФОМС на внесение изменений
FNAMEx = "SD{0}T22_14021.xls" # паценты с несколькими прикреплениями

STEP = 1000
DOC_TYPES = {1:u"1",
             2:u"2",
             3:u"3",
             4:u"4",
             5:u"5",
             6:u"6",
             7:u"7",
             8:u"8",
             9:u"9",
             10:u"10",
             11:u"11",
             12:u"12",
             13:u"13",
             14:u"14",
             15:u"15",
             16:u"16",
             17:u"17",
             18:u"18"
             }

SKIP_OGRN  = True # Do not put OGRN into IBR

CID_LIST   = False # Use cid_lis (list of clinic_id)

MLIST      = True  # Use mis.mlist table (MySQL)

def get_clist(db):
    
    s_sql = "SELECT DISTINCT mcod FROM mlist WHERE done is Null;"
    
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

    s_sql = """UPDATE mlist
    SET done = %s
    WHERE clinic_id = %s;"""
    
    cur = db.con.cursor()
    cur.execute(s_sql, (sdnow, clinic_id))

def p1(patient, insorg):
    import datetime
    now = datetime.datetime.now()
    s_now = u"%04d-%02d-%02d" % (now.year, now.month, now.day)    
    
    res = []
    res.append( u"{0}".format(patient.people_id) )
    res.append( u"{0}".format(patient.lname.strip().upper()) )
    res.append( u"{0}".format(patient.fname.strip().upper()) )
    if patient.mname == None:
        res.append(u"")
    else:
        res.append( u"{0}".format(patient.mname.strip().upper()) )
    dr = patient.birthday
    sdr = u"%04d-%02d-%02d" % (dr.year, dr.month, dr.day)
    res.append(sdr)
    sex = patient.sex
    if sex == u"М":
        res.append(u"1")
    else:
        res.append(u"2")
    doc_type_id = patient.document_type_id_fk
    if doc_type_id == None:
        sdt = u"14"
    elif DOC_TYPES.has_key(doc_type_id):
        sdt = DOC_TYPES[doc_type_id]
    else:
        sdt = u""
    res.append(sdt)
    if patient.document_series == None:
        ds = u""
    else:
        ds = patient.document_series
    res.append(ds)
    if patient.document_number == None:
        dn = u""
    else:
        dn = patient.document_number
    res.append(dn)

    if patient.insurance_certificate == None:
        SNILS = u""
    else:
        SNILS = patient.insurance_certificate
    res.append(SNILS)
    
    ogrn = insorg.ogrn
    if ogrn == None or ogrn == 0 or SKIP_OGRN:
        insorg_ogrn = u""
    else:
        insorg_ogrn = u"{0}".format(ogrn)
    res.append(insorg_ogrn)

    okato = insorg.okato
    if okato == None or okato == 0 or SKIP_OGRN:
        insorg_okato = u""
    else:
        insorg_okato = u"{0}".format(ogrn)
    res.append(insorg_okato)

    # medical_insurance_series (s_mis) & medical_insurance_number (s_min)
    sss = patient.medical_insurance_series
    if sss == None:
        s_mis = u""
    else:
        s_mis = u"{0}".format(sss)

    sss = patient.medical_insurance_number
    if sss == None:
        s_min = u""
    else:
        s_min = u"{0}".format(sss)
    
    enp = u""
    if len(s_mis) == 0:
        tdpfs = u"3" # Полис ОМС единого образца
        enp = s_min
        smin = u""
    elif s_mis[0] in (u"0", u"1", u"2", u"3", u"4", u"5", u"6", u"7", u"8", u"9"):
        tdpfs = u"2" # Временное свидетельство, ....
    else:
        tdpfs = u"1" # Полис ОМС старого образца
    
    
    # ENP
    if SKIP_OGRN:
        if len(enp) > 0:
            s_min = enp
            enp = u""
            
    res.append(enp)
    
    res.append(tdpfs)
    
    res.append(s_mis)
    
    res.append(s_min)
    
    # medical care start
    res.append(s_now)
    # medical care end
    res.append(s_now)
    
    # MO  OGRN
    res.append(CLINIC_OGRN)
    # HEALTHCARE COST
    res.append(u"")
    
    return u"|".join(res)

def plist(dbc, clinic_id, mcod, rows):
    import xlwt
    from dbmysql_connect import DBMY
    from PatientInfo import PatientInfo
    from insorglist import InsorgInfoList
    
    import os    
    import datetime
    import time

    dbmy = DBMY()
    curr = dbmy.con.cursor()
    
    s_sqlf = """SELECT oms_series, oms_number, enp, mcod
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
    
    
    p_obj = PatientInfo()
    insorgs = InsorgInfoList()

    fnamea = FNAMEa.format(mcod)
    fnameb = FNAMEb.format(mcod)
    fnamex = FNAMEx.format(mcod)
    sout = "Output to files: {0} | {1} | {2}".format(fnamea, fnameb, fnamex)
    log.info(sout)

    foa = open(fnamea, "wb")
    fob = open(fnameb, "wb")

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
    
    for row in rows:
        count += 1
        p_id = row[0]
	p_obj.initFromRec(row)

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
	    sss = p1(p_obj, insorg) + "|\n"
	    ps = sss.encode('windows-1251')

	    foa.write(ps)

	    foa.flush()
	    os.fsync(foa.fileno())

	else:
	    f_oms_series = rec[0]
	    f_oms_number = rec[1]
	    f_enp        = rec[2]
	    f_mcod       = rec[3]
	    
	    if mcod == f_mcod:
		count_e += 1
	    else:
		count_b += 1
		
		cur.execute(s_sql_ap,(p_id, ))
		recs_ap = cur.fetchall()
		l_print = False
		if len(recs_ap) == 1:
		    sss = p1(p_obj, insorg) + "|\n"
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
			

			
			if (motive_attach == 2) and (clinic_id == clinic_id_fk) and (not l_print):
			    sss = p1(p_obj, insorg) + "|\n"
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
    wb.save(fnamex)

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
    
    if SKIP_OGRN: CLINIC_OGRN = u""
    
    sout = "clinic_id: {0} cod_mo: {1} clinic_name: {2} clinic_ogrn: {3}".format(clinic_id, mcod, cname, cogrn)
    log.info(sout)

    s_sqlt = """SELECT DISTINCT * FROM vw_peoples p
JOIN area_peoples ap ON p.people_id = ap.people_id_fk
JOIN areas ar ON ap.area_id_fk = ar.area_id
JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
WHERE ca.clinic_id_fk = {0} AND ca.basic_speciality = 1
AND ap.date_end is Null;"""
    s_sql = s_sqlt.format(clinic_id)
    cursor = dbc.con.cursor()
    cursor.execute(s_sql)
    results = cursor.fetchall()
    
    plist(dbc, clinic_id, mcod, results)
    
    dbc.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Insurance Belongings Analysis Finish  '+localtime)


if __name__ == "__main__":
    
    import os    
    import datetime
    from dbmysql_connect import DBMY

    log.info("======================= INSB-2 ===========================================")
    
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
