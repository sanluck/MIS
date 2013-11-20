#!/usr/bin/python
# -*- coding: utf-8 -*-
# insr-1.py - запрос страховой пренадлежности
#             Insurance Belongins Request (IBR)
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insr1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

#clist = [220021, 220022, 220034, 220036, 220037, 220040, 220042, 220043, 220045, 220048, 220051, 220059, 220060, 220062, 220063, 220064, 220068, 220073, 220074, 220078, 220079, 220080, 220081, 220083, 220085, 220091, 220093, 220094, 220097, 220138, 220140, 220152, 220041]
clist = [220137]

cid_list = [105,110,119,121,124,125,127,128,131,133,134,140,141,142,145,146,147,148,150,151,152,157,159,160,161,162,163,165,166,167,168,169,170,174,175,176,177,178,180,181,182,186,192,198,199,200,205,206,208,210,213,215,220,222,223,224,226,227,230,232,233,234,235,236,237,238,239,240,330,381]

CLINIC_OGRN = u""

FNAME = "IM{0}T22_13111.csv"

STEP = 100
DOC_TYPES = {1:u"1",
             2:u"2",
             3:u"3"}

SKIP_OGRN  = True # Do not put OGRN into IBR

ALL_PEOPLE = True # Do IBR for all patients or for DVN candidates only

DVN_LIST   = True # Use clinical_checkups table to find out patients list

CID_LIST   = True # Use cid_lis (list of clinic_id)

def p1(patient, insorg):
    import datetime
    now = datetime.datetime.now()
    s_now = u"%04d-%02d-%02d" % (now.year, now.month, now.day)    
    
    res = []
    res.append( u"{0}".format(patient.people_id) )
    res.append( u"{0}".format(patient.lname.upper()) )
    res.append( u"{0}".format(patient.fname.upper()) )
    if patient.mname == None:
        res.append(u"")
    else:
        res.append( u"{0}".format(patient.mname.upper()) )
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

def plist(dbc, fname, rows):
    from PatientInfo import PatientInfo
    from insorglist import InsorgInfoList
    
    import os    
    import datetime
    import time

    now = datetime.datetime.now()
    s_now = "%04d-%02d-%02d" % (now.year, now.month, now.day)    
    y_now = now.year

    if dbc.ogrn == None:
        CLINIC_OGRN = u""
    else:
        CLINIC_OGRN = dbc.ogrn
    
    cogrn = CLINIC_OGRN.encode('utf-8')
    cname = dbc.name.encode('utf-8')
    mcod  = dbc.mcod
    
    if SKIP_OGRN: CLINIC_OGRN = u""
    
    p_obj = PatientInfo()
    insorgs = InsorgInfoList()

    fo = open(fname, "wb")
    
    ncount = 0
    ccount = 0
    noicc  = 0
    p_id_old = 0
    for row in rows:
        ncount += 1
        p_id = row[0]
        if p_id == p_id_old:
            continue
        p_id_old = p_id
        p_obj.initFromRec(row)
        p_bd = p_obj.birthday
        p_by = p_bd.year
        age  = y_now - p_by
        agem = age % 3
        if ncount % STEP == 0:
            sout = " {0}/{1} people_id: {2} age: {3} agem: {4}".format(ccount, ncount, p_id, age, agem)
            log.info(sout)
            
            insorg_id   = p_obj.insorg_id
            try:
                insorg = insorgs[insorg_id]
            except:
                sout = "People_id: {0}. Have not got insorg_id: {1}".format(p_id, insorg_id)
                log.debug(sout)
                insorg = insorgs[0]
                
            insorg_name = insorg.name.encode('utf-8')
            insorg_ogrn = insorg.ogrn
            sout = "insorg id: {0} name: {1} ogrn:{2}".format(insorg_id, insorg_name, insorg_ogrn)
            log.debug(sout)
            ps = p1(p_obj, insorg).encode('utf-8')
            log.debug(ps)
            # To make sure that you're data is written to disk
            # http://stackoverflow.com/questions/608316/is-there-commit-analog-in-python-for-writing-into-a-file
    
            fo.flush()
            os.fsync(fo.fileno())
            
        if ALL_PEOPLE or (age > 20 and agem == 0):
            ccount += 1
            insorg_id   = p_obj.insorg_id
            try:
                insorg = insorgs[insorg_id]
            except:
                sout = "People_id: {0}. Have not got insorg_id: {1}".format(p_id, insorg_id)
                log.debug(sout)
                insorg = insorgs[0]
                noicc += 1
            sss = p1(p_obj, insorg) + "|\n"
            ps = sss.encode('windows-1251')
            fo.write(ps)

    fo.flush()
    os.fsync(fo.fileno())
    fo.close()
    sout = "candidates: {0} / patients: {1}".format(ccount, ncount)
    log.info( sout )
    sout = "{0} candidates have not got insurance company id".format(noicc)
    log.info( sout )
    
def pclinic(clinic_id, mcod):
    from dbmis_connect2 import DBMIS
    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('Insurance Belongings Request Start {0}'.format(localtime))

    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
    if dbc.ogrn == None:
        CLINIC_OGRN = u""
    else:
        CLINIC_OGRN = dbc.ogrn

    cogrn = CLINIC_OGRN.encode('utf-8')
    cname = dbc.name.encode('utf-8')
    
    if SKIP_OGRN: CLINIC_OGRN = u""
    
    sout = "clinic_id: {0} clinic_name: {1} clinic_ogrn: {2} cod_mo: {3}".format(clinic_id, cname, cogrn, mcod)
    log.info(sout)

    if DVN_LIST:
	s_sqlt = """SELECT DISTINCT p.* FROM vw_peoples p
RIGHT JOIN clinical_checkups cc ON p.people_id = cc.people_id_fk
WHERE cc.clinic_id_fk = {0}
AND ((cc.date_end_1 > '2013-10-31') OR (cc.date_end_2 > '2013-10-31'));"""
	s_sql = s_sqlt.format(clinic_id)
    else:
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
    
    fname = FNAME.format(mcod)
    sout = "Output to file: {0}".format(fname)
    log.info(sout)
    
    plist(dbc, fname, results)

    
    dbc.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Insurance Belongings Request Finish  '+localtime)
    

if __name__ == "__main__":
    
    import os    
    import datetime

    
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
	
    sys.exit(0)
