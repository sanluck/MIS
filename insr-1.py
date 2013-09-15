#!/usr/bin/python
# -*- coding: utf-8 -*-
# insr-1.py - запрос страховой пренадлежности
#

import logging
import sys, codecs

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insr.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

CLINIC_ID = 224
CLINIC_OGRN = u""
STEP = 100

DOC_TYPES = {1:u"1",
             2:u"2",
             3:u"3"}

fname = "IM220096T22_13091.csv"

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
    if ogrn == None or ogrn == 0:
        insorg_ogrn = u""
    else:
        insorg_ogrn = u"{0}".format(ogrn)
    res.append(insorg_ogrn)

    okato = insorg.okato
    if okato == None or okato == 0:
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

if __name__ == "__main__":

    from dbmis_connect2 import DBMIS
    from PatientInfo import PatientInfo
    from insorglist import InsorgInfoList
    
    import os    
    import datetime
    import time
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('Insurance Belongings Request Start {0}'.format(localtime))

    now = datetime.datetime.now()
    s_now = "%04d-%02d-%02d" % (now.year, now.month, now.day)    
    y_now = now.year

    s_sqlt = """SELECT ap.people_id_fk 
FROM area_peoples ap
JOIN areas ar ON ap.area_id_fk = ar.area_id
JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
WHERE ca.clinic_id_fk = {0} AND ca.speciality_id_fk = 1
AND ap.date_end is Null;"""
    
    dbc = DBMIS(CLINIC_ID)
    if dbc.ogrn == None:
        CLINIC_OGRN = u""
    else:
        CLINIC_OGRN = dbc.ogrn
    
    cogrn = CLINIC_OGRN.encode('utf-8')
    cname = dbc.name.encode('utf-8')
    
    sout = "clinic_id: {0} clinic_name: {1} clinic_ogrn: {2}".format(CLINIC_ID, cname, cogrn)
    log.info(sout)
    
    p_obj = PatientInfo()
    insorgs = InsorgInfoList()
    cursor = dbc.con.cursor()
    s_sql = s_sqlt.format(CLINIC_ID)
    cursor.execute(s_sql)
    results = cursor.fetchall()

    fo = open(fname, "wb")
    
    ncount = 0
    ccount = 0
    noicc  = 0
    for row in results:
        ncount += 1
        
        p_id = row[0]
        p_obj.initFromDb(dbc, p_id)
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
            
        if age > 20 and agem == 0:
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
    
    dbc.close()
    sout = "candidates: {0} / patients: {1}".format(ccount, ncount)
    log.info( sout )
    sout = "{0} candidates have not got insurance company id".format(noicc)
    log.info( sout )
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Insurance Belongings Request Finish  '+localtime)
    sys.exit(0)
