#!/usr/bin/python
# -*- coding: utf-8 -*-
# insr1.py - запрос страховой пренадлежности
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
STEP = 100

DOC_TYPES = {1:u"14",
             2:u"15",
             3:u"16"}

def p1(patient):
    res = []
    res.append( u"{0}".format(patient.people_id) )
    res.append( u"{0}".format(patient.lname.upper()) )
    res.append( u"{0}".format(patient.fname.upper()) )
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
    res.append(u"")
    res.append(u"")
    return u"|".join(res)

if __name__ == "__main__":

    from dbmis_connect2 import DBMIS
    from PatientInfo import PatientInfo
    from insorglist import InsorgInfoList
        
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
    
    dbc = DBMIS()
    p_obj = PatientInfo()
    insorgs = InsorgInfoList()
    cursor = dbc.con.cursor()
    s_sql = s_sqlt.format(CLINIC_ID)
    cursor.execute(s_sql)
    results = cursor.fetchall()
    
    ncount = 0
    ccount = 0
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
            log.debug(sout)
            
            insorg_id   = p_obj.insorg_id
            insorg = insorgs[insorg_id]
            insorg_name = insorg.name.encode('utf-8')
            insorg_ogrn = insorg.ogrn
            sout = "insorg id: {0} name: {1} ogrn:{2}".format(insorg_id, insorg_name, insorg_ogrn)
            log.debug(sout)
            ps = p1(p_obj).encode('utf-8')
            log.debug(ps)
        if age > 20 and agem == 0:
            ccount += 1
        
    print ccount, ncount
    
    dbc.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Insurance Belongings Request Finish  '+localtime)
    sys.exit(0)
