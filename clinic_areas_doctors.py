#!/usr/bin/python
# coding: utf-8
# clinic_areas_doctors.py - найти СНИЛС докторов для участков клиники
#
import logging
import sys, codecs
import fdb

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_clinic_areas_doctors.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

CLINIC_ID = 129

SQLT1 = """SELECT a.area_id, a.area_number,
ca.clinic_area_id, ca.speciality_id_fk,
w.worker_id, w.doctor_id_fk,
d.people_id_fk,
p.insurance_certificate
FROM areas a
LEFT JOIN clinic_areas ca ON a.clinic_area_id_fk = ca.clinic_area_id
LEFT JOIN workers w ON a.area_id = w.area_id_fk
LEFT JOIN doctors d ON w.doctor_id_fk = d.doctor_id
LEFT JOIN peoples p ON d.people_id_fk = p.people_id
WHERE ca.clinic_id_fk = ? 
AND ca.basic_speciality = 1;"""

SQLT2 = """SELECT a.area_id, a.area_number,
ca.clinic_area_id, ca.speciality_id_fk,
wa.worker_id_fk,
w.doctor_id_fk,
d.people_id_fk,
p.insurance_certificate
FROM areas a
LEFT JOIN clinic_areas ca ON a.clinic_area_id_fk = ca.clinic_area_id
LEFT JOIN worker_areas wa ON a.area_id = wa.area_id_fk
LEFT JOIN workers w ON wa.worker_id_fk = w.worker_id
LEFT JOIN doctors d ON w.doctor_id_fk = d.doctor_id
LEFT JOIN peoples p ON d.people_id_fk = p.people_id
WHERE ca.clinic_id_fk = ? 
AND ca.basic_speciality = 1;"""

def get_cad(dbc, clinic_id):
    
    cur = dbc.con.cursor()

    dbc.con.begin(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    cur.execute(SQLT1, (clinic_id, ))
    
    result = cur.fetchall()
    cad = {}
    for rec in result:
        area_id = rec[0]
        area_number = rec[1]
        clinic_area_id = rec[2]
        speciality_id  = rec[3]
        worker_id = rec[4]
        doctor_id = rec[5]
        people_id = rec[6]
        insurance_certificate = rec[7]
        if speciality_id != 1: continue
        if insurance_certificate is None: continue
        if cad.has_key(area_number): continue
        cad[area_number] = insurance_certificate.replace(" ","").replace("-","")
        
    dbc.con.commit
        
    dbc.con.begin(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    cur.execute(SQLT2, (clinic_id, ))
    
    result = cur.fetchall()
    for rec in result:
        area_id = rec[0]
        area_number = rec[1]
        clinic_area_id = rec[2]
        speciality_id  = rec[3]
        worker_id = rec[4]
        doctor_id = rec[5]
        people_id = rec[6]
        insurance_certificate = rec[7]
        if speciality_id != 1: continue
        if insurance_certificate is None: continue
        if cad.has_key(area_number): continue
        cad[area_number] = insurance_certificate.replace(" ","").replace("-","")

    dbc.con.commit
    
    return cad

if __name__ == "__main__":
    import time
    from dbmis_connect2 import DBMIS
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('-----------------------------------------------------------------------------------')
    log.info('Clinic Areas Doctors Start {0}'.format(localtime))

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    clinic_id = CLINIC_ID
    mcod = modb.moCodeByMisId(clinic_id)
    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)

    cname = dbc.name.encode('utf-8')

    sout = "clinic_id: {0} clinic_name: {1} cod_mo: {2}".format(clinic_id, cname, mcod)
    log.info(sout)
    
    cad = get_cad(dbc, clinic_id)
    
    dbc.close()
    
    dnumber = len(cad)
    sout = "Totally we have got {0} areas having doctors".format(dnumber)
    log.info(sout)
    
    for a_number in cad.keys():
        snils = cad[a_number]
        sout = "{0}: {1}".format(a_number, snils)
        log.info(sout)
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Clinic Areas Doctors Finish  '+localtime)
    
    sys.exit(0)
    