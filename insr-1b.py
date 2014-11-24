#!/usr/bin/python
# -*- coding: utf-8 -*-
# insr-1b.py - запрос страховой пренадлежности
#              Insurance Belongins Request (IBR)
#              по списку ЛПУ из DBMY (mis.insr_list)
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

from dbmysql_connect import DBMY

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insr1b.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

CLINIC_OGRN = u""

FNAME = "SM{0}T22_14112.csv"
FPATH = "./SM"

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

ALL_PEOPLE = True # Do IBR for all patients or for DVN candidates only

NO_ENP     = False # Do IBR only for patients without ENP
#DATE_RANGE = None
DATE_RANGE = ["2014-11-01","2014-11-30"]

REGISTER_DONE = True

SQLT_FTP = """SELECT id, enp FROM tfoms_peoples
WHERE people_id = %s AND clinic_id = %s;"""

SQLT_INSTP = """INSERT INTO tfoms_peoples
(people_id, clinic_id, date_from_mis)
VALUES
(%s, %s, %s);"""

SQLT_UPDTP = """UPDATE tfoms_peoples
SET date_from_mis = %s
WHERE id = %s;"""

CLEAR_BEFORE_SELECT = True

def write_to_dbmy(curm, p_id, clinic_id, s_now):

    curm.execute(SQLT_FTP, (p_id, clinic_id, ))
    rec = curm.fetchone()
    if rec is None:
	try:
	    curm.execute(SQLT_INSTP, (p_id, clinic_id, s_now, ))
	    return True
	except:
	    return False
    else:
	enp = rec[1]
	if (not NO_ENP) or (enp is None):
	    _id = rec[0]
	    curm.execute(SQLT_UPDTP, ( s_now, _id, ))
	    return True
	else:
	    return False

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

def plist(dbc, fname, rows, clinic_id):
    from PatientInfo import PatientInfo
    from insorglist import InsorgInfoList

    import os
    import datetime
    import time

    now = datetime.datetime.now()
    s_now = "%04d-%02d-%02d" % (now.year, now.month, now.day)
    y_now = now.year

    dbmy = DBMY()
    curm = dbmy.con.cursor()

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
    n_pid_w_err = 0
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
	    if write_to_dbmy(curm, p_id, clinic_id, s_now):
		fo.write(ps)
	    else:
		n_pid_w_err += 1


    fo.flush()
    os.fsync(fo.fileno())
    fo.close()
    dbmy.close()
    sout = "candidates: {0} / patients: {1}".format(ccount, ncount)
    log.info( sout )
    sout = "{0} candidates have not got insurance company id".format(noicc)
    log.info( sout )
    sout = "{0} records have not been written".format(n_pid_w_err)
    log.info( sout )


def pclinic(clinic_id, mcod):
    from dbmis_connect2 import DBMIS
    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('-----------------------------------------------------------------------------------')
    log.info('Insurance Belongings Request Start {0}'.format(localtime))

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

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

    if DATE_RANGE is None:
	s_sqlt = """SELECT DISTINCT * FROM vw_peoples p
JOIN area_peoples ap ON p.people_id = ap.people_id_fk
JOIN areas ar ON ap.area_id_fk = ar.area_id
JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
WHERE ca.clinic_id_fk = {0} AND ca.basic_speciality = 1
AND ap.date_end is Null;"""
	s_sql = s_sqlt.format(clinic_id)
    else:
	d_start  = DATE_RANGE[0]
	d_finish = DATE_RANGE[1]
	s_sqlt = """SELECT DISTINCT * FROM vw_peoples p
JOIN area_peoples ap ON p.people_id = ap.people_id_fk
JOIN areas ar ON ap.area_id_fk = ar.area_id
JOIN clinic_areas ca ON ar.clinic_area_id_fk = ca.clinic_area_id
WHERE ca.clinic_id_fk = {0} AND ca.basic_speciality = 1
AND ap.date_beg >= '{1}' AND ap.date_beg <= '{2}'
AND ap.date_end is Null;"""
	s_sql = s_sqlt.format(clinic_id, d_start, d_finish)
	sout = "date_beg: [{0}] - [{1}]".format(d_start, d_finish)
	log.info(sout)

    cursor = dbc.con.cursor()
    cursor.execute(s_sql)
    results = cursor.fetchall()

    fname = FPATH + "/" + FNAME.format(mcod)
    sout = "Output to file: {0}".format(fname)
    log.info(sout)

    plist(dbc, fname, results, clinic_id)


    dbc.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Insurance Belongings Request Finish  '+localtime)

def get_clist():

    dbmy = DBMY()
    curm = dbmy.con.cursor()

    ssql = "SELECT id, clinic_id, mcod FROM insr_list WHERE done is Null;"
    curm.execute(ssql)
    results = curm.fetchall()

    clist = []
    for rec in results:
	_id = rec[0]
	clinic_id = rec[1]
	mcod = rec[2]
	if (mcod is None) or (clinic_id is None): continue
	clist.append([_id, clinic_id, mcod])

    dbmy.close()
    return clist

def get_1clinic_lock(id_unlock = None):

    dbmy = DBMY()
    curm = dbmy.con.cursor()

    if id_unlock is not None:
	ssql = "UPDATE insr_list SET c_lock = Null WHERE id = %s;"
	curm.execute(ssql, (id_unlock, ))
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

def register_done(_id):
    import datetime

    dbmy = DBMY()
    curm = dbmy.con.cursor()

    dnow = datetime.datetime.now()
    sdnow = str(dnow)

    s_sqlt = """UPDATE insr_list
    SET done = %s
    WHERE
    id = %s;"""

    curm.execute(s_sqlt,(dnow, _id, ))
    dbmy.close()

def clear_tfoms_peoples(clinic_id):

    dbmy = DBMY()
    curm = dbmy.con.cursor()

    s_sqlt = "DELETE FROM tfoms_peoples WHERE clinic_id = %s;"
    curm.execute(s_sqlt, (clinic_id, ))

    dbmy.con.commit()
    dbmy.close()

    log.info('-----------------------------------------------------------------------------------')
    sout = "All records for clinic_id = {0} have been deleted from the tfoms_peoples table".format(clinic_id)
    log.info(sout)

if __name__ == "__main__":

    import os
    import datetime

    c_rec  = get_1clinic_lock()
    while c_rec is not None:
	_id = c_rec[0]
	clinic_id = c_rec[1]
	mcod = c_rec[2]
	if CLEAR_BEFORE_SELECT: clear_tfoms_peoples(clinic_id)

	pclinic(clinic_id, mcod)

	if REGISTER_DONE: register_done(_id)

	c_rec  = get_1clinic_lock(_id)

    sys.exit(0)
