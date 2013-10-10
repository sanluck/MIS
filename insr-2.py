#!/usr/bin/python
# -*- coding: utf-8 -*-
# insr-2.py - обработка ответа на запрос страховой пренадлежности
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insr2.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

CLINIC_OGRN = u""

STEP = 100
DOC_TYPES = {1:u"1",
             2:u"2",
             3:u"3"}

SKIP_OGRN = True

SET_MO = True

SET_INSORG = False

INSR2DO_PATH = "./INSR2DO"
DVN2DO_PATH  = "./DVN2DO"
DVNDONE_PATH = "./DVNDONE"

CLINIC_AREAS = {}

def get_clinic_areas(db, clinic_id):
    
    if CLINIC_AREAS.has_key(clinic_id):
	clinic_areas = CLINIC_AREAS[clinic_id]
	return clinic_areas
    
    s_sqlt = """SELECT 
	            a.area_id, a.area_number,
	            ca.clinic_id_fk
	            FROM areas a
	            LEFT JOIN clinic_areas ca ON a.clinic_area_id_fk = ca.clinic_area_id
	            WHERE ca.clinic_id_fk = {0} AND ca.basic_speciality = 1;"""        
    ssql = s_sqlt.format(clinic_id)
    
    cursor = db.con.cursor()
    cursor.execute(ssql)
    recs = cursor.fetchall()
    if recs == None:
	clinic_areas = None
    else:
	ar = []
	for rec in recs:
	    ar.append([rec[0], rec[1]])
	clinic_areas = ar
    
    CLINIC_AREAS[clinic_id] = clinic_areas
    return clinic_areas

def plist_in(fname):
# read file <fname>
# and get peoples list
    fi = open(fname, "r")
    arr = []
    for line in fi:
        arl = line.split("|")
        sss = arl[4]
        arl[4] = sss.decode('cp1251')
        arr.append(arl)
    fi.close()
    return arr

def pclinic(fname, clinic_id, mcod):
    from dbmis_connect2 import DBMIS
    from PatientInfo2 import PatientInfo2
    from insorglist import InsorgInfoList
    
    import time
    import random

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('Insurance Belongings Results Processing Start {0}'.format(localtime))
    
    ppp = plist_in(fname)
    
    sout = "Totally {0} lines have been read from file <{1}>".format(len(ppp), fname)
    log.info(sout)

    p_obj = PatientInfo2()
    insorgs = InsorgInfoList()

    dbc = DBMIS(clinic_id)
    if dbc.ogrn == None:
        CLINIC_OGRN = u""
    else:
        CLINIC_OGRN = dbc.ogrn

    cogrn = CLINIC_OGRN.encode('utf-8')
    cname = dbc.name.encode('utf-8')
    
    if SKIP_OGRN: CLINIC_OGRN = u""
    
    sout = "clinic_id: {0} clinic_name: {1} clinic_ogrn: {2} cod_mo: {3}".format(clinic_id, cname, cogrn, mcod)
    log.info(sout)
    
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

    not_belongs_2_clinic = 0
    wrong_insorg = 0
    ncount = 0
    dbc2 = DBMIS(clinic_id)
    cur2 = dbc2.con.cursor()
    
    for prec in ppp:
        ncount += 1
        people_id = prec[0]
        insorg_mcod = prec[2]
        if insorg_mcod == "":
            insorg_id = 0
        else:
            insorg_id = int(insorg_mcod) - 22000
        medical_insurance_series = prec[4]
        medical_insurance_number = prec[5]
	s_mcod = prec[7]
	if s_mcod == "\r\n": continue
	f_mcod = int(s_mcod)
	try:
	    mo = modb[f_mcod]
	    f_clinic_id = mo.mis_code
	except:
	    sout = "People_id: {0}. Clinic was not found for mcod = {1}.".format(people_id, f_mcod)
	    log.warn(sout)
	    continue
	
        p_obj.initFromDb(dbc, people_id)
        if f_clinic_id <> p_obj.clinic_id:
            not_belongs_2_clinic += 1
            if SET_MO:
		clinic_areas = get_clinic_areas(dbc, f_clinic_id)
		if clinic_areas == None:
		    sout = "Clinic {0} has not got any areas".format(f_clinic_id)
		    log.warn(sout)
		else:
		    nareas = len(clinic_areas)
		    if nareas == 0:
			sout = "Clinic {0} has not got any areas".format(f_clinic_id)
			log.warn(sout)
			continue
			
		    narea  = random.randint(0,nareas-1)
		    area_id = clinic_areas[narea][0]

		    area_people_id = p_obj.area_people_id
		    s_sqlt = "UPDATE area_peoples SET area_id_fk = {0} WHERE area_people_id = {1};"
		    s_sql  = s_sqlt.format(area_id, area_people_id)
		    cur2.execute(s_sql)
		    dbc2.con.commit()
		    
		    sout = "people_id: {0} has got new clinic {1}".format(people_id, f_mcod)
		    log.debug(sout)
		    
        if (insorg_id <> 0) and (insorg_id <> p_obj.insorg_id):
            wrong_insorg += 1
            if SET_INSORG:
                s_sqlt = "UPDATE peoples SET insorg_id = {0} WHERE people_id = {1};"
                s_sql  = s_sqlt.format(insorg_id, people_id)
                cur2.execute(s_sql)
                dbc2.con.commit()
                
        if ncount % STEP == 0:
            sout = " {0} people_id: {1} clinic_id: {2}".format(ncount, people_id, p_obj.clinic_id)
            log.info(sout)

    sout = "Wrong clinic: {0}".format(not_belongs_2_clinic)
    log.info(sout)
    sout = "Wrong insorg: {0}".format(wrong_insorg)
    log.info(sout)
    
    dbc.close()
    dbc2.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Insurance Belongings Results Processing Finish  '+localtime)

def get_fnames(path = INSR2DO_PATH, file_ext = '.csv'):
    
    import os    
    
    fnames = []
    for subdir, dirs, files in os.walk(path):
        for fname in files:
            if fname.find(file_ext) > 1:
                log.info(fname)
                fnames.append(fname)
    
    return fnames    

def register_insr2_done(db, mcod, clinic_id, fname):
    import datetime    

    dnow = datetime.datetime.now()
    sdnow = str(dnow)
    s_sqlt = """INSERT INTO
    insr2_done
    (mcod, clinic_id, fname, done)
    VALUES
    ({0}, {1}, '{2}', '{3}');
    """

    s_sql = s_sqlt.format(mcod, clinic_id, fname, sdnow)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    db.con.commit()

def insr2_done(db, mcod):

    s_sqlt = """SELECT
    fname, done
    FROM
    insr2_done
    WHERE mcod = {0};
    """

    s_sql = s_sqlt.format(mcod)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    rec = cursor.fetchone()
    if rec == None:
	return False, "", ""
    else:
	fname = rec[0]
	done  = rec[1]
	return True, fname, done

if __name__ == "__main__":
    
    import os, shutil
    import datetime
    from dbmysql_connect import DBMY

    log.info("======================= INSR-2 ===========================================")
    
    fnames = get_fnames()
    n_fnames = len(fnames)
    sout = "Totally {0} files has been found".format(n_fnames)
    log.info( sout )

    dbmy2 = DBMY()
    
    for fname in fnames:
	s_mcod = fname[5:11]
	mcod = int(s_mcod)
    
	try:
	    mo = modb[mcod]
	    clinic_id = mo.mis_code
	    sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod) 
	    log.info(sout)
	except:
	    sout = "Clinic not found for mcod = {0}".format(s_mcod)
	    log.warn(sout)
	    continue

	f_fname = INSR2DO_PATH + "/" + fname
	sout = "Input file: {0}".format(f_fname)
	log.info(sout)


	ldone, dfname, ddone = insr2_done(dbmy2, mcod)
	if ldone:
	    sout = "On {0} hase been done. Fname: {1}".format(ddone, dfname)
	    log.warn( sout )
	else:
	    pclinic(f_fname, clinic_id, mcod)
	    register_insr2_done(dbmy2, mcod, clinic_id, fname)

	# move file
	source = INSR2DO_PATH + "/" + fname
	destination = DVN2DO_PATH + "/" + fname
	shutil.move(source, destination)


    sys.exit(0)
