#!/usr/bin/python
# -*- coding: utf-8 -*-
# insr-list1.py - заполнение списка ЛПУ для создания ЗСП
#                 заполняется таблица mis.isr_list
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

from dbmysql_connect import DBMY

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insr_list1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

#clist = [220021, 220022, 220034, 220036, 220037, 220040, 220042, 220043, 220045, 220048, 220051, 220059, 220060, 220062, 220063, 220064, 220068, 220073, 220074, 220078, 220079, 220080, 220081, 220083, 220085, 220091, 220093, 220094, 220097, 220138, 220140, 220152, 220041]
clist = [220076, 220105, 220104, 220110]

cid_list = [95,98,101,105,110,119,121,124,125,127,128,131,133,134,140,141,142,145,146,147,148,150,151,152,157,159,160,161,162,163,165,166,167,168,169,170,174,175,176,177,178,180,181,182,186,192,198,199,200,205,206,208,210,213,215,220,222,223,224,226,227,230,232,233,234,235,236,237,238,239,240,330,381]

CID_LIST   = True # Use cid_lis (list of clinic_id)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

SQLT_ILIST = """INSERT INTO insr_list
(clinic_id, mcod, name)
VALUES
(%s, %s, %s);"""

def pclinic(clinic_id, mcod, curm):
    from dbmis_connect2 import DBMIS

    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
    if dbc.ogrn == None:
        CLINIC_OGRN = u""
    else:
        CLINIC_OGRN = dbc.ogrn

    cogrn = CLINIC_OGRN.encode('utf-8')
    cname = dbc.name.encode('utf-8')

    sout = "clinic_id: {0} clinic_name: {1} clinic_ogrn: {2} cod_mo: {3}".format(clinic_id, cname, cogrn, mcod)
    log.info(sout)
    
    curm.execute(SQLT_ILIST,(clinic_id, mcod, dbc.name,))

    dbc.close()


if __name__ == "__main__":
    import time
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('----------------------------------------------------------------------------------')
    log.info('Create Clinics List for Insurance Belongings Requests. Start {0}'.format(localtime))
    
    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info( sout )

    dbmy = DBMY()
    curm = dbmy.con.cursor()
    
    ssql = "TRUNCATE TABLE insr_list;"
    curm.execute(ssql)
    dbmy.con.commit()
    
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
	    
	    pclinic(clinic_id, mcod, curm)
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
	    
	    pclinic(clinic_id, mcod, curm)    

    dbmy.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Create Clinics List for Insurance Belongings Requests. Finish  '+localtime)
	
    sys.exit(0)
 