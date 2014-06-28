#!/usr/bin/python
# -*- coding: utf-8 -*-
# pn-list1.py - заполнение списка ЛПУ для экспорта ПН
#                 из xlsx файла
#                 заполняется таблица mis.pn_list
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

from dbmysql_connect import DBMY

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_pn_list1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

SQLT_ILIST = """INSERT INTO pn_list
(clinic_id, mcod, name)
VALUES
(%s, %s, %s);"""

SQLT_FLIST = """SELECT id
FROM pn_list
WHERE mcod = %s;"""

SQLT_ULIST = """UPDATE pn_list
SET 
clinic_id = %s, 
mcod = %s, 
name = %s,
done = Null
WHERE id = %s;"""


F_NAME = "pn_list1.xlsx"
F_PATH = "."
F_FNAME = F_PATH + "/" + F_NAME

UPDATE = False

def get_pn_list(ffname = F_FNAME):
    import xlrd

    sout = "Getting Clinics List from: {0}".format(ffname)
    log.info(sout)
    
    workbook = xlrd.open_workbook(ffname)
    worksheets = workbook.sheet_names()
    wsh = worksheets[0]
    
    worksheet = workbook.sheet_by_name(wsh)
    curr_row = 0
    num_rows = worksheet.nrows - 1

    pn_list = []
    while curr_row < num_rows:
	curr_row += 1
	# Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
	c1_type = worksheet.cell_type(curr_row, 0)
	if c1_type != 2: continue
	clinic_id = worksheet.cell_value(curr_row, 0)
	cname = worksheet.cell_value(curr_row, 1)
	mcod = modb.moCodeByMisId(clinic_id)
	pn_list.append([mcod, cname, clinic_id])
	
    nnn = len(pn_list)
    sout = "Have got {0} Clinics Totally".format(nnn)
    log.info(sout)
    return pn_list


if __name__ == "__main__":
    import time
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('----------------------------------------------------------------------------------')
    log.info('Create Clinics List for Prof Exams Export. Start {0}'.format(localtime))

    pn_list = get_pn_list()
    
    dbmy = DBMY()
    curr = dbmy.con.cursor()
    curm = dbmy.con.cursor()
    
    if UPDATE:
	for clinic in pn_list:
	    mcod  = clinic[0]
	    cname = clinic[1]
	    c_id  = clinic[2]
	    curr.execute(SQLT_FLIST,(mcod,))
	    rec = curr.fetchone()
	    if rec is None:
		curm.execute(SQLT_ILIST,(c_id, mcod, cname,))
	    else:
		_id = rec[0]
		curm.execute(SQLT_ULIST,(c_id, mcod, cname, _id, ))	
    else:
	ssql = "TRUNCATE TABLE pn_list;"
	curm.execute(ssql)
	dbmy.con.commit()
    
	for clinic in pn_list:
	    mcod  = clinic[0]
	    cname = clinic[1]
	    c_id  = clinic[2]
	    curm.execute(SQLT_ILIST,(c_id, mcod, cname,))

    dbmy.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Create Clinics List for Prof Exams Export. Finish  '+localtime)
	
    sys.exit(0)
 