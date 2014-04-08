#!/usr/bin/python
# -*- coding: utf-8 -*-
# insr-list1.py - заполнение списка ЛПУ для создания ЗСП
#                 из xlsx файла
#                 заполняется таблица mis.isr_list
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

from dbmysql_connect import DBMY

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insr_list2.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

SQLT_ILIST = """INSERT INTO insr_list
(clinic_id, mcod, name)
VALUES
(%s, %s, %s);"""

F_NAME = "insr_list.xlsx"
F_PATH = "./"
F_FNAME = F_PATH + "/" + F_NAME

def get_insr_list(ffname = F_FNAME):
    import xlrd

    sout = "Getting Clinics List from: {0}".format(ffname)
    log.info(sout)
    
    workbook = xlrd.open_workbook(ffname)
    worksheets = workbook.sheet_names()
    wsh = worksheets[0]
    
    worksheet = workbook.sheet_by_name(wsh)
    curr_row = 4
    num_rows = worksheet.nrows - 1

    insr_list = []
    while curr_row < num_rows:
	curr_row += 1
	# Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
	c1_type = worksheet.cell_type(curr_row, 0)
	if c1_type != 2: continue
	mcod = int(worksheet.cell_value(curr_row, 0))
	cname = worksheet.cell_value(curr_row, 1)
	try:
	    clinic_id = int(worksheet.cell_value(curr_row, 2))
	except:
	    clinic_id = None
	insr_list.append([mcod, cname, clinic_id])
	
    nnn = len(insr_list)
    sout = "Have got {0} Clinics Totally".format(nnn)
    log.info(sout)
    return insr_list


if __name__ == "__main__":
    import time
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('----------------------------------------------------------------------------------')
    log.info('Create Clinics List for Insurance Belongings Requests. Start {0}'.format(localtime))

    insr_list = get_insr_list()
    
    dbmy = DBMY()
    curm = dbmy.con.cursor()
    
    ssql = "TRUNCATE TABLE insr_list;"
    curm.execute(ssql)
    dbmy.con.commit()
    
    for clinic in insr_list:
	mcod  = clinic[0]
	cname = clinic[1]
	c_id  = clinic[2]
	curm.execute(SQLT_ILIST,(c_id, mcod, cname,))

    dbmy.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Create Clinics List for Insurance Belongings Requests. Finish  '+localtime)
	
    sys.exit(0)
 