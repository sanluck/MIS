#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# mark_card-n.py - отметки о загрузке карт ПН на потрал Минздрава
#

import os
import sys
import ConfigParser
import logging

if __name__ == "__main__":
    LOG_FILENAME = '_cardn.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

Config = ConfigParser.ConfigParser()
PATH = os.path.dirname(sys.argv[0])
FINI = PATH + "/" + "mark_card-n.ini"

from ConfigSection import ConfigSectionMap
# read INI data
Config.read(FINI)
# [DBMIS]
Config1 = ConfigSectionMap(Config, "DBMIS")
HOST = Config1['host']
DB = Config1['db']

# [Cardn]
Config2 = ConfigSectionMap(Config, "Cardn")
DATE_PORTAL = Config2['date_portal']

CLINIC_ID = 22
DATEMODE = 0
# http://stackoverflow.com/questions/1108428/how-do-i-read-a-date-in-excel-format-in-python
# datemode: 0 for 1900-based, 1 for 1904-based

PATH_IN  = "./PN_IN"
PATH_OUT = "./PN_OUT"

SQLT_CARD_U = """UPDATE prof_exam_minor 
SET date_portal = ?
WHERE prof_exam_id = ?;"""

def get_fnames(path = PATH_IN, file_ext = '.xls'):
# get file names
    
    fnames = []
    for subdir, dirs, files in os.walk(path):
        for fname in files:
            if fname.find(file_ext) > 1:
                log.info(fname)
                fnames.append(fname)
    
    return fnames    

def get_cards(fname):
# read xls file, fill card numbers, FIO, date
#
# http://www.youlikeprogramming.com/2012/03/examples-reading-excel-xls-documents-using-pythons-xlrd/
#
    import xlrd
    from datetime import datetime, timedelta
    
    workbook = xlrd.open_workbook(fname)
    worksheets = workbook.sheet_names()
    worksheet0 = workbook.sheet_by_name(worksheets[0])

    arr = []

    num_rows = worksheet0.nrows - 1
    curr_row = -1
    while curr_row < num_rows:
        curr_row += 1
	c0_type = worksheet0.cell_type(curr_row, 0)
	# Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
	if c0_type != 1: continue
	c0_value = worksheet0.cell_value(curr_row, 0)
	if c0_value[0] != u"№" : continue
	card_number = c0_value[1:]
	fio = worksheet0.cell_value(curr_row, 1)
	xldate = worksheet0.cell_value(curr_row, 4)
	card_date = datetime(1899, 12, 30) \
	    + timedelta(days=xldate + 1462 * DATEMODE)	
	arr.append([card_number, fio, card_date])
	
    return arr

def mark_cards(c_arr, date_portal):
    from dbmis_connect2 import DBMIS
    
    try:
	dbc = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
	cur = dbc.con.cursor()
    except Exception, e:
	sout = "Error connecting to {0}:{1} : {2}".format(HOST, DB, e)
	log.error( sout )
	return
    
    for card in c_arr:
	card_number = card[0]
	fio = card[1]
	card_date = card[2]
	
	cur.execute(SQLT_CARD_U, (date_portal, prof_exam_id, ))
	dbc.con.commit()
    
    dbc.close()

    
if __name__ == "__main__":
    
    from datetime import datetime
    import time
    import shutil

    date_portal = datetime.strptime(DATE_PORTAL, '%Y-%m-%d')

    log.info("======================= MARK CARD-X ===========================================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Registering PN Cards. Start {0}'.format(localtime))

    fnames = get_fnames()
    n_fnames = len(fnames)
    sout = "Totally {0} files has been found".format(n_fnames)
    log.info( sout )
    
    
    for fname in fnames:

	f_fname = PATH_IN + "/" + fname
	sout = "Input file: {0}".format(f_fname)
	log.info(sout)
    
	ar = get_cards(f_fname)
	l_ar = len(ar)
	sout = "File has got {0} cards".format(l_ar)
	log.info( sout )

	destination = PATH_OUT + "/" + fname
	
#	mark_cards(ar, date_portal)

#	shutil.move(f_fname, destination)
	
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Registering PN Cards. Finish  '+localtime)  
    
    sys.exit(0)
    