#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# mark_card-x.py - отметки о загрузке карт ПН на потрал Минздрава
#                  по данным xls файла
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

STEP = 1000
DATEMODE = 0
# http://stackoverflow.com/questions/1108428/how-do-i-read-a-date-in-excel-format-in-python
# datemode: 0 for 1900-based, 1 for 1904-based

PATH_IN  = "./PN_IN"
PATH_OUT = "./PN_OUT"

SQLT_CARD_U = """UPDATE prof_exam_minor
SET date_portal = ?
WHERE prof_exam_id = ?;"""

SQLT_FPEOPLE = """SELECT FIRST 20
    PEOPLE_ID,
    LNAME,
    FNAME,
    MNAME,
    LNAME ||' '|| FNAME ||' '|| coalesce(' '||MNAME, '') As FIO,
    BIRTHDAY,
    SEX, INSURANCE_CERTIFICATE
FROM VW_PEOPLES_SMALL_EXT
WHERE
upper(LNAME) starting '{0}'
AND upper(FNAME) starting '{1}'
AND upper(MNAME) starting '{2}';"""

SQLT_FPEOPLE0 = """SELECT FIRST 20
    PEOPLE_ID,
    LNAME,
    FNAME,
    MNAME,
    LNAME ||' '|| FNAME ||' '|| coalesce(' '||MNAME, '') As FIO,
    BIRTHDAY,
    SEX, INSURANCE_CERTIFICATE
FROM VW_PEOPLES_SMALL_EXT
WHERE
upper(LNAME) starting '{0}'
AND upper(FNAME) starting '{1}';"""

SQLT_FIND_CARD = """SELECT prof_exam_id, clinic_id_fk
FROM prof_exam_minor
WHERE people_id_fk = ?
AND card_status = 1
AND status_code = 2
AND clinic_id_fk = ?
AND ((date_begin = ?) OR (date_end = ?));"""

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

def mark_cards(clinic_id, c_arr, date_portal):
    import fdb
    from dbmis_connect2 import DBMIS

    try:
	dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
	# Create new READ ONLY READ COMMITTED transaction
	ro_transaction = dbc.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
	# and cursor
	ro_cur = ro_transaction.cursor()
	cur = dbc.con.cursor()
    except Exception, e:
	sout = "Error connecting to {0}:{1} : {2}".format(HOST, DB, e)
	log.error( sout )
	return

    i = 0
    for card in c_arr:
	i += 1
        card_number = card[0]
        fio = card[1].split(" ")
        card_date = card[2]
        lname1251 = fio[0].upper().encode('cp1251')
        fname1251 = fio[1].upper().encode('cp1251')
        if len(fio) < 3:
            s_sql = SQLT_FPEOPLE0.format(lname1251, fname1251)
        else:
            mname1251 = fio[2].upper().encode('cp1251')
            s_sql = SQLT_FPEOPLE.format(lname1251, fname1251, mname1251)

        ro_cur.execute(s_sql)
        results = ro_cur.fetchall()

        if len(results) == 0: continue

        for rec in results:
            people_id = rec[0]
            ro_cur.execute(SQLT_FIND_CARD, (people_id, clinic_id, card_date, card_date, ))
            rcards = ro_cur.fetchall()
	    if len(rcards) > 1:
		ncards = len(rcards)
		sout = "card_number: {0} people_id: {1} cards count: {2}".format(card_number, people_id, ncards)
		log.warn( sout )
            for rcard in rcards:
	        prof_exam_id = rcard[0]
		if i % STEP == 0:
		    sout = "{0} card_number: {1} people_id: {2} prof_exam_id: {3}".format(i, card_number, people_id, prof_exam_id)
		    log.info( sout )
                cur.execute(SQLT_CARD_U, (date_portal, prof_exam_id, ))
                dbc.con.commit()

    dbc.close()

def get_clinic_id(mcod):
    from dbmysql_connect import DBMY
    dbmy = DBMY()
    cursor = dbmy.con.cursor()
    s_sql = "SELECT clinic_id FROM pn_list WHERE mcod = %s;"
    cursor.execute(s_sql, (mcod, ))
    rec = cursor.fetchone()
    if rec is None:
        clinic_id = 0
    else:
        clinic_id = rec[0]

    dbmy.close()
    return clinic_id


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
        ifinish = fname.find(".xls")
        try:
	    mcod = int(fname[:ifinish])
	except:
	    sout = "Can't determine mcod from fname: {0}".format(fname)
	    log.warn( sout )
	    continue

	clinic_id = get_clinic_id(mcod)
	if clinic_id == 0:
	    sout = "Can't find clinic_id for mcod: {0}".format(mcod)
	    log.warn( sout )
	    continue

        sout = "Input file: {0} clinic_id: {1}".format(f_fname, clinic_id)
        log.info(sout)

        ar = get_cards(f_fname)
        l_ar = len(ar)
        sout = "File has got {0} cards".format(l_ar)
        log.info( sout )

        destination = PATH_OUT + "/" + fname

        mark_cards(clinic_id, ar, date_portal)

        shutil.move(f_fname, destination)


    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Registering PN Cards. Finish  '+localtime)

    sys.exit(0)
