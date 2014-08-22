#!/usr/bin/python
# -*- coding: utf-8 -*-
# d-list1.py - заполнение таблицы mis.md$list
#                 из xlsx файла
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

from dbmysql_connect import DBMY

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_d_list1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

SQLT_ILIST = """INSERT INTO md$list (md_id, lname, fname, mname, sex, birth_dt,
b_cert_id, b_cert_type, b_cert_ser, b_cert_num, b_cert_date, b_cert_who,
b_cert_post, b_cert_when, b_cert_user, lpu, kladr_id_cr, cr, kladr_id, place, street,
b_weight, b_height, mkb_itog, mkb_1, mkb_2, mkb_3, mkb_4, mkb_5,
d_dt, d_age, d_place, d_cause, d_conf_who, d_conf_base, id_lpu)
VALUES
(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

SQLT_FLIST = """SELECT id
FROM md$list
WHERE md_id = %s;"""

F_NAME = "d_2014.csv"
F_PATH = "MEDDEM"
F_FNAME = F_PATH + "/" + F_NAME
SPLIT_CH = u"|"

APPEND = True

def get_md_list_from_csv(ffname = F_FNAME):
    from datetime import datetime

    sout = "Getting List of Cases from: {0}".format(ffname)
    log.info(sout)

    md_list = []

    fin = open( ffname, "r" )
    for line in fin:
	larr = line.decode('utf-8').split(SPLIT_CH)
	smd_id = larr[0]
	try:
	    md_id = int(smd_id)
	except:
	    continue

	lname       = larr[1]
	fname       = larr[2]
	mname       = larr[3]
	sex         = larr[4]
	if sex[0] == u'ж':
	    sex = u'Ж'
	elif sex[0] == u'м':
	    sex = u'М'
	else:
	    sex = u'-'

	sdt         = larr[5]
	if sdt.count(':') == 1:
	    try:
		birth_dt    = datetime.strptime(sdt, '%d.%m.%Y %H:%M')
	    except Exception, e:
		warn_msg = "Get data from CSV file error: {0}".format(e)
		log.warn(warn_msg)
		sout = "md_id: {0}".format(md_id)
		log.warn(sout)
		birth_dt = None
	elif sdt.count(':') == 2:
	    try:
		birth_dt    = datetime.strptime(sdt, '%d.%m.%Y %H:%M:%S')
	    except Exception, e:
		warn_msg = "Get data from CSV file error: {0}".format(e)
		log.warn(warn_msg)
		sout = "md_id: {0}".format(md_id)
		log.warn(sout)
		birth_dt = None
	else:
	    birth_dt    = None
	b_cert_id   = int(larr[6])
	b_cert_type = larr[7]
	b_cert_ser  = larr[8]
	b_cert_num  = larr[9]
	try:
	    b_cert_date = datetime.strptime(larr[10], '%d.%m.%Y')
	except Exception, e:
	    warn_msg = "Get data from CSV file error: {0}".format(e)
	    log.warn(warn_msg)
	    sout = "md_id: {0}".format(md_id)
	    log.warn(sout)
	    b_cert_date = None
	b_cert_who  = larr[11]
	b_cert_post = larr[12]
	sdt = larr[13]
	if sdt.count(':') == 1:
	    b_cert_when    = datetime.strptime(sdt, '%d.%m.%Y %H:%M')
	elif sdt.count(':') == 2:
	    b_cert_when    = datetime.strptime(sdt, '%d.%m.%Y %H:%M:%S')
	else:
	    b_cert_when    = None
	b_cert_user = larr[14]
	lpu         = larr[15]
	try:
	    kladr_id_cr = int(larr[16])
	except:
	    kladr_id_cr = None
	cr          = larr[17]
	try:
	    kladr_id    = int(larr[18])
	except:
	    kladr_id    = None
	place       = larr[19]
	street      = larr[20]
	try:
	    b_weight    = int(larr[21])
	except:
	    b_weight    = None
	try:
	    b_height    = int(larr[22])
	except:
	    b_height    = None
	mkb_itog    = larr[23]
	mkb_1       = larr[24]
	mkb_2       = larr[25]
	mkb_3       = larr[26]
	mkb_4       = larr[27]
	mkb_5       = larr[28]

	sdt = larr[29]
	if sdt.count(':') == 1:
	    d_dt    = datetime.strptime(sdt, '%d.%m.%Y %H:%M')
	elif sdt.count(':') == 2:
	    d_dt    = datetime.strptime(sdt, '%d.%m.%Y %H:%M:%S')
	else:
	    d_dt    = None

	try:
	    d_age       = int(larr[30])
	except:
	    d_age = None
	d_place     = larr[31]
	d_cause     = larr[32]
	d_conf_who  = larr[33]
	d_conf_base = larr[34]
	try:
	    id_lpu      = int(larr[35])
	except:
	    id_lpu = None

	md_list.append([md_id, lname, fname, mname, sex, birth_dt,\
	                b_cert_id, b_cert_type, b_cert_ser, b_cert_num,\
	                b_cert_date, b_cert_who, b_cert_post, b_cert_when,
	                b_cert_user, lpu, kladr_id_cr, cr, kladr_id, place,\
	                street, b_weight, b_height, mkb_itog, mkb_1, mkb_2,\
	                mkb_3, mkb_4, mkb_5, d_dt, d_age, d_place, d_cause,\
	                d_conf_who, d_conf_base, id_lpu])

    nnn = len(md_list)
    sout = "Have got {0} Cases Totally".format(nnn)
    log.info(sout)
    return md_list


def get_md_list(ffname = F_FNAME):
    import xlrd


    sout = "Getting List of Cases from: {0}".format(ffname)
    log.info(sout)

    md_list = []

    workbook = xlrd.open_workbook(ffname)
    worksheets = workbook.sheet_names()
    wsh = worksheets[0]

    worksheet = workbook.sheet_by_name(wsh)
    curr_row = 0
    num_rows = worksheet.nrows - 1

    while curr_row < num_rows:
	curr_row += 1
	# Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
	c1_type = worksheet.cell_type(curr_row, 0)
	if c1_type != 2: continue

	md_id = worksheet.cell_value(curr_row, 0)

	lname       = worksheet.cell_value(curr_row, 1)
	fname       = worksheet.cell_value(curr_row, 2)
	mname       = worksheet.cell_value(curr_row, 3)
	sex         = worksheet.cell_value(curr_row, 4)
	if sex[0] == u'ж':
	    sex = u'Ж'
	elif sex[0] == u'м':
	    sex = u'М'
	else:
	    sex = u'-'

	birth_dt    = worksheet.cell_value(curr_row, 5)
	b_cert_id   = worksheet.cell_value(curr_row, 6)
	b_cert_type = worksheet.cell_value(curr_row, 7)
	b_cert_ser  = worksheet.cell_value(curr_row, 8)
	b_cert_num  = worksheet.cell_value(curr_row, 9)
	b_cert_date = worksheet.cell_value(curr_row, 10)
	b_cert_who  = worksheet.cell_value(curr_row, 11)
	b_cert_post = worksheet.cell_value(curr_row, 12)
	b_cert_when = worksheet.cell_value(curr_row, 13)
	b_cert_user = worksheet.cell_value(curr_row, 14)
	lpu         = worksheet.cell_value(curr_row, 15)
	kladr_id_cr = worksheet.cell_value(curr_row, 16)
	cr          = worksheet.cell_value(curr_row, 17)
	kladr_id    = worksheet.cell_value(curr_row, 18)
	place       = worksheet.cell_value(curr_row, 19)
	street      = worksheet.cell_value(curr_row, 20)
	b_weight    = worksheet.cell_value(curr_row, 21)
	b_height    = worksheet.cell_value(curr_row, 22)
	mkb_itog    = worksheet.cell_value(curr_row, 23)
	mkb_1       = worksheet.cell_value(curr_row, 24)
	mkb_2       = worksheet.cell_value(curr_row, 25)
	mkb_3       = worksheet.cell_value(curr_row, 26)
	mkb_4       = worksheet.cell_value(curr_row, 27)
	mkb_5       = worksheet.cell_value(curr_row, 28)
	d_dt        = worksheet.cell_value(curr_row, 29)
	d_age       = worksheet.cell_value(curr_row, 30)
	d_place     = worksheet.cell_value(curr_row, 31)
	d_cause     = worksheet.cell_value(curr_row, 32)
	d_conf_who  = worksheet.cell_value(curr_row, 33)
	d_conf_base = worksheet.cell_value(curr_row, 34)
	id_lpu      = worksheet.cell_value(curr_row, 35)

	md_list.append([md_id, lname, fname, mname, sex, birth_dt,\
	                b_cert_id, b_cert_type, b_cert_ser, b_cert_num,\
	                b_cert_date, b_cert_who, b_cert_post, b_cert_when,
	                b_cert_user, lpu, kladr_id_cr, cr, kladr_id, place,\
	                street, b_weight, b_height, mkb_itog, mkb_1, mkb_2,\
	                mkb_3, mkb_4, mkb_5, d_dt, d_age, d_place, d_cause,\
	                d_conf_who, d_conf_base, id_lpu])

    nnn = len(md_list)
    sout = "Have got {0} Cases Totally".format(nnn)
    log.info(sout)
    return md_list

if __name__ == "__main__":
    import time
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('----------------------------------------------------------------------------------')
    log.info('Load MedDem Cases List. Start {0}'.format(localtime))

    md_list = get_md_list_from_csv()

    dbmy = DBMY()
    curr = dbmy.con.cursor()
    curm = dbmy.con.cursor()

    if not APPEND:
	ssql = "TRUNCATE TABLE md$list;"
	curm.execute(ssql)
	dbmy.con.commit()

    nnn = 0
    for md in md_list:
	md_id       = md[0]
	curr.execute(SQLT_FLIST,(md_id,))
	rec = curr.fetchone()
	if rec is not None: continue

	lname       = md[1]
	fname       = md[2]
	mname       = md[3]
	sex         = md[4]
	birth_dt    = md[5]
	b_cert_id   = md[6]
	b_cert_type = md[7]
	b_cert_ser  = md[8]
	b_cert_num  = md[9]
	b_cert_date = md[10]
	b_cert_who  = md[11]
	b_cert_post = md[12]
	b_cert_when = md[13]
	b_cert_user = md[14]
	lpu         = md[15]
	kladr_id_cr = md[16]
	cr          = md[17]
	kladr_id    = md[18]
	place       = md[19]
	street      = md[20]
	b_weight    = md[21]
	b_height    = md[22]
	mkb_itog    = md[23]
	mkb_1       = md[24]
	mkb_2       = md[25]
	mkb_3       = md[26]
	mkb_4       = md[27]
	mkb_5       = md[28]
	d_dt        = md[29]
	d_age       = md[30]
	d_place     = md[31]
	d_cause     = md[32]
	d_conf_who  = md[33]
	d_conf_base = md[34]
	id_lpu      = md[35]

	try:
	    curm.execute(SQLT_ILIST, (md_id, lname, fname, mname, sex, birth_dt,\
	                         b_cert_id, b_cert_type, b_cert_ser, \
	                         b_cert_num, b_cert_date, b_cert_who, \
	                         b_cert_post, b_cert_when, b_cert_user, lpu,
	                         kladr_id_cr, cr, kladr_id, place, street, \
	                         b_weight, b_height, mkb_itog, mkb_1, mkb_2, \
	                         mkb_3, mkb_4, mkb_5, d_dt, d_age, d_place, \
	                         d_cause, d_conf_who, d_conf_base, id_lpu, ))
	    nnn += 1
	except Exception, e:
	    warn_msg = "Insert into md$list error: {0}".format(e)
	    log.warn(warn_msg)
	    sout = "md_id: {0}".format(md_id)
	    log.warn(sout)

    dbmy.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Load MedDem Cases List. Finish  '+localtime)
    sout = "Totally {0} recors have been inserted into md$list table".format(nnn)
    log.info(sout)

    sys.exit(0)
