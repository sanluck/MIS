#!/usr/bin/python
# -*- coding: utf-8 -*-
# dvn-f6.py - обработка ответов из ТФОМС (файлы VT22M*.xls)
#I            сверка с реальной базой (fb.ctmed.ru:DBMIS)
#I            - если people_id в протоколе = people_id в реальной базе 
#I              (в рамках одного ЛПУ) - оставить в протоколе
#I            - оставить в Excel только строки, где Код ошибки = 54 или 57
#I            - ошибки с другими кодами удалите из протокола 
#I              (подразумевается, что мы их устранили)
#
# INPUT  DIRECTORY RVT2DO - файлы с ошибками в реестрах ДВН
# OUTPUT DIRECTORY RVTDONE
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()
from insorglist import InsorgInfoList
insorgs = InsorgInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_dvnf6.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

MIS_HOST = "fb.ctmed.ru"
MIS_DB   = "DBMIS"

f_t            = "1_t.xls"
RVT2DO_PATH    = "./RVT2DO"
RVTDONE_PATH   = "./RVTDONE"
CHECK_FILE     = False
REGISTER_FILE  = False

STEP = 100

def plist_in(fname):
# read xls file <fname>
# and get peoples list
    import xlrd
    
    workbook = xlrd.open_workbook(fname)
    
    worksheets = workbook.sheet_names()
    wshn0 = worksheets[0]
    worksheet = workbook.sheet_by_name(wshn0)

    curr_row = 2
    num_rows = worksheet.nrows - 1
    arr = []
    
    while curr_row < num_rows:
	curr_row += 1
	row = worksheet.row(curr_row)
	# Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
	c1_type = worksheet.cell_type(curr_row, 0)
	if c1_type != 2: continue
	    
	code        = worksheet.cell_value(curr_row, 4)
	people_id_s = worksheet.cell_value(curr_row, 11)
	people_id   = int(people_id_s)
	
	arr.append([people_id, code])
    
    workbook.release_resources()
    return arr

def get_cc_lines(db, people_id):
    s_sqlt = """SELECT clinical_checkup_id
    FROM clinical_checkups
    WHERE people_id_fk = {0}
    ORDER BY clinical_checkup_id;"""
    s_sql  = s_sqlt.format(people_id)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    recs = cursor.fetchall()
    ar = []
    for rec in recs:
	cc_id = rec[0]
	ar.append(cc_id)
	
    return ar
    

def pfile(f_in, f_out):
    from dbmis_connect2 import DBMIS
    from xlrd import open_workbook # http://pypi.python.org/pypi/xlrd
    from xlutils.save import save # http://pypi.python.org/pypi/xlutils
    
        
    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('RVT Processing Start {0}'.format(localtime))
    
    i1 = fname.find("VT22M")
    if i1 < 0:
	sout = "Wrong file name <{0}>".format(fname)
	log.warn(sout)
	return

    s_mcod = fname[i1+5:i1+11]
    mcod   = int(s_mcod)

    try:
	mo = modb[mcod]
	clinic_id = mo.mis_code
	sout = "clinic_id: {0} MO Code: {1}".format(clinic_id, mcod) 
	log.info(sout)
    except:
	sout = "Clinic not found for mcod = {0}".format(s_mcod)
	log.warn(sout)
	return

    
    dbc = DBMIS(clinic_id = clinic_id, mis_host = MIS_HOST, mis_db = MIS_DB)
    cursor = dbc.con.cursor()
    
    cname = dbc.name.encode('utf-8')

    sout = "cod_mo: {0} clinic_id: {1} clinic_name: {2} ".format(mcod, clinic_id, cname)
    log.info(sout)


    inBook = open_workbook(f_in)
    inSheets = inBook.sheet_names()
    wshn0 = inSheets[0]
    inSheet = inBook.sheet_by_name(wshn0)

    rb = open_workbook(f_t, formatting_info=True)
    r_sheet = rb.sheet_by_index(0) # read only copy to introspect the file
    
    # copy header
    value = inSheet.cell_value(0, 0)
    cell  = r_sheet.cell(0,0)
    r_sheet.put_cell(0,0,cell.ctype,value,cell.xf_index)

#    value = inSheet.cell_value(1, 0)
#    cell  = r_sheet.cell(1,0)
#    r_sheet.put_cell(1,0,cell.ctype,value,cell.xf_index)

    curr_row = 2
    num_rows = inSheet.nrows - 1

    out_row = 2

    numb_in  = 0
    numb_out = 0
    while curr_row < num_rows:
	curr_row += 1
	numb_in  += 1
	row = inSheet.row(curr_row)
	# Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
	c1_type = inSheet.cell_type(curr_row, 0)
	if c1_type != 2: continue
	    
	code        = inSheet.cell_value(curr_row, 4)
	people_id_s = inSheet.cell_value(curr_row, 11)
	people_id   = int(people_id_s)

	if numb_in % STEP == 0:
	    sout = " {0} people_id: {1}".format(numb_in, people_id)
	    log.info(sout)
	
	if not code in (54, 57): continue

	s_sqlt = """SELECT clinical_checkup_id
	FROM clinical_checkups
	WHERE (clinic_id_fk = {0}) AND (people_id_fk = {1});"""
	s_sql  = s_sqlt.format(clinic_id, people_id)

	cursor.execute(s_sql)
	rec = cursor.fetchone()
	if rec == None: continue

	out_row += 1
	numb_out += 1
	
	out_col  = 0
	for col in range(2,17):
	    in_cell = inSheet.cell(curr_row, col)
	    value = inSheet.cell_value(curr_row, col)
	    cell = r_sheet.cell(out_row,out_col)
	    try:
		r_sheet.put_cell(out_row,out_col,in_cell.ctype,value,cell.xf_index)
	    except:
		sout = "{0} put_cell error: ({1}, {2})".format(f_out, out_row, out_col)
		log.warn(sout)
	    out_col  += 1
	    

    
    save(rb,f_out)
    
    sout = "Input lines number: {0}".format(numb_in)
    log.info(sout)


    sout = "Output lines number: {0}".format(numb_out)
    log.info(sout)

    dbc.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('VT Processing Finish  '+localtime)

def get_fnames(path = RVT2DO_PATH, file_ext = '.xls'):
    
    import os    
    
    fnames = []
    for subdir, dirs, files in os.walk(path):
        for fname in files:
            if fname.find(file_ext) > 1:
                log.info(fname)
                fnames.append(fname)
    
    return fnames    

def register_rvt_done(db, mcod, clinic_id, fname):
    import datetime    

    dnow = datetime.datetime.now()
    sdnow = str(dnow)
    s_sqlt = """INSERT INTO
    rvt_done
    (mcod, clinic_id, fname, done)
    VALUES
    ({0}, {1}, '{2}', '{3}');
    """

    s_sql = s_sqlt.format(mcod, clinic_id, fname, sdnow)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    db.con.commit()

def rvt_done(db, mcod):

    s_sqlt = """SELECT
    fname, done
    FROM
    rvt_done
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
    from dbmysql_connect import DBMY

    log.info("======================= DVN-F6 ===========================================")

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

	f_in  = RVT2DO_PATH + "/" + fname
	f_out = RVTDONE_PATH + "/" + fname
	sout  = "Input file: {0}".format(f_in)
	log.info(sout)
    
	if CHECK_FILE: 
	    ldone, dfname, ddone = rvt_done(dbmy2, mcod)
	else:
	    ldone = False
	
	if ldone:
	    sout = "On {0} hase been done. Fname: {1}".format(ddone, dfname)
	    log.warn( sout )
	else:
	    pfile(f_in, f_out)
	    if REGISTER_FILE: register_rvt_done(dbmy2, mcod, clinic_id, fname)
    
    dbmy2.close()
    sys.exit(0)
