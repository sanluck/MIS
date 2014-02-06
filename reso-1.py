#!/usr/bin/python
# -*- coding: utf-8 -*-
# reso-1.py - чтение номеров полисов ОМС из файла xls
#             поиск в DBMIS и заполнение xls файла данными
#             из таблицы peoples
#

import logging
import sys, codecs

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_reso1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

CLINIC_ID = 22

F_PATH    = "./RESO/"
FIN_NAME  = "reso1.xls"
FOUT_NAME = "reso1_out.xls"

STEP = 100

s_sqlt1 = """SELECT
document_type_id_fk, document_series, document_number, insurance_certificate,
addr_jure_town_name, addr_jure_town_socr, 
addr_jure_area_name, addr_jure_area_socr,
addr_jure_country_name, addr_jure_country_socr,
addr_jure_street_name, addr_jure_street_socr, 
addr_jure_house, addr_jure_flat,
phone, mobile_phone
FROM peoples
WHERE medical_insurance_series = ?
AND medical_insurance_number = ?"""

s_sqlt2 = """SELECT
medical_insurance_series,
document_type_id_fk, document_series, document_number, insurance_certificate,
addr_jure_town_name, addr_jure_town_socr, 
addr_jure_area_name, addr_jure_area_socr,
addr_jure_country_name, addr_jure_country_socr,
addr_jure_street_name, addr_jure_street_socr, 
addr_jure_house, addr_jure_flat,
phone, mobile_phone
FROM peoples
WHERE medical_insurance_number = ?"""

s_sqlt3 = """SELECT
medical_insurance_series, medical_insurance_number,
document_type_id_fk, document_series, document_number, document_when,
insurance_certificate,
addr_jure_town_name, addr_jure_town_socr, 
addr_jure_area_name, addr_jure_area_socr,
addr_jure_country_name, addr_jure_country_socr,
addr_jure_street_name, addr_jure_street_socr, 
addr_jure_house, addr_jure_flat,
phone, mobile_phone
FROM peoples
WHERE people_id = ?"""

def get_plist(fname):
    
    import xlrd
    from datetime import datetime
    from people import PEOPLE

    plist = []
    try:
	workbook = xlrd.open_workbook(fname)
    except:
	exctype, value = sys.exc_info()[:2]
	log.error( 'Can not open xls file: {0}'.format(ffname) )
	log.error( '{0}'.format(value.message) )
	return plist
 

    worksheets = workbook.sheet_names()
    wshn0 = worksheets[0]
    worksheet = workbook.sheet_by_name(wshn0)

    num_rows = worksheet.nrows - 1
    curr_row = 4
    ncount = 0
    nimscount = 0
    while curr_row < num_rows:
	    curr_row += 1
	    row = worksheet.row(curr_row)
	    # Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
	    c1_type = worksheet.cell_type(curr_row, 0)
	    c1_value = worksheet.cell_value(curr_row, 0)
	    c2_type = worksheet.cell_type(curr_row, 1)
	    c2_value = worksheet.cell_value(curr_row, 1)
	    
	    c7_type = worksheet.cell_type(curr_row, 6)
	    if c7_type == 2:
		
		lname  = worksheet.cell_value(curr_row, 0)
		fname  = worksheet.cell_value(curr_row, 1)
		mname  = worksheet.cell_value(curr_row, 2)
		
		s_dr   = worksheet.cell_value(curr_row, 3)
		dr     = datetime.strptime(s_dr, '%Y-%m-%d')
		
		p = PEOPLE()
		p.lname    = lname
		p.fname    = fname
		p.mname    = mname
		p.birthday = dr
		
		plist.append(p)

    return plist

if __name__ == "__main__":
    import sys
    import time
    import datetime
    
    import xlwt
    
    from dbmis_connect2 import DBMIS
    from people import get_people

    f_fname = F_PATH + FIN_NAME
    plist = get_plist(f_fname)
    l_plist = len(plist)
    sout = "Have got {0} peoples from {1}".format(l_plist, f_fname)
    log.info( sout )

    sout = "clinic_id: {0} database: {1}:{2}".format(CLINIC_ID, HOST, DB)
    log.info( sout )
    clinic_id = CLINIC_ID
    
    dbc  = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
    curr = dbc.con.cursor()
    cur2 = dbc.con.cursor()

    f_fname = F_PATH + FOUT_NAME    
    
    wb = xlwt.Workbook(encoding='cp1251')
    ws = wb.add_sheet('Peoples')
    
    row = 3
    ws.write(row,0,u'Фамилия')
    ws.write(row,1,u'Имя')
    ws.write(row,2,u'Отчетсво')
    ws.write(row,3,u'Дата рождения')
    ws.write(row,4,u'Серия ОМС')
    ws.write(row,5,u'Номер ОМС')    
    ws.write(row,6,u'СНИЛС')    
    ws.write(row,7,u'Тип УДЛ')
    ws.write(row,8,u'Серия УДЛ')
    ws.write(row,9,u'Номер УДЛ')
    ws.write(row,10,u'Дата выдачи УДЛ')
    ws.write(row,11,u'Телефон')
    ws.write(row,12,u'Сотовый')
    ws.write(row,13,u'Адрес')
    
    row += 1
    
    for p in plist:
	lname = p.lname
	fname = p.fname
	mname = p.mname
	dr    = p.birthday
	
	results = get_people(curr, lname, fname, mname, dr)
	for rec in results:
	    people_id = rec[0]
	    cur2.execute(s_sqlt3,(people_id,))
	    rfs = cur2.fetchall()
	    for rf in rfs:
		medical_insurance_series = rf[0]
		medical_insurance_number = rf[1]
		document_type_id_fk      = rf[2]
		document_series          = rf[3]
		document_number          = rf[4]
		document_when            = rf[5]
		insurance_certificate    = rf[6]
		addr_jure_town_name      = rf[7]
		addr_jure_town_socr      = rf[8]
		addr_jure_area_name      = rf[9]
		addr_jure_area_socr      = rf[10]
		addr_jure_country_name   = rf[11]
		addr_jure_country_socr   = rf[12]
		addr_jure_street_name    = rf[13]
		addr_jure_street_socr    = rf[14]
		addr_jure_house          = rf[15]
		addr_jure_flat           = rf[16]
		phone                    = rf[17]
		mobile_phone		= rf[18]
		
		row += 1
		
		if row % STEP == 0:
		    
		    sout = "{0} {1} {2} {3} {4}".format(row, people_id, lname.encode('utf-8'), fname.encode('utf-8'), mname.encode('utf-8'))
		    log.info( sout )

		ws.write(row,0,lname)
		ws.write(row,1,fname)
		ws.write(row,2,mname)
		s_dr = u"%04d-%02d-%02d" % (dr.year, dr.month, dr.day)
		ws.write(row,3,s_dr)

		ws.write(row,4,medical_insurance_series)
		ws.write(row,5,medical_insurance_number)
		ws.write(row,6,insurance_certificate)
		ws.write(row,7,document_type_id_fk)
		ws.write(row,8,document_series)
		ws.write(row,9,document_number)
		
		if document_when is None:
		    s_dw = ''
		else:
		    s_dw = u"%04d-%02d-%02d" % (document_when.year, document_when.month, document_when.day)
		    
		ws.write(row,10,s_dw)

		ws.write(row,11,phone)
		ws.write(row,12,mobile_phone)

		
		addr = u''
		
		if addr_jure_town_name is not None:
		    addr += addr_jure_town_name 
		    if addr_jure_town_socr is not None:
			addr += u' ' + addr_jure_town_socr + u', '
		    else:
			addr += u', '
		if addr_jure_area_name is not None:
		    addr += addr_jure_area_name
		    if addr_jure_area_socr is not None:
			addr += u' ' + addr_jure_area_socr + u', '
		    else:
			addr += u', '
	        if addr_jure_country_name is not None:
		    addr += addr_jure_country_name
		    if addr_jure_country_socr is not None:
			addr += u' ' + addr_jure_country_socr + u', '
		    else:
			addr += u', '
		
		if addr_jure_street_name is not None:
		    addr += addr_jure_street_name
		    if addr_jure_street_socr is not None:
			addr += u' ' + addr_jure_street_socr
		
		if addr_jure_house is not None:
		    addr += u', дом ' + addr_jure_house

		if addr_jure_flat is not None:
		    addr += u', кв. ' + addr_jure_flat
		
		ws.write(row,13,addr)
		
    
    dbc.close()
    wb.save(f_fname)
    sys.exit(0)
    

