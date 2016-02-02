#!/usr/bin/python
# -*- coding: utf-8 -*-
# reso-5.py - выбор из DBMIS пациентов страховой компании
#             по номеру страховой компании (insorg_id)
#             с группировкой по месту работы (work_place)
#             и заполнение xls файлов данными из таблицы peoples
#

import logging
import sys, codecs

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_reso5.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

CLINIC_ID = 125
INSORG_ID = 8
# OMS_SER = u"АК%"
OMS_SER = None
DATE_START = "2015-01-01"

F_PATH    = "./RESO/"
FOUT_NAME = "work_place_out"

LIMIT = 20

STEP = 200
STEP_F = 2000

s_sqlt_wp = """SELECT p.work_place, count(p.work_place) as w_count
FROM peoples p
WHERE p.insorg_id = ?
GROUP BY p.work_place
ORDER BY w_count DESC;"""

s_sqlt0 = """SELECT 
people_id, lname, fname, mname, birthday 
FROM peoples p
WHERE insorg_id = ? 
AND work_place = ?;"""

s_sqlt0t = """SELECT 
ticket_id
FROM tickets 
WHERE people_id_fk = ? 
AND visit_date >= ?;"""

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

def get_wlist(insorg_id=INSORG_ID, limit=LIMIT):
    import fdb
    from dbmis_connect2 import DBMIS
    from people import PEOPLE

    sout = "database: {0}:{1}".format(HOST, DB)
    log.info( sout )
    dbc  = DBMIS(mis_host = HOST, mis_db = DB)
    sout = "limit: {0}".format(limit)
    log.info(sout)
    
    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = dbc.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    ro_cur = ro_transaction.cursor()

    wlist = []

    ro_cur.execute(s_sqlt_wp, (insorg_id, ))
    results = ro_cur.fetchall()
    
    for rec in results:
        w_place = rec[0]
        w_count = rec[1]
        
        if w_count < limit: break

        wlist.append(w_place)
        
    dbc.close()
    
    return wlist

def get_plist(work_place, insorg_id=INSORG_ID):
    import fdb
    from dbmis_connect2 import DBMIS
    from people import PEOPLE

    sout = "database: {0}:{1}".format(HOST, DB)
    log.info( sout )
    dbc  = DBMIS(mis_host = HOST, mis_db = DB)
    wplace = work_place.encode('utf-8')
    sout = "work_place: {0}".format(wplace)
    log.info(sout)
    
    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = dbc.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    ro_cur = ro_transaction.cursor()

    plist = []

    ro_cur.execute(s_sqlt0, (insorg_id, work_place, ))
    results = ro_cur.fetchall()
    
    for rec in results:
        people_id = rec[0]
        lname = rec[1]
        fname = rec[2]
        mname = rec[3]
        birthday = rec[4]

        p = PEOPLE()
        p.people_id = people_id
        p.lname    = lname.strip()
        p.fname    = fname.strip()
        if mname:
            p.mname    = mname.strip()
        else:
            p.mname    = None
        p.birthday = birthday
        
        plist.append(p)
        
    dbc.close()
    
    return plist

def export_wplace(w_place, w_place_id):
    import datetime
    
    import xlwt
    
    import fdb
    from dbmis_connect2 import DBMIS
    from people import get_people

    plist = get_plist(work_place=w_place)
    l_plist = len(plist)

    sout = "Have got {0} peoples matching criteria".format(l_plist)
    log.info( sout )

    date_start = datetime.datetime.strptime(DATE_START, "%Y-%m-%d")
    sout = "date_start: {0}".format(date_start)
    log.info( sout )

    dbc  = DBMIS(mis_host = HOST, mis_db = DB)

    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = dbc.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    ro_cur = ro_transaction.cursor()

    wb = xlwt.Workbook(encoding='cp1251')
    ws = wb.add_sheet('Peoples')

    row = 0
    ws.write(row,0,w_place)
    
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
    
    counta = 0
    countp = 0
    fnumber = 1
    for p in plist:
        
        counta += 1
        
        lname = p.lname
        fname = p.fname
        mname = p.mname
        dr    = p.birthday
        p_id  = p.people_id

        ro_cur.execute(s_sqlt0t,(p_id, date_start,))
        tickets = ro_cur.fetchall()
        if len(tickets) == 0: 
            continue

        ro_cur.execute(s_sqlt3,(p_id,))
        rfs = ro_cur.fetchall()
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
            mobile_phone             = rf[18]
                
            row += 1

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

            countp += 1

                
            if countp % STEP == 0:
                sout = "{0}/{1} {2} {3} {4} {5}".format(countp, counta, p_id, \
                                                        lname.encode('utf-8'), \
                                                        fname.encode('utf-8'), \
                                                        mname.encode('utf-8'))
                log.info( sout )

    dbc.close()

    f_fname = F_PATH + FOUT_NAME + "-" + "%03d" % w_place_id + ".xls"
    wb.save(f_fname)

    sout = "File {0} has been written".format(f_fname)
    log.info( sout )

if __name__ == "__main__":
    import sys

    wlist = get_wlist()
    
    w_place_id = 0
    for w_place in wlist:
        export_wplace(w_place, w_place_id)
        w_place_id += 1
        
    sys.exit(0)
    