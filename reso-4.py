#!/usr/bin/python
# -*- coding: utf-8 -*-
# reso-4.py - выбор из KISSMO действующих карт
#             enp$card where activ=1 and sfile_id_fk is null
#             sfile_id_fk is not null - полис закрыт
#

import logging
import sys, codecs

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_reso4.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

KISSMO_HOST = "tb.ctmed.ru"
KISSMO_DB   = "KISSMO"
KISSMO_PW   = "Gjkbrkbybrf"

DB_USER = 'sysdba'
DB_ROLE = 'supervisor'

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

CLINIC_ID = 125

F_PATH    = "./RESO/"
FOUT_NAME = "kissmo_out"

MODE = 2
# 1 - проверять наличие среди выгруженных ранее
# 2 - не проверять

STEP = 200
STEP_F = 2000

s_sqlt0 = """SELECT 
uid, person$fam, person$im, person$ot, person$dr,
insurance$polis$npolic, insurance$polis$spolic,
person$ss,
doc$type, doc$docser, doc$docnum, doc$docdate,
person$phone, person$contact, 
addres_p$kladr, addres_p$indx, 
addres_p$dom, addres_p$korp, addres_p$kv,
tip_op, 
person$w, doc$mr
FROM enp$card
WHERE activ=1 and sfile_id_fk is null
ORDER BY addres_p$kladr;"""

s_sql_kladr = """SELECT 
name, socr 
FROM kladr_kladr
WHERE code = ?;"""

s_sql_street = """SELECT 
name, socr 
FROM kladr_street
WHERE code = ?;"""

s_sql_f1 = """SELECT id
FROM reso_list 
WHERE 
lname = %s 
AND fname = %s 
AND mname = %s
AND birthday = %s;"""

def get_plist():
    import fdb
    from dbmysql_connect import DBMY
    from people import PEOPLE

    sout = "database: {0}:{1}".format(KISSMO_HOST, KISSMO_DB)
    log.info( sout )

    s_dsn = "%s:%s" % (KISSMO_HOST, KISSMO_DB)
    try:
        con = fdb.connect(dsn=s_dsn, user=DB_USER, password=KISSMO_PW, \
                          role = DB_ROLE, charset='WIN1251')
    except Exception, e:
        log.error( 'KISSMO init error: {0}'.format(e) )
        sys.exit(1)
    
    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    ro_cur = ro_transaction.cursor()
    r2_cur = ro_transaction.cursor()

    plist = []

    ro_transaction.begin()
    
    ro_cur.execute(s_sqlt0)

    results = ro_cur.fetchall()
    
    dbmy = DBMY()
    cur = dbmy.con.cursor()
    
    nnn = 0
    nnf = 0
    for rec in results:
        
        nnn += 1
        
        uid   = rec[0]
        lname = rec[1].strip().upper()
        fname = rec[2].strip().upper()
        if rec[3]:
            mname = rec[3].strip().upper()
        else:
            mname = None
        birthday = rec[4]
        
        if MODE == 1:
            # look for existing record in the reso_list table
            cur.execute(s_sql_f1, (lname, fname, mname, birthday))
            rf1 = cur.fetchone()
        
            # skip in case this person was found in the reso_list table
            if rf1: continue
        
        nnf += 1

        oms_num = rec[5]
        oms_ser = rec[6]
        snils = rec[7]

        doc_type = rec[8]
        doc_ser  = rec[9]
        doc_num  = rec[10]
        doc_date = rec[11]

        phone = rec[12]
        contact = rec[13]
        
        kladr = rec[14]
        zip_code = rec[15]
        
        dom = rec[16]
        korp = rec[17]
        kv = rec[18]

        tip_op = rec[19]
        
        person_w = rec[20]
        if person_w == 2:
            sex = u'Ж'
        else:
            sex = u'М'
            
        doc_mr = rec[21]
        
        p = PEOPLE()
        p.uid = uid
        p.lname = lname
        p.fname = fname
        p.mname = mname
        p.sex = sex
        p.birthday = birthday
        p.doc_mr = doc_mr

        p.oms_ser = oms_ser
        p.oms_num = oms_num
        if snils:
            p.snils = snils.strip().replace(' ','').replace('-','')
        else:
            p.snils = None

        p.doc_type = doc_type
        p.doc_ser  = doc_ser
        p.doc_num  = doc_num
        p.doc_date = doc_date

        p.phone = phone
        p.contact = contact
        
        p.kladr = kladr

        if zip_code:
            zip_code = zip_code.strip()
            if len(zip_code) == 0:
                zip_code == None
            p.zip_code = zip_code
        else:
            p.zip_code = None
        
        p.dom = dom
        p.korp = korp
        p.kv = kv

        p.tip_op = tip_op
        
        if zip_code:
            s_address = zip_code + u", "
        else:
            s_address = u""
        if kladr:
            kladr_k2 = kladr[:13]
            kladr_k1 = kladr[:8] + "00000"
            kladr_k0 = kladr[:2] + "00000000000"
            r2_cur.execute(s_sql_kladr, (kladr_k0, ))
            raddr = r2_cur.fetchone()
            if raddr:
                name = raddr[0]
                socr = raddr[1]
                if socr == u"Респ":
                    s_address += socr + u" " + name + ", "
                else:
                    s_address += name + u" " + socr + ", "
            
            if kladr_k1 != kladr_k0:
                r2_cur.execute(s_sql_kladr, (kladr_k1, ))
                raddr = r2_cur.fetchone()
                if raddr:
                    name = raddr[0]
                    socr = raddr[1]
                    if socr == u"р-н":
                        s_address += name + u" " + socr + ", "
                    else:
                        s_address += socr + u" " + name + ", "
                
            if kladr_k2 != kladr_k1:
                r2_cur.execute(s_sql_kladr, (kladr_k2, ))
                raddr = r2_cur.fetchone()
                if raddr:
                    name = raddr[0]
                    socr = raddr[1]
                    s_address += socr + u" " + name + ", "
                
            r2_cur.execute(s_sql_street, (kladr, ))
            raddr = r2_cur.fetchone()
            if raddr:
                name = raddr[0]
                socr = raddr[1]
                s_address += socr + u" " + name + ", "
                
        
        if dom:
            s_address += dom
        
        if korp:
            s_address += u"/" + korp
            
        if kv:
            s_address += u"-" + kv
        
        p.address = s_address
        
        plist.append(p)
        
    ro_transaction.commit()
    con.close()
    dbmy.close()
    
    sout = "We have got {0} of {1} records".format(nnf, nnn)
    log.info(sout)
    
    return plist

def export_plist(f_fname, plist):
    """
    Export plist content into xls files
    """
    import xlwt
    
    wb = xlwt.Workbook(encoding='cp1251')
    ws = wb.add_sheet('Peoples')
    
    row = 3
    ws.write(row,0,u'Фамилия')
    ws.write(row,1,u'Имя')
    ws.write(row,2,u'Отчетсво')
    ws.write(row,3,u'Пол')

    ws.write(row,4,u'Дата рождения')
    ws.write(row,5,u'Место рождения')

    ws.write(row,6,u'Серия ОМС')
    ws.write(row,7,u'Номер ОМС')    
    ws.write(row,8,u'СНИЛС')    
    ws.write(row,9,u'Тип УДЛ')
    ws.write(row,10,u'Серия УДЛ')
    ws.write(row,11,u'Номер УДЛ')
    ws.write(row,12,u'Дата выдачи УДЛ')
    ws.write(row,13,u'Телефон')
    ws.write(row,14,u'Контакт')
    ws.write(row,15,u'Адрес')
    ws.write(row,16,u'Тип оп.')
    
    row += 1
    
    for p in plist:
        lname = p.lname
        fname = p.fname
        mname = p.mname
        sex   = p.sex
        dr    = p.birthday
        mr    = p.doc_mr
        uid   = p.uid

        oms_ser = p.oms_ser
        oms_num = p.oms_num
        if p.snils:
            sss = p.snils
            if len(sss) < 11:
                snils = sss
            else:
                snils = sss[:3] + '-' + sss[3:6] + '-' + sss[6:9] + ' ' + sss[9:]
        else:
            snils = u"-"

        doc_type = p.doc_type
        doc_ser = p.doc_ser
        doc_num = p.doc_num
        doc_date = p.doc_date

        phone = p.phone
        contact = p.contact
        
        address = p.address
        tip_op = p.tip_op
            
        row += 1

        ws.write(row,0,lname)
        ws.write(row,1,fname)
        ws.write(row,2,mname)
        ws.write(row,3,sex)
        s_dr = u"%04d-%02d-%02d" % (dr.year, dr.month, dr.day)
        ws.write(row,4,s_dr)
        ws.write(row,5,mr)

        ws.write(row,6,oms_ser)
        ws.write(row,7,oms_num)
        ws.write(row,8,snils)
        ws.write(row,9,doc_type)
        ws.write(row,10,doc_ser)
        ws.write(row,11,doc_num)
            
        if doc_date is None:
            s_dw = ''
        else:
            s_dw = u"%04d-%02d-%02d" % (doc_date.year, doc_date.month, doc_date.day)
                
        ws.write(row,12,s_dw)

        ws.write(row,13,phone)
        ws.write(row,14,contact)
        
        ws.write(row,15,address)
        ws.write(row,16,tip_op)
    
    wb.save(f_fname)
    sout = "File {0} has been written".format(f_fname)
    log.info( sout )
    

if __name__ == "__main__":
    import time
    sout = "------------------------------------------------------------------------------"
    log.info(sout)
    localtime = time.asctime( time.localtime(time.time()) )
    sout = "Select ENP$CARDs. Start {0}".format(localtime)
    log.info(sout)    
    
    plist = get_plist()
    
    nfiles = len(plist) / STEP_F + 1
    
    if nfiles == 1:
        fname = F_PATH + FOUT_NAME + ".xls"
        export_plist(fname, plist)
    
    else:
        for i in range(nfiles):
            fname = F_PATH + FOUT_NAME + ("-%02d" % i) + ".xls"
            psublist = plist[i*STEP_F:(i+1)*STEP_F]
            export_plist(fname, psublist)
            

    localtime = time.asctime( time.localtime(time.time()) )
    log.info("Process ENP$CARDs. Finish {0}".format(localtime))
    
    sys.exit(0)
    