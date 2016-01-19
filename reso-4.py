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
tip_op
FROM enp$card
WHERE activ=1 and sfile_id_fk is null;"""

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
        
        # look for existing record in the reso_list table
        cur.execute(s_sql_f1, (lname, fname, mname, birthday))
        rf1 = cur.fetchone()
        
        # skip in case this person was found in the reso_list table
        if rf1: continue
        
        nnf += 1

        oms_ser = rec[5]
        oms_num = rec[6]
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
        
        p = PEOPLE()
        p.uid = uid
        p.lname = lname
        p.fname = fname
        p.mname = mname
        p.birthday = birthday

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
        p.zip_code = zip_code
        
        p.dom = dom
        p.korp = korp
        p.kv = kv

        p.tip_op = tip_op
        
        if zip_code:
            s_address = zip_code + u", "
        else:
            s_address = u""
        if kladr:
            kladr_k1 = kladr[:13]
            kladr_k0 = kladr[:8] + "00000"
            r2_cur.execute(s_sql_kladr, (kladr_k0, ))
            raddr = r2_cur.fetchone()
            if raddr:
                name = raddr[0]
                socr = raddr[1]
                s_address += socr + u" " + name + ", "
                
            r2_cur.execute(s_sql_kladr, (kladr_k1, ))
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

if __name__ == "__main__":
    import time
    sout = "------------------------------------------------------------------------------"
    log.info(sout)
    localtime = time.asctime( time.localtime(time.time()) )
    sout = "Select ENP$CARDs. Start {0}".format(localtime)
    log.info(sout)    
    
    plist = get_plist()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info("Process ENP$CARDs. Finish {0}".format(localtime))
    
    sys.exit(0)
    