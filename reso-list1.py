#!/usr/bin/python
# -*- coding: utf-8 -*-
# reso-list1.py - загрузка списка застрахованных
#                 из xls файлов
#                 в таблицу reso_list
#

import logging
import os, sys, codecs
import shutil
import time

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_reso_list.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

F_PATH    = "./RESO"

O_PATH    = "./RESO_OUT"

ssql_insert = """INSERT INTO reso_list 
(lname, fname, mname, birthday, oms_ser, oms_num, snils) 
VALUES
(%s, %s, %s, %s, %s, %s, %s);"""

def get_fnames(path = F_PATH, file_ext = '.xls'):

    import os

    fnames = []
    for subdir, dirs, files in os.walk(path):
        for fname in files:
            if fname.find(file_ext) > 1:
                log.info(fname)
                fnames.append(fname)

    return fnames

def get_plist(fname):
    
    import xlrd
    from datetime import datetime
    from datetime import timedelta
    from people import PEOPLE

    plist = []
    try:
        workbook = xlrd.open_workbook(fname)
    except:
        exctype, value = sys.exc_info()[:2]
        log.error( 'Can not open xls file: {0}'.format(fname) )
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
            
            c4_type = worksheet.cell_type(curr_row, 3)
            c4_value = worksheet.cell_value(curr_row, 3)
            if c4_type == 1:
                
                
                try:
                    dr = datetime.strptime(c4_value, "%Y-%m-%d")
                except:
                    continue
                
                lname   = worksheet.cell_value(curr_row, 0)
                fname   = worksheet.cell_value(curr_row, 1)
                mname   = worksheet.cell_value(curr_row, 2)
                oms_ser = worksheet.cell_value(curr_row, 4)
                oms_num = worksheet.cell_value(curr_row, 5)
                snils   = worksheet.cell_value(curr_row, 6)
                
                p = PEOPLE()
                p.lname    = lname.strip().upper()
                p.fname    = fname.strip().upper()
                p.mname    = mname.strip().upper()
                p.birthday = dr
                p.oms_ser  = oms_ser.strip()
                p.oms_num  = oms_num.strip()
                p.snils    = snils.strip().replace(' ','').replace('-','')
                
                plist.append(p)

    return plist

def save_plist(plist):
    from dbmysql_connect import DBMY

    dbmy = DBMY()
    cur = dbmy.con.cursor()
    
    nnn = 0
    for p in plist:

        lname    = p.lname
        fname    = p.fname
        mname    = p.mname
        birthday = p.birthday
        oms_ser  = p.oms_ser
        oms_num  = p.oms_num
        snils    = p.snils
        if len(snils) == 0: snils = None
        
        try:
            cur.execute(ssql_insert, (lname, fname, mname, \
                                      birthday,
                                      oms_ser, oms_num, snils))
            
            nnn += 1
        except Exception, e:
            #sout = "Insert error: {0}".format(e)
            #log.info(sout)
            continue

    dbmy.close()
    
    return nnn
    

if __name__ == "__main__":
    
    fnames = get_fnames()
    n_fnames = len(fnames)
    sout = "Totally {0} files has been found".format(n_fnames)
    log.info( sout )
    
    for fname in fnames:

        f_fname = F_PATH + "/" + fname
        sout = "Input file: {0}".format(f_fname)
        log.info(sout)
        
        p_list = get_plist(f_fname)
        sout = "Have got {0} peoples".format(len(p_list))
        log.info( sout )
        
        nnn = save_plist(p_list)
        sout = "{0} records has been written".format(len(p_list))
        log.info( sout )

        source = f_fname
        destination = O_PATH + "/" + fname
        shutil.move(source, destination)
        
    sys.exit(0)
    