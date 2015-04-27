#!/usr/bin/python
# coding=utf8
# report-1824.py - отчет по задаче из redmine 1824
#             Отчет по заболеваемости из DBMIS
#

import os
import sys, codecs
import ConfigParser

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

import logging

LOG_FILENAME = '_report1824.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

Config = ConfigParser.ConfigParser()
#PATH = os.path.dirname(sys.argv[0])
PATH = os.getcwd()
FINI = PATH + "/" + "report.ini"

from ConfigSection import ConfigSectionMap
# read INI data
Config.read(FINI)
# [DBMIS]
Config1 = ConfigSectionMap(Config, "DBMIS")
HOST = Config1['host']
DB = Config1['db']

# [Report]
Config2 = ConfigSectionMap(Config, "Report")
D_START = Config2['d_start']
D_FINISH = Config2['d_finish']
DATE_RANGE = [D_START,D_FINISH]


F_PATH    = "./REPORT/"
FIN_NAME  = "rep1824.xls"
FOUT_NAME = "rep1824_out.xls"

STEP = 100

s_sqlt1 = """SELECT t.ticket_id, t.people_id_fk, clinic_id_fk, t.visit_date,
td.diagnosis_id_fk,
c.clinic_name
FROM tickets t
JOIN ticket_diagnosis td ON t.ticket_id = td.ticket_id_fk
LEFT JOIN clinics c ON t.clinic_id_fk = c.clinic_id
WHERE t.visit_date BETWEEN ? AND ?
AND td.visit_type_id_fk = 1
ORDER BY t.clinic_id_fk, t.people_id_fk;"""

if __name__ == "__main__":
    import sys
    import time
    from datetime import datetime

    import xlrd
    import xlwt
    from xlrd import open_workbook # http://pypi.python.org/pypi/xlrd
    from xlutils.save import save
    
    import fdb
    from dbmis_connect2 import DBMIS

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('-----------------------------------------------------------------------------------')
    log.info('Report 1824 Start {0}'.format(localtime))

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    dbc = DBMIS(mis_host = HOST, mis_db = DB)
    
    d_start = datetime.strptime(D_START, "%Y-%m-%d")
    sout = "d_start: {0}".format(d_start)
    log.info(sout)

    d_finish = datetime.strptime(D_FINISH, "%Y-%m-%d")
    sout = "d_finish: {0}".format(d_finish)
    log.info(sout)
    
    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = dbc.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    ro_cur = ro_transaction.cursor()
    
    ro_cur.execute(s_sqlt1, (d_start, d_finish, ))

    results = ro_cur.fetchall()

    fin_fname = F_PATH + FIN_NAME
    sout = "Template file: {0}".format(fin_fname)
    log.info( sout )
    rb = open_workbook(fin_fname,formatting_info=True)
    r_sheet = rb.sheet_by_index(0)
    l = 2

    fout_fname = F_PATH + FOUT_NAME
    sout = "Output file: {0}".format(fout_fname)
    log.info( sout )

    p_id = 0
    c_id = 0
    c_count_arr = []
    p_count_arr = []
    c_name_c = ''
    l_arr = 11
    for i in range(l_arr):
        c_count_arr.append(0)
        p_count_arr.append(0)

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Report 1824 Calculations Start {0}'.format(localtime))
        
    for rec in results:
        ticket_id = rec[0]
        people_id = rec[1]
        clinic_id = rec[2]
        visit_date = rec[3]
        ds_id     = rec[4]
        c_name    = rec[5]
        
        if clinic_id != c_id:
            if c_id != 0:

                localtime = time.asctime( time.localtime(time.time()) )
                sout = "Clinic {0}: {1}".format(c_id, c_name_c.encode('utf-8'))
                log.info(sout)
                log.info('{0}'.format(localtime))

                lout = False
                for i in range(l_arr):
                    if c_count_arr[i] > 0: lout = True
                    
                # http://www.lexicon.net/sjmachin/xlrd.html#xlrd.Cell-class
             
                if not lout:
                    log.info("=== 0 ===")
                else:
                    cell0 = r_sheet.cell(l,0)
                    r_sheet.put_cell(l,0,1,c_name_c,cell0.xf_index)
                    col = 0
                    for i in range(l_arr):
                        col += 1
                        cell = r_sheet.cell(l,col)
                        r_sheet.put_cell(l,col,2,c_count_arr[i],cell.xf_index)
                    l += 1
                
            c_id = clinic_id
            p_id = people_id
            c_name_c = c_name
            for i in range(l_arr):
                c_count_arr[i] = 0
                p_count_arr[i] = 0
                
        
        if p_id != people_id:
            for i in range(l_arr):
                c_count_arr[i] += p_count_arr[i]
                p_count_arr[i] = 0
                
        # проверяем значение МКБ на попадание в каждый столбец
        
        # 0: I00-I99
        ds3 = ds_id[:3]
        if (ds3 >= 'I00') and (ds3 <= 'I99') and (p_count_arr[0] ==0):
            p_count_arr[0] = 1
            
        # 1: I20.0
        ds5 = ds_id[:5]
        if (ds5 == 'I20.0') and (p_count_arr[1] ==0):
            p_count_arr[1] = 1
            
        # 2: I21.0-I21.9
        if (ds5 >= 'I21.0') and (ds5 <= 'I21.9') and (p_count_arr[2] ==0):
            p_count_arr[2] = 1
            
        # 3: I22.0-I22.9
        if (ds5 >= 'I22.0') and (ds5 <= 'I22.9') and (p_count_arr[3] ==0):
            p_count_arr[3] = 1
        
        # 4: I24.1, I24.8, 24.9
        if (ds5 in ('I24.1', 'I24.8', 'I24.9')) and (p_count_arr[4] ==0):
            p_count_arr[4] = 1

        # 5: I60.0-I60.9
        if (ds5 >= 'I60.0') and (ds5 <= 'I60.9') and (p_count_arr[5] ==0):
            p_count_arr[5] = 1
        
        # 6: I61.0-I61.9, I62.0, I62.1, I62.9
        if (((ds5 >= 'I61.0') and (ds5 <= 'I61.9')) \
           or (ds5 in ('I62.0', 'I62.1', 'I62.9'))) \
           and (p_count_arr[6] ==0):
            p_count_arr[6] = 1
        
        # 7: I63.0-I63.9
        if (ds5 >= 'I63.0') and (ds5 <= 'I63.9') and (p_count_arr[7] ==0):
            p_count_arr[7] = 1
            
        # 8: I64
        if (ds3 == 'I64') and (p_count_arr[8] ==0):
            p_count_arr[8] = 1

        # 9: J00-J98
        if (ds3 >= 'J00') and (ds3 <= 'J98') and (p_count_arr[9] ==0):
            p_count_arr[9] = 1
            
        # 10: J12-J16, J18
        if (((ds3 >= 'J12') and (ds3 <= 'J18')) \
           or (ds3 == 'J18')) and (p_count_arr[10] ==0):
            p_count_arr[10] = 1
            

    save(rb,fout_fname)
    dbc.close()
    sys.exit(0)
    

