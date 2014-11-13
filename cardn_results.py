#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# cardn_results.py - результаты формирования файлов ПН для выгрузки в Минздрав
#
import os
import sys
import xlsxwriter

from medlib.modules.dbmysql_connect import DBMY

FRESULT = "cardn_results.xlsx"
DB = "mis"

if __name__ == "__main__":
    
    dbmy = DBMY(db=DB)
    cur= dbmy.con.cursor()
    ssql = "SELECT clinic_id, mcod, name, done, nout_all FROM pn_list;"
    cur.execute(ssql)
    results = cur.fetchall()

    # Create an new Excel file and add a worksheet.
    workbook = xlsxwriter.Workbook(FRESULT)
    worksheet = workbook.add_worksheet()
    
    worksheet.write(0, 0, "clinic_id")
    worksheet.write(0, 1, "mcod")
    worksheet.write(0, 2, "Clinic Name")
    worksheet.write(0, 3, "Done")
    worksheet.write(0, 4, "Cards")
    # Widen the third column to make the text clearer.
    worksheet.set_column('C:C', 75)    
    
    row = 0
    
    for rec in results:
        clinic_id = rec[0]
        mcod = rec[1]
        name = rec[2]
        done = rec[3]
        nout_all = rec[4]
        
        row += 1
        worksheet.write(row, 0, clinic_id)
        worksheet.write(row, 1, mcod)
        worksheet.write(row, 2, name)
        worksheet.write(row, 3, done)
        worksheet.write(row, 4, nout_all)
        
    workbook.close()
    dbmy.close()
    sys.exit(0)
    