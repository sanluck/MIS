#!/usr/bin/python
# -*- coding: utf-8 -*-
# em-clist.py - список клиник для выборки пациентов

clist = [51,222,229,215,227,228,224,223,220,225,231,230,232,233,235,234,238,236,237,226,239,254,52,306,246,255,300,299,280,268]

def get_clist_from_xls(fname = 'em_clist.xls'):
    import xlrd
    
    workbook = xlrd.open_workbook(fname)
    worksheets = workbook.sheet_names()
    wsh = worksheets[0]
    worksheet = workbook.sheet_by_name(wsh)
    
    m = []

    curr_row = 0
    num_rows = worksheet.nrows - 1
    num_clinics = 0
    while curr_row < num_rows:
        curr_row += 1
        # Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
        c1_type = worksheet.cell_type(curr_row, 0)
        if c1_type != 2: continue
        clinic_id = int(worksheet.cell_value(curr_row, 0))
        m.append(clinic_id)
        
    
    return m
