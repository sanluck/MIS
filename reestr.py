#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import logging

log = logging.getLogger(__name__)

class REESTR:
    def __init__(self):
        self.kat = None
        self.kod_lpu = None
        self.dn  = None
        self.dk  = None
        self.fam = None
        self.im  = None
        self.ot  = None
        self.dr  = None
        self.w   = None
        self.kpl = None
        self.vpolis = None
        self.spolis = None
        self.npolis = None
        self.str_pens = None
        self.ds = None
        self.uet = None
        self.profil = None
        self.data_p = None
        self.spec = None
        self.kod_i = None
        self.f_doctor = None
        self.perv_povt = None
        self.cel_posobr = None
    
    def initFromDBF(self, rec):

        self.kat = rec.kat
        self.kod_lpu = rec.kod_lpu
        self.dn  = rec.dn
        self.dk  = rec.dk
        self.fam = rec.fam
        self.im  = rec.im
        self.ot  = rec.ot
        self.dr  = rec.dr
        self.w   = rec.w
        self.kpl = rec.kpl
        self.vpolis = rec.vpolis
        self.spolis = rec.spolis
        self.npolis = rec.npolis
        self.str_pens = rec.str_pens
        self.ds = rec.ds
        self.uet = rec.uet
        self.profil = rec.profil
        self.data_p = rec.data_p
        self.spec = rec.spec
        self.kod_i = rec.kod_i
        self.f_doctor = rec.f_doctor
        self.perv_povt = rec.perv_povt
        self.cel_posobr = rec.cel_posobr
    
def get_reestr(table_name):
    import dbf
    
    table = dbf.Table(table_name)
    table.open()

    r_arr = []
    for rec in table:
        r_dbf = REESTR()
        r_dbf.initFromDBF(rec)
        r_arr.append(r_dbf)
        
    table.close()
    return r_arr

def put_reestr(db, f_id, r_arr):
    import sys

    s_sqlt = """INSERT INTO
    pr
    (f_id,
    kat, kod_lpu, dn, dk, 
    fam, im, ot, dr, w,
    kpl, vpolis, spolis, npolis, str_pens,
    ds, profil, data_p, spec, kod_i,
    f_doctor, perv_povt, cel_posobr)
    VALUES
    (%s, 
    %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s);"""

    cursor = db.con.cursor()
    
    for pr in r_arr:
        
        if pr.fam is None: 
            fam = u""
        else:
            fam = pr.fam.strip()

        if pr.im is None: 
            im = u""
        else:
            im = pr.im.strip()

        if pr.ot is None: 
            ot = u""
        else:
            ot = pr.ot.strip()

        try:
            cursor.execute(s_sqlt,(f_id, 
                             pr.kat, pr.kod_lpu, pr.dn, pr.dk, 
                             fam, im, ot, pr.dr, pr.w,
                             pr.kpl, pr.vpolis, pr.spolis, pr.npolis, pr.str_pens,
                             pr.ds, pr.profil, pr.data_p, pr.spec, pr.kod_i,
                             pr.f_doctor, pr.perv_povt, pr.cel_posobr,))
        except Exception, e:
            sout = "Can't insert into pr table"
            log.error(sout)
            sout = "{0}".format(e)
            log.error(sout)
            return False
        
        db.con.commit()
        
    
    return True
        