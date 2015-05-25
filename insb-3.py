#!/usr/bin/python
# -*- coding: utf-8 -*-
# insb-3.py - обработка файлов SD*.xls (пациенты с множественным прикреплением)
#             формирование файлов MO
#
# INPUT DIRECTORY SD2DO
#

import os
import sys, codecs
import ConfigParser

from medlib.moinfolist import MoInfoList
modb = MoInfoList()
from insorglist import InsorgInfoList
insorgs = InsorgInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

import logging

LOG_FILENAME = '_insb3.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

Config = ConfigParser.ConfigParser()
#PATH = os.path.dirname(sys.argv[0])
#PATH = os.path.realpath(__file__)
PATH = os.getcwd()
FINI = PATH + "/" + "insr.ini"

log.info("INI File: {0}".format(FINI))

from ConfigSection import ConfigSectionMap
# read INI data
Config.read(FINI)
# [DBMIS]
Config1 = ConfigSectionMap(Config, "DBMIS")
HOST = Config1['host']
DB = Config1['db']

# [Insb]
Config2 = ConfigSectionMap(Config, "Insb")
ADATE_ATT = Config2['adate_att']
SET_ADATE = Config2['set_adate']
if SET_ADATE == "1":
    ASSIGN_ATT = True
else:
    ASSIGN_ATT = False

STEP = 100
PRINT_FOUND = False

SD2DO_PATH        = "./SD2DO"
SDDONE_PATH       = "./SDDONE"
SDMO_PATH         = "./SDMO"

CHECK_REGISTERED  = False
REGISTER_FILE     = False
MOVE_FILE         = True

FNAMEb = "MO2{0}{1}.csv" # в ТФОМС на внесение изменений

OCATO  = '01000'

def get_fnames(path = SD2DO_PATH, file_ext = '.xls'):

    import os

    fnames = []
    for subdir, dirs, files in os.walk(path):
        for fname in files:
            if fname.find(file_ext) > 1:
                log.info(fname)
                fnames.append(fname)

    return fnames

def get_sd(fname):
    import xlrd

    array = []
    p_id  = 0

    workbook = xlrd.open_workbook(fname)
    worksheets = workbook.sheet_names()
    worksheet_name = worksheets[0]
    worksheet = workbook.sheet_by_name(worksheet_name)
    num_rows = worksheet.nrows - 1
    num_cells = worksheet.ncols - 1
    curr_row = -1
    while curr_row < num_rows:
            curr_row += 1

            # Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
            c0_type = worksheet.cell_type(curr_row, 0)
            if c0_type <> 2: continue

            people_id = int(worksheet.cell_value(curr_row, 0))
            date_beg  = worksheet.cell_value(curr_row, 1)
            if worksheet.cell_type(curr_row, 2) == 2:
                motive    = int(worksheet.cell_value(curr_row, 2))
            else:
                motive = None
            clinic_id = int(worksheet.cell_value(curr_row, 3))

            if people_id <> p_id:
                if p_id <>0:
                    array.append([p_id, p_arr])
                p_arr = []
                p_id  = people_id

            p_arr.append([date_beg, motive, clinic_id])

    return array

def write_mo(clinic_id, mcod, ar):
    import os
    import time
    from datetime import datetime
    from insorglist import InsorgInfoList
    from dbmysql_connect import DBMY
    from dbmis_connect2 import DBMIS
    from PatientInfo import PatientInfo, p1, p2

    insorgs = InsorgInfoList()

    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
    cur = dbc.con.cursor()
    if dbc.ogrn == None:
        CLINIC_OGRN = u""
    else:
        CLINIC_OGRN = dbc.ogrn

    cogrn = CLINIC_OGRN.encode('utf-8')
    cname = dbc.name.encode('utf-8')

    sout = "clinic_id: {0} cod_mo: {1} clinic_name: {2} clinic_ogrn: {3}".format(clinic_id, mcod, cname, cogrn)
    log.info(sout)


    stime   = time.strftime("%Y%m%d")
    fnameb  = FNAMEb.format(mcod, stime)
    f_fname = SDMO_PATH + "/" + fnameb

    fob = open(f_fname, "wb")

    sout = "Output to file: {0}".format(f_fname)
    log.info(sout)

    s_sqlt = """select first 1
t.ticket_id, t.clinic_id_fk, t.visit_date, t.worker_id_fk,
w.speciality_id_fk
from tickets t
left join workers w on t.worker_id_fk = w.worker_id
where t.people_id_fk = ?
and t.visit_date >= '2013-01-01'
and w.speciality_id_fk in (1,7,38)
order by t.ticket_id desc;"""


    dbmy = DBMY()
    curm = dbmy.con.cursor()

    s_sqlsm = """select ocato, smo_code, enp, mcod
    from sm
    where people_id = %s;"""

    count_a  = 0
    count_p  = 0

    noioc    = 0

    for rec in ar:
        count_a += 1

        people_id  = rec[0]
        p_arr      = rec[1]
        c_count = len(p_arr)

        if count_a % STEP == 0:
            sout = " {0} people_id: {1} c_count: {2}".format(count_a, people_id, c_count)
            log.info(sout)


        if c_count == 1: continue

        dates   = []
        motives = []
        clinics = []

        count_1  = 0
        count_99 = 0
        count_   = 0
        lclinic = False

        date_m   = p_arr[0][0]
        motive_m = p_arr[0][1]
        c_id_m   = p_arr[0][2]
        for pp in p_arr:
            date_b = pp[0]
            motive = pp[1]
            c_id   = pp[2]

            if date_b > date_m:
                date_m   = date_b
                c_id_m   = c_id
                motive_m = motive

            dates.append(date_b)
            motives.append(motive)
            clinics.append(c_id)

            if c_id == clinic_id:
                lclinic = True
                date_beg = date_b
                cmotive = motive

            if motive == 1:
                count_1 += 1
            elif motive == 99:
                count_99 += 1
            elif motive not in (1, 2, 3, 99):
                count_ += 1

        if (2 in motives) or (3 in motives): continue # was taken into account by insb-2

        if not lclinic: continue

        cur.execute(s_sqlt, (people_id, ))
        rec_t = cur.fetchone()

        p_obj = PatientInfo()
        p_obj.initFromDb(dbc, people_id)

        if p_obj.people_id is None:
            sout = "Can not find people_id = {0}".format(people_id)
            log.warn( sout )
            continue

        curm.execute(s_sqlsm, (people_id,))
        rec_m = curm.fetchone()
        ocato = None
        if rec_m is not None:
            p_obj.enp = rec_m[2]
            ocato     = rec_m[0]
            if ocato <> OCATO: continue

        insorg_id   = p_obj.insorg_id
        try:
            insorg = insorgs[insorg_id]
        except:
            sout = "People_id: {0}. Have not got insorg_id: {1}".format(people_id, insorg_id)
            log.debug(sout)
            insorg = insorgs[0]
            noioc += 1

        l_print = False
        if (rec_t is None):
            if (c_id_m == clinic_id):
                l_print = True
        else:
            c_id_t = rec_t[1]
            if c_id_t == clinic_id: l_print = True

        if l_print:

            try:
                dd_beg = datetime.strptime(date_beg, '%Y-%m-%d')
            except:
                dd_beg = None
            sss = p2(p_obj, mcod, cmotive, dd_beg, ADATE_ATT, ASSIGN_ATT) + "\r\n"
            ps = sss.encode('windows-1251')
            if l_print:
                fob.write(ps)

                fob.flush()
                os.fsync(fob.fileno())

                count_p += 1

    dbc.close()
    dbmy.close()
    fob.close()

    sout = "Totally {0} of {1} patients have been written into file".format(count_p, count_a)
    log.info( sout )

    return count_a, count_p

def register_sd_done(db, mcod, clinic_id, fname):
    import datetime

    dnow = datetime.datetime.now()
    sdnow = str(dnow)
    s_sqlt = """INSERT INTO
    sd_done
    (mcod, clinic_id, fname, done)
    VALUES
    ({0}, {1}, '{2}', '{3}');
    """

    s_sql = s_sqlt.format(mcod, clinic_id, fname, sdnow)
    cursor = db.con.cursor()
    cursor.execute(s_sql)
    db.con.commit()

def sd_done(db, mcod, w_month = '1402'):

    s_sqlt = """SELECT
    fname, done
    FROM
    sd_done
    WHERE mcod = {0} AND fname LIKE '%{1}%';
    """

    s_sql = s_sqlt.format(mcod, w_month)
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
    import time
    from dbmysql_connect import DBMY

    log.info("======================= INSB-3 ===========================================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('SD Files Processing. Start {0}'.format(localtime))


    fnames = get_fnames()
    n_fnames = len(fnames)
    sout = "Totally {0} files has been found".format(n_fnames)
    log.info( sout )

    dbmy2 = DBMY()

    for fname in fnames:
        s_mcod  = fname[2:8]
        w_month = fname[12:16]
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

        f_fname = SD2DO_PATH + "/" + fname
        sout = "Input file: {0}".format(f_fname)
        log.info(sout)

        if CHECK_REGISTERED:
            ldone, dfname, ddone = sd_done(dbmy2, mcod, w_month)
        else:
            ldone = False

        if ldone:
            sout = "On {0} hase been done. Fname: {1}".format(ddone, dfname)
            log.warn( sout )
        else:
            #pfile(f_fname)
            ar = get_sd(f_fname)
            l_ar = len(ar)
            sout = "File has got {0} patients".format(l_ar)
            log.info( sout )



            count_a, count_p = write_mo(clinic_id, mcod, ar)
            #sout = "Totally {0} lines of {1} have been inserted, {2} - updated".format(count_i, count_a, count_u)
            #log.info( sout )
            if REGISTER_FILE: register_sd_done(dbmy2, mcod, clinic_id, fname)

        if MOVE_FILE:
        # move file
            source = SD2DO_PATH + "/" + fname
            destination = SDDONE_PATH + "/" + fname
            shutil.move(source, destination)

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('SD Files Processing. Finish  '+localtime)

    dbmy2.close()
    sys.exit(0)
