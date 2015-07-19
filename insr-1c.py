#!/usr/bin/python
# -*- coding: utf-8 -*-
# insr-1c.py - запрос страховой пренадлежности
#              Insurance Belongins Request (IBR)
#              по всей базе DBMIS (по всем ЛПУ)
#              вывод результатов из таблицы mis.im
#              результаты получены в insr-1b.py
#

import os, sys, codecs
import logging
import ConfigParser
import time

from dbmysql_connect import DBMY

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_insr1c.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

Config = ConfigParser.ConfigParser()
#PATH = os.path.dirname(sys.argv[0])
PATH = os.getcwd()
FINI = PATH + "/" + "insr.ini"

from ConfigSection import ConfigSectionMap
# read INI data
Config.read(FINI)

# [Insr]
Config2 = ConfigSectionMap(Config, "Insr")
D_START = Config2['d_start']
D_FINISH = Config2['d_finish']
USE_DATERANGE = Config2['use_daterange']
#FNAME = "IM22M220000_1507{0}.csv"
FNAME = Config2['fnamec']
STEP = int(Config2['stepc'])

FPATH = "./IM"

SQLT1 = """SELECT
people_id, lname, fname, mname, bd, sex,
doc_type, doc_ser, doc_number, snils,
insorg_ogrn, insorg_okato,
enp, tdpfs, oms_ser, oms_number, 
mc_start, mc_end, mo_ogrn, hc_cost 
FROM im;"""

if __name__ == "__main__":
    from people import IM_PEOPLE

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('-----------------------------------------------------------------------------------')
    log.info('Generate Insurance Belongings Request Start {0}'.format(localtime))

    dbmy = DBMY()
    curm = dbmy.con.cursor()
    curm.execute(SQLT1)
    results = curm.fetchall()
    
    iii = 0
    npack = 1
    fname = FPATH + "/" + FNAME.format(npack)
    sout = "Output to file: {0}".format(fname)
    log.info(sout)
    fo = open(fname, "wb")
    
    for rec in results:
        im_people = IM_PEOPLE()
        im_people.init2(rec)
        sss = im_people.p1() + "|\n"
        ps = sss.encode('windows-1251')
        fo.write(ps)
        iii += 1
        
        if iii % STEP == 0:
            fo.flush()
            os.fsync(fo.fileno())
            fo.close()
            
            npack += 1
            fname = FPATH + "/" + FNAME.format(npack)
            sout = "Output to file: {0}".format(fname)
            log.info(sout)
            fo = open(fname, "wb")
            
            
    fo.flush()
    os.fsync(fo.fileno())
    fo.close()
    dbmy.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Generate Insurance Belongings Request Finish {0}'.format(localtime))
        
    sys.exit(0)
