#!/usr/bin/python
# -*- coding: utf-8 -*-
# r_analysis-1.py - анализ реестров
#                   посещения <-> обращения
#

import logging
import sys, codecs
from datetime import datetime

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_ranalysis.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

CLINIC_ID  = 106
DBF_DIR    = "/home/gnv/MIS/import/{0}J/".format(CLINIC_ID)
TABLE_NAME = DBF_DIR + "REGISTRY.DBF"

STEP = 100

s_sqlt1 = """SELECT *
FROM area_peoples
WHERE people_id_fk = ?"""

ap_d = {}
ap_d[106] = [3248, '2014-01-10']
ap_d[132] = [3245, '2014-01-10']
ap_d[117] = [3243, '2014-01-10']

AREA_ID  = ap_d[CLINIC_ID][0]
DATE_BEG = datetime.strptime(ap_d[CLINIC_ID][1], '%Y-%m-%d')

s_sqlt2 = """INSERT INTO
area_peoples
(people_id_fk, area_id_fk, date_beg)
VALUES
(?, ?, ?);"""

if __name__ == "__main__":
    import time
    import datetime
    from dbmis_connect2 import DBMIS
    from people import PEOPLE, get_registry, get_people