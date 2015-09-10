#!/usr/bin/python
# -*- coding: utf-8 -*-
# d-list2.py - поиск записей в таблице peoples
#              для пациентов из таблицы mis.md$list
#              с последующим заданием peope_id:
#                -1 - не найден
#
#

import logging
import sys, codecs
from datetime import datetime

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_dlist2.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

STEP = 1000

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"
CLINIC_ID = 22

SQLT1 = """SELECT id, md_id, lname, fname, mname, birth_dt
FROM md$list
WHERE people_id is Null;"""

SQLT2 = """UPDATE md$list
SET
people_id = %s
WHERE id = %s;"""

SQLT3 = """UPDATE md$list
SET
people_id = %s,
c_check_id = %s,
d_end_1 = %s
WHERE id = %s;"""

SQLF1 = """SELECT clinical_checkup_id, date_end_1
FROM clinical_checkups
WHERE
people_id_fk = ?
ORDER BY date_end_1 DESC;"""

if __name__ == "__main__":
    import time
    import datetime
    from dbmis_connect2 import DBMIS
    from dbmysql_connect import DBMY
    from people import PEOPLE, get_people

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('--------------------------------------------------------------------')
    log.info('Search people_id for d$list cases. Start {0}'.format(localtime))


    dbmy = DBMY()
    curr = dbmy.con.cursor()
    curu = dbmy.con.cursor()

    p_arr = []
    curr.execute(SQLT1)
    recs = curr.fetchall()
    for rec in curr:
        _id   = rec[0]
        md_id = rec[1]
        lname = rec[2]
        fname = rec[3]
        mname = rec[4]
        birth = rec[5]
        p_arr.append([_id, md_id,  lname, fname, mname, birth])

    nnn = len(p_arr)
    sout = "Totally {0} cases to be processed".format(nnn)
    log.info(sout)


    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    clinic_id = CLINIC_ID
    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)
    cursor = dbc.con.cursor()


    counta  = 0
    countn  = 0

    for p_md in p_arr:

        counta += 1

        _id      = p_md[0]
        md_id    = p_md[1]
        u_lname  = p_md[2]
        u_fname  = p_md[3]
        u_mname  = p_md[4]
        birthday = p_md[5]

        if birthday is None: continue

        lname = u_lname.encode('utf-8')
        fname = u_fname.encode('utf-8')
        if u_mname is None:
            mname = ""
        else:
            mname = u_mname.encode('utf-8')

        if counta % STEP == 0:
            sout = "{0} {1} {2} {3}".format(counta, lname, fname, mname)
            log.info( sout )

        pf_arr = get_people(cursor, u_lname, u_fname, u_mname, birthday)
        if (pf_arr is None) or (len(pf_arr) == 0):
            curu.execute(SQLT2, (-1, _id, ))
            dbmy.con.commit()
            countn += 1
        else:
            c_found = False
            for pf in pf_arr:
                people_id = pf[0]
                cursor.execute(SQLF1, (people_id, ))
                rec = cursor.fetchone()
                if rec is None:
                    c_id = "-"
                    c_d  = "-"
                else:
                    c_found = True
                    c_id = rec[0]
                    c_d  = rec[1]
                    curu.execute(SQLT3, (people_id, c_id, c_d, _id, ))
                    dbmy.con.commit()

                if  len(pf_arr) > 1:
                    sout = "id: {0} md_id: {1} people_id: {2} clinical_checkup_id: {3} date_end_1: {4}".format(_id, md_id, people_id, c_id, c_d)
                    log.warn(sout)

            if not c_found:
                pf = pf_arr[0]
                people_id = pf[0]
                curu.execute(SQLT2, (people_id, _id, ))
                dbmy.con.commit()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Search people_id for d$list cases. Finish  '+localtime)
    sout = "Totally {0} cases of {1} have not been identified".format(countn, counta)
    log.info( sout )

    dbc.close()
    dbmy.close()
    sys.exit(0)