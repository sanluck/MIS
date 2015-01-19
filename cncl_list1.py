#!/usr/bin/python
# -*- coding: utf-8 -*-
# cnc_list1.py - заполнение списка ЛПУ (cncl_clinics) в базе web2py_mis
#                для расчета количества заполненных заключений
#                составляется список ЛПУ у которых есть талоны
#

import logging
import sys, codecs

from dbmysql_connect import DBMY

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_cncl_list1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

SQLT_CLIST = """SELECT clinic_id, mcod, clinic_name
FROM clinics
ORDER BY clinic_id;"""

SQLT_T_COUNT = """SELECT count(t.clinic_id_fk)
FROM tickets t
WHERE t.clinic_id_fk = ?;"""

MY_DB = "web2py_mis"
MY_DBHOST = "127.0.0.1"

SQLT_ILIST = """INSERT INTO cncl_clinics
(clinic_id, mcod, name, t_count)
VALUES
(%s, %s, %s, %s);"""

SQLT_FLIST = """SELECT id
FROM cncl_clinics
WHERE clinic_id = %s;"""

SQLT_ULIST = """UPDATE cncl_clinics
SET
clinic_id = %s,
mcod = %s,
name = %s,
t_count = %s
WHERE id = %s;"""

UPDATE = False

def get_cncl_list():
    import fdb
    from dbmis_connect2 import DBMIS

    sout = "Getting Clinics List from:"
    log.info(sout)

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    dbc = DBMIS(mis_host = HOST, mis_db = DB)
    cur = dbc.con.cursor()

    dbc.con.begin(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)

    cur.execute(SQLT_CLIST)
    results = cur.fetchall()
    dbc.con.commit()

    c_arr = []

    for rec in results:
	clinic_id = rec[0]
	mcod = rec[1]
	clinic_name = rec[2]
	c_arr.append([clinic_id, mcod, clinic_name])


    sout = "DBMIS clinic's list has got {0} clinics".format(len(c_arr))
    log.info(sout)

    cncl_arr = []
    for clinic in c_arr:
	clinic_id = clinic[0]
	mcod = clinic[1]
	clinic_name = clinic[2]

	dbc.con.begin(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
	cur.execute(SQLT_T_COUNT, (clinic_id, ))
	rec = cur.fetchone()
	dbc.con.commit()
	if rec is None: continue
	t_count = rec[0]
	if t_count == 0: continue
	sout = "{0}/{1} {2} : {3}".format(clinic_id, mcod, clinic_name.encode("utf-8"), t_count)
	log.info(sout)
	cncl_arr.append([clinic_id, mcod, clinic_name, t_count])

    dbc.close()

    sout = "===================================================================================="
    log.info(sout)

    sout = "TOTALLY {0} clinics have got tickets".format(len(cncl_arr))
    log.info(sout)


    return cncl_arr


if __name__ == "__main__":
    import time
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('----------------------------------------------------------------------------------')
    log.info('Create Clinics List for Conclusions Count Report. Start {0}'.format(localtime))

    cncl_list = get_cncl_list()

    dbmy = DBMY(host = MY_DBHOST, db = MY_DB)
    curr = dbmy.con.cursor()
    curm = dbmy.con.cursor()

    if UPDATE:
	for clinic in cncl_list:
	    c_id = clinic[0]
	    mcod = clinic[1]
	    name = clinic[2]
	    cnt  = clinic[3]

	    curr.execute(SQLT_FLIST,(c_id,))
	    rec = curr.fetchone()
	    if rec is None:
		curm.execute(SQLT_ILIST,(c_id, mcod, name, cnt, ))
	    else:
		_id = rec[0]
		curm.execute(SQLT_ULIST,(c_id, mcod, name, cnt, _id, ))
    else:
	ssql = "TRUNCATE TABLE cncl_clinics;"
	curm.execute(ssql)
	dbmy.con.commit()

	for clinic in cncl_list:
	    c_id = clinic[0]
	    mcod = clinic[1]
	    name = clinic[2]
	    cnt  = clinic[3]
	    curm.execute(SQLT_ILIST,(c_id, mcod, name, cnt, ))

    dbmy.con.commit()
    dbmy.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Create Clinics List for Conclusions Count Report. Finish  '+localtime)

    sys.exit(0)
