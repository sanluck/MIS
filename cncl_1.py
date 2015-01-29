#!/usr/bin/python
# -*- coding: utf-8 -*-
# cncl_1.py - показать заключение для указанноего талона
#

import logging
import sys, codecs

from dbmysql_connect import DBMY

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_cncl_1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST = "fb2.ctmed.ru"
DB   = "DBMIS"
CLINIC_ID = 22

TICKET_ID = 99897361

SQLT_CNCL = """SELECT t.ticket_id, t.clinic_id_fk, c.ms_uid
FROM tickets t
LEFT JOIN conclusions c ON t.ticket_id = c.ticket_id_fk
WHERE t.ticket_id = ?;"""

def get_uid(ticket_id):
    import fdb
    from dbmis_connect2 import DBMIS

    dbc = DBMIS(clinic_id = CLINIC_ID, mis_host = HOST, mis_db = DB)
    cur = dbc.con.cursor()

    dbc.con.begin(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)

    cur.execute(SQLT_CNCL, (ticket_id, ))
    rec = cur.fetchone()
    dbc.con.commit()
    if rec is None: return None, None
    ms_uid = rec[2]
    c_id = rec[1]


    dbc.close()

    return ms_uid, c_id

if __name__ == "__main__":
    import time
    from medlib.modules.medobjects.Tickets import Ticket, get_from_mongo
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('----------------------------------------------------------------------------------')
    log.info('Get Conclusion. Start {0}'.format(localtime))

    ticket_id = TICKET_ID
    ms_uid, c_id = get_uid(ticket_id)

    sout = "ticket_id: {0} ms_uid: {1} clinic_id: {2}".format(ticket_id, ms_uid, c_id)
    if ms_uid is not None:
        cncl = get_from_mongo(c_id, ms_uid)
        s1 = cncl[0]
        s2 = cncl[1]
        s3 = cncl[2]

        sout = "s1: {0}".format(s1.encode("utf-8"))
        log.info(sout)

        sout = "s2: {0}".format(s2.encode("utf-8"))
        log.info(sout)

        sout = "s3: {0}".format(s3.encode("utf-8"))
        log.info(sout)

        sout = "len(s1): {0} len(s2): {1} len(s3): {2}".format(len(s1), len(s2), len(s3))
        log.info(sout)
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Get Conclusion. Finish  '+localtime)

    sys.exit(0)
