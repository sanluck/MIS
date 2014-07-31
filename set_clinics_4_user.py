#!/usr/bin/python
# -*- coding: utf-8 -*-
# set_clinics_4_user.py - сделать пользователя
#                         пользователем клиник
#

import logging
import sys, codecs

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_set_clinics_4_user.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

HOST      = "fb.ctmed.ru"
DB        = "DBMIS"

USER_ID   = 1007
CLINIC0   = 22

clist     = [222,229]

SQLT1 = "EXECUTE PROCEDURE SP_USER_CHANGE_CLINIC(0,{0});"

SQLT2 = """UPDATE VW_USERS
SET STATUS = 1,
INCLINICBOOL = 1
WHERE
ORG_TYPE_ID=1 and USER_ID = ?;"""

def get_clist(fin = 'clinics.txt'):
    ins = open( fin, "r" )
    array = []
    for line in ins:
        i = line.find("/")
        if i < 0: continue
        s = line[:i]
        try:
            c_id = int(s)
        except:
            continue
        array.append( c_id )
    ins.close()

    return array

if __name__ == "__main__":
    from dbmis_connect2 import DBMIS
    import time

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('-----------------------------------------------------------------------------------')
    log.info('Setting Clinics for User {0}. Start {1}'.format(USER_ID, localtime))

    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    dbc = DBMIS(CLINIC0, mis_host = HOST, mis_db = DB)
    cur = dbc.con.cursor()

    clist = get_clist()

    for c_id in clist:
        sout = "clinic_id: {0}".format(c_id)
        log.info(sout)

        s_sql  = SQLT1.format(c_id)
        cur.execute(s_sql)

        cur.execute(SQLT2,(USER_ID, ))

    dbc.con.commit()
    dbc.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('-----------------------------------------------------------------------------------')
    log.info('Setting Clinics for User {0}. Finish {1}'.format(USER_ID, localtime))

    sys.exit(0)

