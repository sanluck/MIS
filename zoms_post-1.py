#!/usr/bin/env python
# encoding: utf8
# 
# post OMS policy requests
# using policy_requests table in the DBMIS database
#

import logging

URL  = "ims.ctmed.ru"
PORT = 80
PATH = "/zoms/zoms3.php"

SQL_TEMLATE_GET = """SELECT policy_requests_id, people_id_fk, user_id_fk 
   FROM policy_requests
   WHERE transfer_date is Null"""

SQL_TEMPLATE_POST = """UPDATE policy_requests
   SET status = 1
   WHERE policy_requests_id = {0}"""

LOG_FILENAME = '_zoms3.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

def zoms_post(user_id=3037, people_id=1048193):
    # user_id = 72 - Горлов Николай Владимирович
    # people_id = 334 - Корзников Игорь Александрович
    # people_id = 884575 - Тарасов Александр Игоревич
    import httplib, urllib
    params = urllib.urlencode({'user_id': user_id, 'people_id': people_id})
    headers = {"Content-type": "application/x-www-form-urlencoded",
            "Accept": "text/plain"}
    conn = httplib.HTTPConnection("{0}:{1}".format(URL,PORT))
    conn.request("POST", PATH, params, headers)
    response = conn.getresponse()
    data = response.read()
    conn.close()
    return response, data

if __name__=="__main__":
    import sys, time
    from medlib.modules.dbmis_connect import DBMIS
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('ZOMS3 posting started {0}'.format(localtime))
    
    try:
        dbc = DBMIS()
        cursor = dbc.con.cursor()
        ssql = SQL_TEMLATE_GET
        cursor.execute(ssql)
        results = cursor.fetchall()
    except:
        log.error("DBMIS connecting error: {0}".format(sys.exc_value))
        exctype, value = sys.exc_info()[:2]
        log.error( ' {0}'.format(value.message) )
        sys.exit(1)
        
    nposted = 0
    for row in results:
        r_id = row[0]
        r_people_id = row[1]
        r_user_id = row[2]
        sout = "r_id: {0} people_id: {1} user_id: {2}".format( r_id, r_people_id, r_user_id )
        log.debug(sout)
        response, data = zoms_post(user_id=r_user_id, people_id=r_people_id)
        if response.status == 200:
            nposted += 1
            ssql = SQL_TEMPLATE_POST.format(r_id)
            cursor.execute(ssql)
            log.debug("POST status OK")
        else:
            sout = "POST status: {0} reason: {1}".format(response.status, response.reason)
            log.warn(sout)
    
#    response, data = zoms_post()
#    print response.status, response.reason
    log.info('Totally {0} requests have been posted'.format(nposted))
    dbc.con.commit()
    dbc.close()
    sys.exit(0)
    