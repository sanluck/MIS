#!/usr/bin/env python
# encoding: utf8
# 
# look for patients having old medical insurance polices
# among 
# 
# post OMS policy requests
# using policy_requests table in the DBMIS database
#

import logging

URL  = "ims.ctmed.ru"
PORT = 80
PATH = "/zoms/zoms3.php"

USER_ID   = 1800 # 11гб Тропина Наталья Евгеньевна
CLINIC_ID = 220  # 11гб

DBMY_HOST = 'localhost'
DBMY_USER = 'root'
DBMY_PWD  = '18283848'
DBMY_DB   = 'web2py_mis'

SQL_TEMPLATE_GET_TICKETS = """SELECT ticket_id, people_id_fk, user_id_fk, visit_date, visit_status
   FROM vw_tickets
   WHERE user_id_fk = {0} AND visit_date >= '{1}'
"""

SQL_TEMLATE_GET = """SELECT policy_requests_id, people_id_fk, user_id_fk 
   FROM policy_requests
   WHERE transfer_date is Null"""

SQL_TEMPLATE_POST = """UPDATE policy_requests
   SET status = 1
   WHERE policy_requests_id = {0}"""

LOG_FILENAME = '_zoms3-2.out'
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

def get_tickets_list(user_id = USER_ID, clinic_id = CLINIC_ID):
    import datetime
    now = datetime.datetime.now()
    s_now = now.strftime("%Y-%m-%d")
    try:
        dbc = DBMIS(lpu_id = clinic_id)
        cursor = dbc.con.cursor()
        ssql = SQL_TEMPLATE_GET_TICKETS.format(user_id, s_now)
        log.debug(ssql)
        cursor.execute(ssql)
        results = cursor.fetchall()
    except:
        log.error("DBMIS connecting error: {0}".format(sys.exc_value))
        exctype, value = sys.exc_info()[:2]
        log.error( ' {0}'.format(value.message) )
        sys.exit(1)
    
    tlist = []
    for ticket in results:
        ticket_id    = ticket[0]
        people_id_fk = ticket[1]
        user_id_fk   = ticket[2]
        visit_date   = ticket[3]
        visit_status = ticket[4]
        tlist.append([ticket_id, people_id_fk, user_id_fk, visit_date, visit_status])
    log.debug("Totally {0} tickets have been retrieved".format(len(tlist)))
    dbc.close()
    return tlist

if __name__=="__main__":
    import sys, time
    from medlib.modules.dbmis_connect import DBMIS
    from medlib.modules.dbmysql_connect import DBMY
    from PatientInfo import PatientInfo
    import datetime
    now = datetime.datetime.now()
    s_now = now.strftime("%Y-%m-%d")
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('------------------------------------------------------------')
    log.info('ZOMS3-2 posting started {0}'.format(localtime))
    
    tlist = get_tickets_list()

    try:
        dbc = DBMIS()
        cursor = dbc.con.cursor()
    except Exception, err:
        sout = "DBMIS connecting error: {0}".format(str(err))
#        sout = "DBMIS connecting error: {0}".format(sys.exc_value)
        log.error(sout)
        exctype, value = sys.exc_info()[:2]
        log.error( ' {0}'.format(value.message) )
        sys.exit(1)  

    try:
        dbm = DBMY(host=DBMY_HOST, user=DBMY_USER, passwd=DBMY_PWD, db=DBMY_DB)
        dbm_cursor = dbm.con.cursor()
    except Exception, err:
        sout = "DBMY connecting error: {0}".format(str(err))
        log.error(sout)
        sys.exit(1)  


    obj = PatientInfo()

    nold = 0
    nnew = 0
    for ticket in tlist:
        ticket_id    = ticket[0]
        people_id_fk = ticket[1]
        user_id_fk   = ticket[2]
        visit_date   = ticket[3]
        visit_status = ticket[4]
        
        obj.initFromDb(dbc, people_id_fk)
        
        inss = obj.medical_insurance_series
        insn = obj.medical_insurance_number

        print ticket_id, people_id_fk, inss, insn


        if insn == None: insn = ""
        
        if inss == None and len(insn) > 15:
            nnew += 1
            continue
        else:
            nold += 1
        
        
        # check if policy has been requested by Ctrl+F12
        ssql = "SELECT people_id_fk, transfer_date FROM policy_requests WHERE people_id_fk = {0};".format(people_id_fk)
        cursor.execute(ssql)
#        results = cursor.fetchall()
        if cursor.rowcount > 0:
            continue
        

        # check if policy has been requested by this programm
        ssql = "SELECT people_id, date_sent FROM zoms WHERE people_id = {0};".format(people_id_fk)
        dbm_cursor.execute(ssql)
#        results = cursor.fetchall()
        if dbm_cursor.rowcount > 0:
            continue
        
        ssql = "INSERT INTO zoms (user_id, people_id, date_sent) VALUES ({0}, {1}, '{2}');".format(USER_ID, people_id_fk, s_now)
        dbm_cursor.execute(ssql)
        ssql = "COMMIT;"
        dbm_cursor.execute(ssql)
        
    
    sout = "User_id: {0} New tickets: {1} Old policies: {2} New policies: {3}".format(USER_ID, len(tlist), nold, nnew)
    log.info(sout)
    
        
    dbc.close()
    dbm.close()
    sys.exit(0)
    