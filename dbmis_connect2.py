#!/usr/bin/python
#encoding: utf-8
#
# dbmis_connect2.py 
# 

import sys
import time
import exceptions
import logging
import fdb

HOST = 'fb.ctmed.ru'
DB = 'DBMIS'
DB_USER = 'sysdba'
DB_PWD = 'Gjkbrkbybrf'

#DB_USER = 'clinic'
#DB_PWD = 'Polyclinic'
DB_ROLE = 'supervisor'

#CUR_VGID = u'З'

#MIS_USER = 1007
#MIS_USER_PWD = '8622A315FF489C84DA4E37C0CFABD2EE' # 951123
MIS_USER = 0
MIS_USER_PWD = 'C499A157F23622C2A63362CE0EE882A' # 753159

SET_CONTEXT_SQL_TEMPLATE = """SELECT rdb$set_context('USER_SESSION', 'USER_ID', %d) AS C1, rdb$set_context('USER_SESSION', 'PASSWD', '%s') AS C2, rdb$set_context('USER_SESSION', 'CURRENT_ORG_1', %d) AS C3, current_user, current_role FROM rdb$database"""

SQL_TEMPLATE_PEOPLE = """SELECT *
FROM VW_PEOPLES WHERE
  PEOPLE_ID = {0}
"""

SQL_TEMPLATE_WORKERS = """SELECT
  WORKER_ID, SPECIALITY_ID_FK, SPECIALITY_NAME, DOCTOR_FIO
FROM
  VW_WORKERS WHERE SPECIALITY_ID_FK = {0}
"""

SQL_TEMPLATE_WORKER = """SELECT
  WORKER_ID, SPECIALITY_ID_FK, SPECIALITY_NAME, DOCTOR_FIO, PAYMENT_TYPE_ID_FK, ROOM
FROM VW_WORKERS WHERE
  WORKER_ID = {0}
"""

SQL_TEMPLATE_WORKTIME = """SELECT 
   work_date, begin_time, end_time, step, visibility_groups_ids
FROM work_time WHERE
   WORKER_ID_FK={0} AND
   work_date BETWEEN '{1}-{2}-{3}' AND '{4}-{5}-{6}'
ORDER by work_date, begin_time"""

# Cписок ЛПУ, которые видит пользователь
SQL_TEMPLATE_CLINICS_USER = """SELECT
  cu.USER_ID,
  cu.CLINIC_ID,
  c.CLINIC_NAME
FROM
  CLINIC_USERS cu
LEFT JOIN VW_CLINICS c ON c.CLINIC_ID = cu.CLINIC_ID
WHERE
   cu.USER_ID = {0}
ORDER BY cu.CLINIC_ID"""

SQL_TEMPLATE_CLINICS = """SELECT
*
FROM
  VW_CLINICS"""

SQL_TEMPLATE_TICKETS = """SELECT
*
FROM
  VW_TICKETS 
WHERE
  WORKER_ID_FK = {0} AND VISIT_DATE = '{1}' AND VISIT_TIME = '{2}'
"""

SQL_TEMPLATE_TICKET_UPD = """UPDATE VW_TICKETS
SET PEOPLE_ID_FK = {0}
WHERE TICKET_ID = {1};
"""

SQL_TEMPLATE_GETPEOPLE = """SELECT 
LNAME, FNAME, MNAME, INSURANCE_CERTIFICATE, PEOPLE_ID, BIRTHDAY 
FROM VW_PEOPLES 
WHERE 
UPPER(LNAME) = '{0}' 
AND UPPER(FNAME)= '{1}' 
AND UPPER(MNAME)= '{2}' 
AND INSURANCE_CERTIFICATE = '{3}';"""

SQL_TEMPLATE_GETCLINICS = """SELECT
*
FROM VW_CLINICS
"""

SQL_TEMPLATE_GETCLINIC = """SELECT
clinic_name, inn, kpp, ogrn, mcod
FROM VW_CLINICS
WHERE CLINIC_ID = {0}
"""

# Вборка результата обследования, диагноза и заключения
# для определенного conclusion_id
SQL_TEMPLATE = "select c.conclusion_id, cast (c.results as BLOB SUB_TYPE TEXT ), cast (c.diagnosis as BLOB SUB_TYPE TEXT ), cast (c.advices as BLOB SUB_TYPE TEXT ) from conclusions c where c.conclusion_id = %d;"

log = logging.getLogger(__name__)

def unpack_date(sdate = "2012-10-01"):
    year = sdate[:4]
    month = sdate[5:7]
    day = sdate[8:10]
    return int(year), int(month), int(day)

class DBMIS:
    'Base class for DBMIS connection'
    
    def __init__(self, clinic_id = 22, mis_user = MIS_USER, mis_user_pwd = MIS_USER_PWD):
        s_dsn = "%s:%s" % (HOST, DB)
        try:
            self.con = fdb.connect(dsn=s_dsn, user=DB_USER, password=DB_PWD, role = DB_ROLE, charset='WIN1251')
            self.cur = self.con.cursor()
        except:
            exctype, value = sys.exc_info()[:2]
            log.warn( 'DBMIS init error: %s' % value.message )
            raise
        sout = "Connection to %s has been established" % (s_dsn)
        log.debug(sout)
        self.clinic_id = clinic_id
        self.set_context(clinic_id, mis_user, mis_user_pwd)
        self.get_clinic_info()
                
        
    def set_context(self, lpu_id = 22, mis_user = MIS_USER, mis_user_pwd = MIS_USER_PWD):

        s0 = SET_CONTEXT_SQL_TEMPLATE % (mis_user, mis_user_pwd, lpu_id)
        log.debug(s0)
        try:
            self.cur.execute(s0)
            self.session = self.cur.fetchone()
        except:
            exctype, value = sys.exc_info()[:2]
            log.warn( 'DBMIS set_context error: %s' % value.message )
            self.con.close()
            raise
        sout = "Context for user %d, lpu %d has been set" % (MIS_USER, lpu_id)
        log.debug(sout)
        self.lpu_id = lpu_id
            
    def execute(self, ssql):
        try:
            self.cur.execute(ssql)
        except:
            exctype, value = sys.exc_info()[:2]
            log.warn( 'DBMIS execute error: %s' % value.message )
            self.con.close()
            raise
    
    def close(self):
        self.con.close()

    def get_people(self, people_id):
    # Get single person data from peoples table
        ssql = SQL_TEMPLATE_PEOPLE.format(people_id)
        self.execute(ssql)
        result = self.cur.fetchone()
        return result


    def get_workers(self, speciality_id = 1):
    # Get workers list for current LPU_ID, 
    #    self.set_context(lpu_id)
        lpu_id = self.lpu_id
        ssql = SQL_TEMPLATE_WORKERS.format(speciality_id)
        wlist = []
        log.debug("Getting workers list for lpu %d, speciality %d" % (lpu_id, speciality_id))
        log.debug(ssql)
        self.execute(ssql)
        for result in self.cur:
            wlist.append([result[0], result[3]])
            sout = "%d : %s" % (result[0], result[3])
            log.debug(sout)
        return wlist
    
    def get_worker(self, worker_id):
    # Get single worker parameters using current settings, made by setcontext
    # that means the worker is searched among current LPU workers
        lpu_id = self.lpu_id
        ssql = SQL_TEMPLATE_WORKER.format(worker_id)
        worker = []
        log.debug("Getting worker params for lpu %d, worker %d" % (lpu_id, worker_id))
        log.debug(ssql)
        self.execute(ssql)
        for result in self.cur:
            worker_id = result[0]
            speciality_id = result[1]
            speciality_name = result[2]
            doctor_FIO = result[3]
            payment_type_id = result[4]
            room = result[5]
            log.debug("       worker_id: {0}".format(worker_id))
            log.debug("   speciality_id: {0}".format(speciality_id))
            log.debug(" speciality_name: {0}".format(speciality_name.encode('utf-8')))
            if doctor_FIO == None:
                doctor_FIO = ""
            log.debug("      doctor_FIO: {0}".format(doctor_FIO.encode('utf-8')))
            log.debug(" payment_type_id: {0}".format(payment_type_id))
            if room == None:
                room = ""
            log.debug("            room: {0}".format(room.encode('utf-8')))
            worker.append([result[0], # worker ID 
                           result[1], # speciality ID
                           result[2], # speciality name
                           result[3], # doctor FIO
                           result[4], # payment type ID
                           room  # room
                           ])
            
        return worker

    def get_schedule(self, worker_id, schedule_dates = ["2012-10-01","2012-10-07"], CUR_VGID = u'З'):
    # Get schedule for WORKER_ID and particular time period
    # CUR_VGID - Current Visibility Group ID
        year_start, month_start, day_start = unpack_date(schedule_dates[0])
        year_end, month_end, day_end = unpack_date(schedule_dates[1])
        ssql = SQL_TEMPLATE_WORKTIME.format(worker_id, year_start, month_start, day_start,year_end, month_end, day_end)
        self.execute(ssql)
        schedule = {}
        step = -1
        for result in self.cur:
            if step == -1:
                step = result[3]
            vgroups = result[4]
            if vgroups != None:
                if vgroups.count(CUR_VGID) > 0:
                    d = result[0].isoweekday()
                    if schedule.get(d, None) == None:
                        schedule[d] = []
                    schedule[d].append([result[1], result[2]])
        return schedule, step   
    
    def get_worker_tickets(self, worker_id, visit_date, visit_time):
    # Get tickets for particular worker
        ssql = SQL_TEMPLATE_TICKETS.format(worker_id, visit_date, visit_time)
        self.execute(ssql)

    def update_ticket(self, ticket_id, people_id):
    # Update people_id for ticket with particular ticket_id 
        # Prepare SQL query to UPDATE required records
        ssql = SQL_TEMPLATE_TICKET_UPD.format(people_id, ticket_id)
        # prepare a cursor object using cursor() method
        cursor = self.con.cursor()
        try:
            # Execute the SQL command
            cursor.execute(ssql)
            # Commit your changes in the database
            self.con.commit()
            log.debug("people_id for ticket {0} was set to {1}".format(ticket_id, people_id))
            return True
        except:
            exctype, value = sys.exc_info()[:2]
            # Rollback in case there is any error
            self.con.rollback()
            log.warn("can not set people_id for ticket {0}".format(ticket_id))
            log.warn( 'DBMIS execute error: %s' % value.message )
            return False

    def get_clinic_info(self):
    # Get clinics name, inn, kpp, ogrn, mcod for particular clinic
        clinic_id = self.clinic_id
        ssql = SQL_TEMPLATE_GETCLINIC.format(clinic_id)
        self.execute(ssql)
        rec = self.cur.fetchone()
        if rec == None:
            self.name = None
            self.inn  = None
            self.kpp  = None
            self.ogrn = None
            self.mcod = None
        else:
            self.name = rec[0]
            self.inn  = rec[1]
            self.kpp  = rec[2]
            self.ogrn = rec[3]
            self.mcod = rec[4]
            
        
        

if __name__ == "__main__":
    dbc = DBMIS(22)
    print dbc.name, dbc.mcod, dbc.ogrn
    cursor = dbc.con.cursor()
    ssql = SQL_TEMPLATE_GETCLINICS
    print ssql
    cursor.execute(ssql)
    results = cursor.fetchall()
    print cursor.rowcount
    for rec in results:
        print rec[0], rec[1]
    
    l_f = 'ЗИБРОВА'
    l_i = 'ОЛЬГА'
    l_o = 'ГРИГОРЬЕВНА'
    l_f1251 = l_f.decode('utf-8').encode('cp1251')
    l_i1251 = l_i.decode('utf-8').encode('cp1251')
    l_o1251 = l_o.decode('utf-8').encode('cp1251')
    l_snils = '085-702-031 55'
    ssql = SQL_TEMPLATE_GETPEOPLE.format(l_f1251, l_i1251, l_o1251, l_snils)
    print ssql
    cursor.execute(ssql)
    rec = cursor.fetchone()
    print cursor.rowcount
#    print rec
    f = rec[0].encode('utf-8')
    i = rec[1].encode('utf-8')
    o = rec[2].encode('utf-8')
    sout = "F I O S: {0} {1} {2} {3}; PEOPLE_ID: {4}; BIRTHDAY: {5}".format(f, i, o, rec[3], rec[4], rec[5])
    print sout
    
    ssql = "SELECT * FROM VW_PEOPLES WHERE PEOPLE_ID = 205"
    cursor.execute(ssql)
    r = cursor.fetchone()
    print cursor.rowcount
    print r[0], r[1]
    
