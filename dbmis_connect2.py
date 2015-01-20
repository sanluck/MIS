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
from get_lan_ip import get_lan_ip


HOST = 'fb2.ctmed.ru'
#DB = 'DBMIS'
DB = 'DVN5'
DB_USER = 'sysdba'
DB_PWD = 'Fbdbmis1'

#DB_USER = 'clinic'
#DB_PWD = 'Polyclinic'
DB_ROLE = 'supervisor'

#CUR_VGID = u'З'

#MIS_USER = 1007
#MIS_USER_PWD = '8622A315FF489C84DA4E37C0CFABD2EE' # 951123
MIS_USER = 0
#MIS_USER_PWD = 'C499A157F23622C2A63362CE0EE882A' # 753159
MIS_USER_PWD = '885A4C90411054B6AAA3769BDC19DCBC' # k1a5ter

SET_CONTEXT_SQL_TEMPLATE = """SELECT rdb$set_context('USER_SESSION', 'USER_ID', %d) AS C1, rdb$set_context('USER_SESSION', 'PASSWD', '%s') AS C2, rdb$set_context('USER_SESSION', 'CURRENT_ORG_1', %d) AS C3, current_user, current_role FROM rdb$database"""

SET_CONTEXT_CLINIC = """SELECT rdb$set_context('USER_SESSION', 'CURRENT_ORG_1', %d) AS C3, current_user, current_role FROM rdb$database
"""

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
clinic_name, inn, kpp, ogrn, mcod,
addr_jure_town_name, addr_jure_town_socr,
addr_jure_area_name, addr_jure_area_socr,
addr_jure_country_name, addr_jure_country_socr,
addr_jure_street_name, addr_jure_street_socr,
addr_jure_house,
addr_jure_post_index
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

    def __init__(self, clinic_id = 22, mis_user = MIS_USER, mis_user_pwd = MIS_USER_PWD, mis_host = HOST, mis_db = DB):
        s_dsn = "%s:%s" % (mis_host, mis_db)
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
#        self.set_context(clinic_id, mis_user, mis_user_pwd)
        self.dbmis_authentication(clinic_id, mis_user, mis_user_pwd)
        self.get_clinic_info()


    def dbmis_authentication(self, lpu_id = 22, mis_user = MIS_USER, mis_user_pwd = MIS_USER_PWD):
        lan_ip = get_lan_ip()
        s_sqlt = "EXECUTE PROCEDURE SP_USER_AUTHENTICATION({0},'{1}','{2}')"
        s_sql  = s_sqlt.format(mis_user, mis_user_pwd, lan_ip)
        try:
            self.con.begin()
            self.cur.execute(s_sql)
            self.con.commit()
        except Exception, e:
            exctype, value = sys.exc_info()[:2]
            log.warn( 'DBMIS SP_USER_AUTHENTICATION Error: {0}'.format(e) )
            self.con.close()
            raise

        s_sqlt = "EXECUTE PROCEDURE SP_USER_CHANGE_CLINIC({0},{1})"
        s_sql  = s_sqlt.format(mis_user, lpu_id)
#        s_sql = SET_CONTEXT_CLINIC % (lpu_id)
        try:
            self.con.begin()
            self.cur.execute(s_sql)
            self.con.commit()
        except Exception, e:
            exctype, value = sys.exc_info()[:2]
            log.warn( 'DBMIS SP_USER_CHANGE_CLINIC Error: {0}'.format(e) )
            self.con.close()
            raise

        self.lpu_id = lpu_id

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

    def get_p_ids(self, lname, fname, mname, birthday):
        s_sqlt = """SELECT
                    people_id
                    FROM peoples
                    WHERE
                    UPPER(lname) = '{0}'
                    AND UPPER(fname)= '{1}'
                    AND UPPER(mname)= '{2}'
                    AND birthday = '{3}';"""
        LNAME = lname.upper().encode('cp1251')
        FNAME = fname.upper().encode('cp1251')
        bd = "%04d-%02d-%02d" % (birthday.year, birthday.month, birthday.day)
        if mname is None:
            s_sqlt = """SELECT
                        people_id
                        FROM peoples
                        WHERE
                        UPPER(lname) = '{0}'
                        AND UPPER(fname)= '{1}'
                        AND mname is Null
                        AND birthday = '{2}';"""
            s_sql = s_sqlt.format(LNAME, FNAME, bd)
        else:
            MNAME = mname.upper().encode('cp1251')
            s_sql = s_sqlt.format(LNAME, FNAME, MNAME, bd)

        self.execute(s_sql)
        results = self.cur.fetchall()
        ar = []
        for rec in results:
            ar.append(rec[0])

        return ar



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
            self.addr_jure = None
        else:
            self.name = rec[0]
            self.inn  = rec[1]
            self.kpp  = rec[2]
            self.ogrn = rec[3]
            self.mcod = rec[4]

            addr_jure_town_name = rec[5]
            addr_jure_town_socr = rec[6]
            addr_jure_area_name = rec[7]
            addr_jure_area_socr = rec[8]
            addr_jure_country_name = rec[9]
            addr_jure_country_socr = rec[10]
            addr_jure_street_name = rec[11]
            addr_jure_street_socr = rec[12]
            addr_jure_house = rec[13]
            addr_jure_post_index = rec[14]

            if addr_jure_post_index is None:
                addr_jure = u"Алтайский край, "
            else:
                addr_jure = addr_jure_post_index + u", Алтайский край, "

            if addr_jure_town_socr is not None:
                addr_jure += addr_jure_town_socr + u". "
            if addr_jure_town_name is not None:
                addr_jure += addr_jure_town_name + u", "
            if addr_jure_area_name is not None:
                addr_jure += addr_jure_area_name
                if addr_jure_area_socr is not None:
                    addr_jure += u" " + addr_jure_area_socr
                addr_jure +=  u", "
            if addr_jure_country_socr is not None:
                addr_jure += addr_jure_country_socr + u". "
            if addr_jure_country_name is not None:
                addr_jure += addr_jure_country_name + u", "
            if addr_jure_street_socr is not None:
                addr_jure += addr_jure_street_socr + u". "
            if addr_jure_street_name is not None:
                addr_jure += addr_jure_street_name + u", "
            if addr_jure_house is not None:
                addr_jure += addr_jure_house

            self.addr_jure = addr_jure

        s_sqlt = """SELECT
                a.area_id, a.area_number,
                ca.clinic_id_fk
                from areas a
                left join clinic_areas ca on a.clinic_area_id_fk = ca.clinic_area_id
                where ca.clinic_id_fk = {0} and ca.basic_speciality = 1;"""
        ssql = s_sqlt.format(clinic_id)
        self.execute(ssql)
        recs = self.cur.fetchall()
        if recs == None:
            self.clinic_areas = None
        else:
            ar = []
            for rec in recs:
                ar.append([rec[0], rec[1]])
            self.clinic_areas = ar

def dset(d1='2013-08-15', d2='2013-09-25'):
    # set random date in the region [d1-d2]
    # exlude weekends
    from datetime import datetime, timedelta
    import random

    dd1 = datetime.strptime(d1, "%Y-%m-%d").date()
    dd2 = datetime.strptime(d2, "%Y-%m-%d").date()
    ddd = dd2-dd1
    ddays = ddd.days
    while True:
        delta = random.randint(0, ddays)
        dd3  = dd1 + timedelta(days=delta)
        if dd3.weekday() not in (5,6): break

    d3 = "%04d-%02d-%02d" % (dd3.year, dd3.month, dd3.day)
    return d3


if __name__ == "__main__":
    import datetime

    dbc = DBMIS(22)
    print dbc.name, dbc.mcod, dbc.ogrn

    l_f = u'Кислицина'
    l_i = u'Татьяна'
    l_o = u'Викторовна'

    birthday = datetime.datetime.strptime('1971-02-01', "%Y-%m-%d").date()

    p_ids = dbc.get_p_ids(l_f, l_i, l_o, birthday)

    for p_id in p_ids:
        print "people_id: {0}".format(p_id)

    print dset()
