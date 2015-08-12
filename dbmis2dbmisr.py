#!/usr/bin/python
# coding: utf-8
# dbmis2dbmisr.py - импорт посещений
#                   из DBMIS (Firebird)
#                   в dbmisr(MySQL)
#

import os, sys, codecs
from datetime import datetime

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

from dbmysql_connect import DBMY
import fdb
from dbmis_connect2 import DBMIS

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

import logging

if __name__ == "__main__":
    LOG_FILENAME = '_dbmis2dbmisr.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

STEP = 10000
LIMIT = 1000000

HOST = "fb2.ctmed.ru"
DB = "DBMIS"

M_HOST = "ct216.ctmed.ru"
M_DB = "dbmisr"

SQLT_F_TICKET_ID = """SELECT ticket_id
FROM dbmisr.tickets
ORDER BY ticket_id DESC
LIMIT 1;"""

SQLT_GET_TICKETS = """SELECT FIRST ?
ticket_id,
clinic_id_fk, people_id_fk, visit_date, visit_time,
visit_motive_id_fk, worker_id_fk
FROM tickets
WHERE ticket_id > ?
ORDER BY ticket_id;"""

class DIAGNOSIS:
    def __init__(self):
        ticket_diagnosis_id = None
        ticket_id_fk = None
        line = None
        diagnosis_id_fk = None
        medical_service_id_fk = None
        visit_type_id_fk = None
        diagnosis_state_id_fk = None

class TICKET:
    SQLT_GET_PEOPLE = """SELECT sex, birthday,
    insurance_certificate, enp
    FROM peoples
    WHERE people_id = ?;"""

    SQLT_GET_CLINIC = """SELECT clinic_name, mcod
    FROM clinics
    WHERE clinic_id = ?;"""

    SQLT_GET_WORKER = """SELECT speciality_id_fk
    FROM workers
    WHERE worker_id = ?;"""

    SQLT_GET_DIAGS = """SELECT
    ticket_diagnosis_id, line, diagnosis_id_fk,
    medical_service_id_fk, visit_type_id_fk, diagnosis_state_id_fk
    FROM ticket_diagnosis
    WHERE ticket_id_fk = ?;"""

    SQLT_PUT_TICKET = """INSERT INTO tickets
    (ticket_id, clinic_id_fk, people_id_fk, visit_dt,
    visit_motive_id_fk, worker_id_fk)
    VALUES
    (%s, %s, %s, %s, %s, %s);"""

    SQLT_FIND_PEOPLE = "SELECT enp FROM peoples WHERE people_id =%s;"

    SQLT_FIND_CLINIC = "SELECT clinic_name FROM clinics WHERE clinic_id =%s;"

    SQLT_FIND_WORKER = "SELECT speciality_id_fk FROM workers WHERE worker_id =%s;"

    SQLT_PUT_PEOPLE = """INSERT INTO peoples
    (people_id, sex, birthday, snils, enp)
    VALUES
    (%s, %s, %s, %s, %s);"""

    SQLT_PUT_CLINUIC = """INSERT INTO clinics
    (clinic_id, clinic_name, mcod)
    VALUES
    (%s, %s, %s);"""

    SQLT_PUT_WORKER = """INSERT INTO workers
    (worker_id, speciality_id_fk)
    VALUES
    (%s, %s);"""

    SQLT_PUT_DIAG = """INSERT INTO ticket_diagnosis
    (ticket_diagnosis_id, ticket_id_fk, line, diagnosis_id_fk,
    medical_service_id_fk, visit_type_id_fk,  diagnosis_state_id_fk)
    VALUES
    (%s, %s, %s, %s, %s, %s, %s);"""

    def __init__(self, ticket_id = None):
        self.ticket_id = ticket_id
        self.clinic_id_fk = None
        self.people_id_fk = None
        self.visit_date = None
        self.visit_time = None
        self.visit_motive_id_fk = None

        self.sex = None
        self.birthday = None
        self.snils = None
        self.enp = None

        self.clinic_name = None
        self.mcod = None

        self.worker_id_fk = None
        self.speciality_id_fk = None

        self.diags = []

    def get_people(self, cur):
        if self.people_id_fk:
            cur.execute(self.SQLT_GET_PEOPLE, (self.people_id_fk, ))
            rec = cur.fetchone()
            if rec:
                self.sex = rec[0]
                self.birthday = rec[1]
                self.snils = rec[2]
                self.enp = rec[3]

    def get_clinic(self, cur):
        if self.clinic_id_fk:
            cur.execute(self.SQLT_GET_CLINIC, (self.clinic_id_fk, ))
            rec = cur.fetchone()
            if rec:
                self.clinic_name = rec[0]
                if rec[1]:
                    self.mcod = int(rec[1])

    def get_worker(self, cur):
        if self.worker_id_fk:
            cur.execute(self.SQLT_GET_WORKER, (self.worker_id_fk, ))
            rec = cur.fetchone()
            if rec:
                self.speciality_id_fk = rec[0]

    def get_diags(self, cur):
        if self.ticket_id:
            cur.execute(self.SQLT_GET_DIAGS, (self.ticket_id, ))
            results = cur.fetchall()
            self.diags = []
            for rec in results:
                diag = DIAGNOSIS()
                diag.ticket_id_fk = self.ticket_id
                diag.ticket_diagnosis_id = rec[0]
                diag.line = rec[1]
                diag.diagnosis_id_fk = rec[2]
                diag.medical_service_id_fk = rec[3]
                diag.visit_type_id_fk = rec[4]
                diag.diagnosis_state_id_fk = rec[5]
                self.diags.append(diag)

    def put2dbmisr(self, cur):

        if self.ticket_id is None: return

        ticket_id = self.ticket_id
        clinic_id_fk = self.clinic_id_fk
        people_id_fk = self.people_id_fk
        visit_date = self.visit_date
        visit_time = self.visit_time
        visit_motive_id_fk = self.visit_motive_id_fk
        worker_id_fk = self.worker_id_fk

        if visit_time:
            visit_dt = datetime.combine(visit_date, visit_time)
        else:
            visit_dt = visit_date

        cur.execute(self.SQLT_PUT_TICKET, (ticket_id, clinic_id_fk, \
                                           people_id_fk, visit_dt, \
                                           visit_motive_id_fk, \
                                           worker_id_fk))

        sex = self.sex
        birthday = self.birthday
        snils = self.snils
        enp = self.enp

        cur.execute(self.SQLT_FIND_PEOPLE, (people_id_fk, ))
        rec = cur.fetchone()
        if rec is None:
            cur.execute(self.SQLT_PUT_PEOPLE, (people_id_fk, \
                                               sex, birthday, \
                                               snils, enp))

        clinic_name = self.clinic_name
        mcod = self.mcod

        cur.execute(self.SQLT_FIND_CLINIC, (clinic_id_fk, ))
        rec = cur.fetchone()
        if rec is None:
            cur.execute(self.SQLT_PUT_CLINUIC, (clinic_id_fk,
                                           clinic_name, mcod))

        if worker_id_fk:
            speciality_id_fk = self.speciality_id_fk
            cur.execute(self.SQLT_FIND_WORKER, (worker_id_fk, ))
            rec = cur.fetchone()
            if rec is None:
                cur.execute(self.SQLT_PUT_WORKER, (worker_id_fk, \
                                                   speciality_id_fk))

        for diag in self.diags:
            ticket_diagnosis_id = diag.ticket_diagnosis_id
            ticket_id_fk = diag.ticket_id_fk
            line = diag.line
            diagnosis_id_fk = diag.diagnosis_id_fk
            medical_service_id_fk = diag.medical_service_id_fk
            visit_type_id_fk = diag.visit_type_id_fk
            diagnosis_state_id_fk = diag.diagnosis_state_id_fk

            cur.execute(self.SQLT_PUT_DIAG, (ticket_diagnosis_id, \
                                             ticket_id_fk, \
                                             line, \
                                             diagnosis_id_fk, \
                                             medical_service_id_fk, \
                                             visit_type_id_fk, \
                                             diagnosis_state_id_fk))


if __name__ == "__main__":

    import time

    log.info("======================= DBMIS2DBMISR==========================================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('DBMIS 2 DBMISR import. Start {0}'.format(localtime))

    sout = "Source database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    sout = "Target database: {0}:{1}".format(M_HOST, M_DB)
    log.info(sout)

    dbmy = DBMY(host = M_HOST, db = M_DB)
    curm = dbmy.con.cursor()

    curm.execute(SQLT_F_TICKET_ID)
    recm = curm.fetchone()
    if recm:
        start_ticket_id = recm[0]
    else:
        start_ticket_id = 0

    sout = "Start ticket_id: {0}".format(start_ticket_id)
    log.info(sout)

    dbc = DBMIS(mis_host = HOST, mis_db = DB)
    cur = dbc.con.cursor()

    # Create new READ ONLY READ COMMITTED transaction
    ro_transaction = dbc.con.trans(fdb.ISOLATION_LEVEL_READ_COMMITED_RO)
    # and cursor
    ro_cur = ro_transaction.cursor()

    cur.execute(SQLT_GET_TICKETS, (LIMIT, start_ticket_id, ))
    results = cur.fetchall()

    i = 0

    for rec in results:
        ticket_id = rec[0]
        clinic_id_fk = rec[1]
        people_id_fk = rec[2]
        visit_date = rec[3]
        visit_time = rec[4]
        visit_motive_id_fk = rec[5]
        worker_id_fk = rec[6]

        ticket= TICKET(ticket_id)
        ticket.clinic_id_fk = clinic_id_fk
        ticket.people_id_fk = people_id_fk
        ticket.visit_date = visit_date
        ticket.visit_time = visit_time
        ticket.visit_motive_id_fk = visit_motive_id_fk
        ticket.worker_id_fk = visit_motive_id_fk

        ticket.get_people(ro_cur)
        ticket.get_clinic(ro_cur)
        ticket.get_worker(ro_cur)
        ticket.get_diags(ro_cur)

        ticket.put2dbmisr(curm)

        i += 1

        if i % STEP == 0:
            sout = " {0}: ticket_id: {1}".format(i, ticket_id)
            log.info(sout)

    dbmy.con.close()
    dbc.con.close()

    localtime = time.asctime( time.localtime(time.time()) )
    log.info('DBMIS 2 DBMISR import. Finish {0}'.format(localtime))

    sys.exit(0)

