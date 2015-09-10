#!/usr/bin/python
# -*- coding: utf-8 -*-
# d-list1.py - заполнение таблицы mis.md$list
#                 из базы данных meddoc
#

import logging
import sys, codecs

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

from dbmysql_connect import DBMY

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_d_list1b.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

SQLT_ILIST = """INSERT INTO md$list (md_id, lname, fname, mname, sex, birth_dt,
b_cert_id, b_cert_type, b_cert_ser, b_cert_num, b_cert_date, b_cert_who,
b_cert_post, b_cert_when, b_cert_user, lpu, kladr_id_cr, cr, kladr_id, place, street,
b_weight, b_height, mkb_itog, mkb_1, mkb_2, mkb_3, mkb_4, mkb_5,
d_dt, d_age, d_place, d_cause, d_conf_who, d_conf_base, id_lpu)
VALUES
(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

SQLT_FLIST = """SELECT id
FROM md$list
WHERE md_id = %s;"""

DDB_HOST = "ct217.ctmed.ru"
DDB_NAME = "meddoc"
D_START = "2015-01-01"

SQLT_DGET = """SELECT
`идентификатор случая`,
`Фамилия умершего`,
`имя умершего`,
`отчество умершего`,
`пол`,
`дата и время рождения`,
`идентификатор свидетельства`,
`тип_свидетельства`,
`серия свидетельства`,
`номер свидетельства`,
`дата выдачи свидетельства`,
`фио заполнившего свидетельство`,
`должность заполнившего свидетельство`,
`дата ввода/создания свидетельства`,
`имя пользователя`,
`ЛПУ выдавшее свидетельство`,
`город_район`,
`населенный пункт`,
`улица дом`,
`масса тела при рождении`,
`длина тела при рождении`,
`код МКБ итоговый общий`,
`код МКБ а`,
`код МКБ б`,
`код МКБ в`,
`код МКБ г`,
`код МКБ д`,
`дата и время смерти`,
`возраст`,
`место смерти`,
`причина смерти`,
`удостоверил`,
`на основании`,
`id_lpu`
FROM create_list_death
WHERE `дата и время смерти` >= %s;"""

APPEND = True

def get_md_list_from_db(d_start=D_START):
    from datetime import datetime

    sout = "Getting List of Cases from: {0}:{1}".format(DDB_HOST, DDB_NAME)
    log.info(sout)

    sout = "D_START: {0}".format(d_start)
    log.info(sout)

    dbmy = DBMY(host=DDB_HOST, db=DDB_NAME)
    curr = dbmy.con.cursor()

    curr.execute(SQLT_DGET, (d_start, ))
    results = curr.fetchall()

    md_list = []

    for rec in results:
        md_id = rec[0]

        lname  = rec[1]
        fname  = rec[2]
        mname  = rec[3]
        sex    = rec[4]
        if sex[0] == u'ж':
            sex = u'Ж'
        elif sex[0] == u'м':
            sex = u'М'
        else:
            sex = u'-'

        birth_dt    = rec[5]
        b_cert_id   = rec[6]
        b_cert_type = rec[7]
        b_cert_ser  = rec[8]
        b_cert_num  = rec[9]
        b_cert_date = rec[10]
        b_cert_who  = rec[11]
        b_cert_post = rec[12]
        b_cert_when = rec[13]
        b_cert_user = rec[14]
        lpu         = rec[15]
        kladr_id_cr = None
        cr          = rec[16]
        kladr_id    = None
        place       = rec[17]
        street      = rec[18]
        b_weight    = rec[19]
        b_height    = rec[20]
        mkb_itog    = rec[21]
        mkb_1       = rec[22]
        mkb_2       = rec[23]
        mkb_3       = rec[24]
        mkb_4       = rec[25]
        mkb_5       = rec[26]

        d_dt        = rec[27]
        d_age       = rec[28]
        d_place     = rec[29]
        d_cause     = rec[30]
        d_conf_who  = rec[31]
        d_conf_base = rec[32]
        id_lpu      = rec[33]

        md_list.append([md_id, lname, fname, mname, sex, birth_dt,\
                        b_cert_id, b_cert_type, b_cert_ser, b_cert_num,\
                        b_cert_date, b_cert_who, b_cert_post, b_cert_when,
                        b_cert_user, lpu, kladr_id_cr, cr, kladr_id, place,\
                        street, b_weight, b_height, mkb_itog, mkb_1, mkb_2,\
                        mkb_3, mkb_4, mkb_5, d_dt, d_age, d_place, d_cause,\
                        d_conf_who, d_conf_base, id_lpu])

    dbmy.close()
    nnn = len(md_list)
    sout = "Have got {0} Cases Totally".format(nnn)
    log.info(sout)
    return md_list

if __name__ == "__main__":
    import time
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('----------------------------------------------------------------------------------')
    log.info('Load MedDoc Cases List. Start {0}'.format(localtime))

    md_list = get_md_list_from_db()

    dbmy = DBMY()
    curr = dbmy.con.cursor()
    curm = dbmy.con.cursor()

    if not APPEND:
        ssql = "TRUNCATE TABLE md$list;"
        curm.execute(ssql)
        dbmy.con.commit()

    nnn = 0
    for md in md_list:
        md_id       = md[0]
        curr.execute(SQLT_FLIST,(md_id,))
        rec = curr.fetchone()
        if rec is not None: continue

        lname       = md[1]
        fname       = md[2]
        mname       = md[3]
        sex         = md[4]
        birth_dt    = md[5]
        b_cert_id   = md[6]
        b_cert_type = md[7]
        b_cert_ser  = md[8]
        b_cert_num  = md[9]
        b_cert_date = md[10]
        b_cert_who  = md[11]
        b_cert_post = md[12]
        b_cert_when = md[13]
        b_cert_user = md[14]
        lpu         = md[15]
        kladr_id_cr = md[16]
        cr          = md[17]
        kladr_id    = md[18]
        place       = md[19]
        street      = md[20]
        b_weight    = md[21]
        b_height    = md[22]
        mkb_itog    = md[23]
        mkb_1       = md[24]
        mkb_2       = md[25]
        mkb_3       = md[26]
        mkb_4       = md[27]
        mkb_5       = md[28]
        d_dt        = md[29]
        d_age       = md[30]
        d_place     = md[31]
        d_cause     = md[32]
        d_conf_who  = md[33]
        d_conf_base = md[34]
        id_lpu      = md[35]

        try:
            curm.execute(SQLT_ILIST, (md_id, lname, fname, mname, sex, birth_dt,\
                                 b_cert_id, b_cert_type, b_cert_ser, \
                                 b_cert_num, b_cert_date, b_cert_who, \
                                 b_cert_post, b_cert_when, b_cert_user, lpu,
                                 kladr_id_cr, cr, kladr_id, place, street, \
                                 b_weight, b_height, mkb_itog, mkb_1, mkb_2, \
                                 mkb_3, mkb_4, mkb_5, d_dt, d_age, d_place, \
                                 d_cause, d_conf_who, d_conf_base, id_lpu, ))
            nnn += 1
        except Exception, e:
            warn_msg = "Insert into md$list error: {0}".format(e)
            log.warn(warn_msg)
            sout = "md_id: {0}".format(md_id)
            log.warn(sout)

    dbmy.close()
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Load MedDoc Cases List. Finish  '+localtime)
    sout = "Totally {0} recors have been inserted into md$list table".format(nnn)
    log.info(sout)

    sys.exit(0)
