#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# card-1.py - сбор карты для одного ребенка
#

import sys
import logging

from medlib.moinfolist import MoInfoList
modb = MoInfoList()

from medlib.modules.medobjects.SimpleXmlConstructor import SimpleXmlConstructor

from child import CHILD
from card import CARD
from card_diag_b import CARD_DIAG_B_ARR
from card_diag_b import CARD_DIAG_A_ARR
from card_results import CARD_RESULTS
from card import getDoctor

HOST      = "fb2.ctmed.ru"
DB        = "DBMIS"

CLINIC_ID = 268
# только осмотры, нет исследований
# PROF_EXAM_ID = 392088
# PEOPLE_ID = 1567963
# no sex formula
PROF_EXAM_ID = 324132
PEOPLE_ID = 956579

FNAME = "PN{0}.xml"
FPATH = "./PN"

SQLT_O1 = """SELECT
oplata
FROM prof_exam_stages
WHERE prof_exam_id_fk = ?;"""

if __name__ == "__main__":
    LOG_FILENAME = '_card1.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

def addNode(doc, nodeName, nodeValue):
    doc.startNode(nodeName)
    doc.putText(nodeValue)
    doc.endNode()

def getOplata(dbc, card_id):
    doc = SimpleXmlConstructor()
    cur = dbc.con.cursor()
    cur.execute(SQLT_O1, (card_id, ))
    rec = cur.fetchone()
    
    if rec is None:
        oms = 0
    else:
        oms = rec[0]
        if oms <> 1:
            oms = 0
    addNode(doc, "oms", str(oms))

    return doc
    

def getCard(dbc, card_id = PROF_EXAM_ID, people_id = PEOPLE_ID):
    from dateutil.relativedelta import relativedelta
    
    cname = dbc.name.encode('utf-8')
    caddr = dbc.addr_jure.encode('utf-8')

    docTXT = "<child>"

    child = CHILD()
    child.initFromDB(dbc, people_id)
    child.medSanName = cname
    child.medSanAddress = caddr
    
    bd = child.birthday
    
    childXML = child.asXML()
    
    docTXT += childXML.asText()
    
    docTXT += "<cards>"

    docTXT += "<card>"
    card = CARD()
    card.initFromDB(dbc, card_id)
    dateOfObsled = card.dateOfObsled
    age = relativedelta(dateOfObsled, bd).years
    card.age = age
    card.sex = child.sex
    cardXML = card.asXML()
    docTXT += cardXML.asText()
    
    card_diag_b = CARD_DIAG_B_ARR()
    card_diag_b.initFromDB(dbc, card_id)
    asXML = card_diag_b.asXML()
    docTXT += asXML.asText()

    card_diag_a = CARD_DIAG_A_ARR()
    card_diag_a.initFromDB(dbc, card_id)
    asXML = card_diag_a.asXML()
    docTXT += asXML.asText()
    
    card_results = CARD_RESULTS()
    card_results.initFromDB(dbc, card_id)
    issledXML = card_results.issledXML()
    if issledXML is None:
        docTXT += "<issled />"
    else:
        docTXT += issledXML.asText()
    
    z_XML = card.z_asXML()
    docTXT += z_XML.asText()
    
    d_XML = getDoctor(dbc, card_id)
    docTXT += d_XML.asText()

    osmotriXML = card_results.osmotriXML()
    docTXT += osmotriXML.asText()

    docTXT += "<recommendZOZH>Режим: Щадящий; питание: Рациональное; иммунопрофилактика: по возрасту</recommendZOZH>"
    
    docTXT += "<privivki><state>1</state></privivki>"

    omsXML = getOplata(dbc, card_id)
    docTXT += omsXML.asText()
    
    docTXT += "</card>"
    docTXT += "</cards>"

    docTXT += "</child>"

    return docTXT


if __name__ == "__main__":
    from dbmis_connect2 import DBMIS
    
    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    clinic_id = CLINIC_ID
    mcod = modb.moCodeByMisId(clinic_id)
    
    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)

    cname = dbc.name.encode('utf-8')
    caddr = dbc.addr_jure.encode('utf-8')
    
    sout = "clinic_id: {0} mcod: {1} clinic_name: {2}".format(clinic_id, mcod, cname)
    log.info(sout)
    sout = "address: {0}".format(caddr)
    log.info(sout)

    f_fname = FPATH + "/" + FNAME.format(mcod)
    sout = "Output to file: {0}".format(f_fname)
    log.info(sout)
    
    fo = open(f_fname, "wb")
    
    sout = """<?xml version="1.0" encoding="UTF-8"?>
    <children>"""
    fo.write(sout)

    docTXT = getCard(dbc, PROF_EXAM_ID, PEOPLE_ID)
    fo.write(docTXT)
    sout = '</children>'
    fo.write(sout)
    fo.close()
    
    log.info(docTXT)
    
    dbc.close()
    sys.exit(0)
    