#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# card-1.py - сбор карты для одного ребенка
#

import sys
import logging

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
PROF_EXAM_ID = 423140
PEOPLE_ID = 1403106

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
    
    cname = dbc.name.encode('utf-8')
    caddr = dbc.addr_jure.encode('utf-8')

    docTXT = "<child>"

    child = CHILD()

    child.initFromDB(dbc, people_id)
    child.medSanName = cname
    child.medSanAddress = caddr
    
    childXML = child.asXML()
    
    docTXT += childXML.asText()
    
    docTXT += "<cards>"

    docTXT += "<card>"
    card = CARD()
    card.initFromDB(dbc, card_id)
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
    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)

    cname = dbc.name.encode('utf-8')
    caddr = dbc.addr_jure.encode('utf-8')
    
    sout = "clinic_id: {0} clinic_name: {1}".format(clinic_id, cname)
    log.info(sout)
    sout = "address: {0}".format(caddr)
    log.info(sout)

    docTXT = getCard(dbc, PROF_EXAM_ID, PEOPLE_ID)
    
    log.info(docTXT)
    
    dbc.close()
    sys.exit(0)
    