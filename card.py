#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# card.py - PROF_EXAM card
#

import sys
import logging
from medlib.modules.medobjects.SimpleXmlConstructor import SimpleXmlConstructor
from child_const import TYPE_EXAM_CODE, TYPE_EXAM, TYPE_EXAM0

HOST      = "fb2.ctmed.ru"
DB        = "DBMIS"

CLINIC_ID = 200
PROF_EXAM_ID = 6185

SQLT_E1 = """SELECT
date_begin, type_exam_code, height, weight
FROM prof_exam_minor
WHERE prof_exam_id = ?;"""

if __name__ == "__main__":
    LOG_FILENAME = '_card.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)


def addNode(doc, nodeName, nodeValue):
    doc.startNode(nodeName)
    doc.putText(nodeValue)
    doc.endNode()

class CARD:
    def __init__(self):
        self.idInternal = None
        self.dateOfObsled = None
        self.idType = None
        self.height = None
        self.weight = None
        
    def initFromDB(self, dbc, exam_id):
        cur = dbc.con.cursor()
        cur.execute(SQLT_E1, (exam_id, ))
        rec = cur.fetchone()
        if rec is None:
            self.__init__()
        else:
            self.idInternal = exam_id
            self.dateOfObsled = rec[0]
            type_exam_code = rec[1]
            try:
                type_exam = TYPE_EXAM[type_exam_code]
            except:
                type_exam = TYPE_EXAM0
            self.idType = type_exam
            self.height = rec[2]
            self.weight = rec[3]
            
    def asXML(self):
        doc = SimpleXmlConstructor()    
        idInternal = "{0}".format(self.idInternal)
        addNode(doc, "idInternal", idInternal)
        do = self.dateOfObsled
        dateOfObsled = "%04d-%02d-%02d" % (do.year, do.month, do.day)
        addNode(doc, "dateOfObsled", dateOfObsled)
        addNode(doc, "idType", self.idType)

        height = self.height
        if height is None:
            height = 0
        addNode(doc, "height", str(height))
        weight = self.weight
        if weight is None:
            weight = 0
        addNode(doc, "weight", str(weight))

        return doc

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

    card = CARD()
    
    card_id = PROF_EXAM_ID
    
    card.initFromDB(dbc, card_id)
    
    sout = "card_id: {0}".format(card_id)
    log.info(sout)
    cardXML = card.asXML()
    log.info(cardXML.asText())
    