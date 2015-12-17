#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# card_results.py - PROF_EXAM_RESULTS table
#

import sys
import logging
from SimpleXmlConstructor import SimpleXmlConstructor
from child_const import ISSLED_ID, OSMOTR_ID

HOST      = "fb2.ctmed.ru"
DB        = "DBMIS"

CLINIC_ID = 268
PROF_EXAM_ID = 392088

SQLT_R1 = """SELECT
cc_line, date_checkup, diagnosis_id_fk
FROM prof_exam_results
WHERE prof_exam_id_fk = ?;"""

if __name__ == "__main__":
    LOG_FILENAME = '_card_results.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)


def addNode(doc, nodeName, nodeValue):
    doc.startNode(nodeName)
    doc.putText(nodeValue)
    doc.endNode()

class CARD_RESULT:
    def __init__(self, cc_line = None, date_checkup = None, diagnosis_id_fk = None):
        self.cc_line = cc_line
        self.date_checkup = date_checkup
        self.diagnosis_id_fk = diagnosis_id_fk

class CARD_RESULTS:
    def __init__(self):
        self.idInternal = None
        self.results = None
        
    def initFromDB(self, dbc, exam_id):
        cur = dbc.con.cursor()
        cur.execute(SQLT_R1, (exam_id, ))
        recs = cur.fetchall()
        if recs is None:
            self.__init__()
        else:
            self.idInternal = exam_id
            arr = []
            for rec in recs:
                cc_line = rec[0]
                date_checkup = rec[1]
                diagnosis_id_fk = rec[2]
                card_result = CARD_RESULT(cc_line, date_checkup, diagnosis_id_fk)
                arr.append(card_result)
            self.results = arr
            
    def issledXML(self):
        arr = self.results
        doc = SimpleXmlConstructor()
        doc.startNode("issled")
        doc.startNode("basic")
        nn = 0
        for card_result in arr:
            cc_line = card_result.cc_line
            di = card_result.date_checkup
            if di is None: continue
            ds = card_result.diagnosis_id_fk
            try:
                issled_id = ISSLED_ID[cc_line]
            except:
                issled_id = "0"
            
            if issled_id > "0":
                nn += 1
                doc.startNode("record")
                addNode(doc, "id", issled_id)
                dd = "%04d-%02d-%02d" % (di.year, di.month, di.day)
                addNode(doc, "date", dd)
                addNode(doc, "result", "Выполнено")
                doc.endNode() # record
        doc.endNode() # basic
        doc.endNode() # issled
        
        if nn == 0:
            doc = None
        return doc

    def osmotriXML(self):
        doc = SimpleXmlConstructor()
        doc.startNode("osmotri")
        arr = self.results
        for card_result in arr:
            cc_line = card_result.cc_line
            di = card_result.date_checkup
            if di is None: continue
            ds = card_result.diagnosis_id_fk
            try:
                osmotr_id = OSMOTR_ID[cc_line]
            except:
                osmotr_id = "0"
            
            if osmotr_id > "0":
                doc.startNode("record")
                addNode(doc, "id", osmotr_id)
                dd = "%04d-%02d-%02d" % (di.year, di.month, di.day)
                addNode(doc, "date", dd)
                doc.endNode() # record
        doc.endNode() # osmotri

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

    card_results = CARD_RESULTS()
    
    card_id = PROF_EXAM_ID
    
    card_results.initFromDB(dbc, card_id)
    
    sout = "card_id: {0}".format(card_id)
    log.info(sout)
    log.info(" issled:")
    issledXML = card_results.issledXML()
    if issledXML is None:
        log.info("<issled />")
    else:
        log.info(issledXML.asText())

    log.info(" osmotri:")
    osmotriXML = card_results.osmotriXML()
    log.info(osmotriXML.asText())
    