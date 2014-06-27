#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# diagnosisBefore 
# card_diag_b.py - PROF_EXAM_DIAG_B table
#

import sys
import logging
from medlib.modules.medobjects.SimpleXmlConstructor import SimpleXmlConstructor

HOST      = "fb2.ctmed.ru"
DB        = "DBMIS"

CLINIC_ID = 200
#PROF_EXAM_ID = 170
# PROF_EXAM_ID = 6185
PROF_EXAM_ID = 90

SQLT_D1 = """SELECT
diag_id, dn, ln1, ul1, mo1, lv1
FROM prof_exam_diag_b
WHERE prof_exam_id = ?;"""

if __name__ == "__main__":
    LOG_FILENAME = '_card_diag_b.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)


def addNode(doc, nodeName, nodeValue):
    doc.startNode(nodeName)
    doc.putText(nodeValue)
    doc.endNode()

class CARD_DIAG_B:
    def __init__(self):
        self.mkb = None
        self.dn = None
        self.ln1 = None
        self.ul1 = None
        self.mo1 = None
        self.lv1 = None

class CARD_DIAG_B_ARR:
    def __init__(self):
        self.idInternal = None
        self.diag_b_arr = None
        
    def initFromDB(self, dbc, exam_id):
        cur = dbc.con.cursor()
        cur.execute(SQLT_D1, (exam_id, ))
        recs = cur.fetchall()
        if recs is None:
            self.__init__()
        else:
            self.idInternal = exam_id
            arr = []
            for rec in recs:
                card_diag_b = CARD_DIAG_B()
                
                card_diag_b.mkb = rec[0]
                card_diag_b.dn = rec[1]
                card_diag_b.ln1 = rec[2]
                card_diag_b.ul1 = rec[3]
                card_diag_b.mo1 = rec[4]
                card_diag_b.lv1 = rec[5]
                
                arr.append(card_diag_b)

            self.diag_b_arr = arr
            
    def asXML(self):
        doc = SimpleXmlConstructor()
        doc.startNode("diagnosisBefore")
        arr = self.diag_b_arr
        for diag_b in arr:
            mkb = diag_b.mkb
            if mkb is None: continue
            doc.startNode("diagnosis")
            addNode(doc, "mkb", mkb.strip())
            dn = diag_b.dn
            if dn is None: dn = 3
            addNode(doc, "dispNablud", str(dn))

            ln1 = diag_b.ln1
            if ln1 == 1:
                doc.startNode("lechen")
                ul1 = diag_b.ul1
                if ul1 is None: ul1 = 1
                addNode(doc, "condition", str(ul1))
                mo1 = diag_b.mo1
                if mo1 is None: mo1 = 2
                addNode(doc, "organ", str(mo1))
                lv1 = diag_b.lv1
                if lv1 == 2:
                    addNode(doc, "notDone", "1")
                doc.endNode() # lechen
            doc.endNode() # diagnosis
        doc.endNode() # diagnosisBefore

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

    card_diag_b = CARD_DIAG_B_ARR()
    
    card_id = PROF_EXAM_ID
    
    card_diag_b.initFromDB(dbc, card_id)
    
    sout = "card_id: {0}".format(card_id)
    asXML = card_diag_b.asXML()
    log.info(asXML.asText())

    