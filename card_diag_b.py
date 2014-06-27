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
PROF_EXAM_ID = 346

SQLT_D1 = """SELECT
diag_id, dn, ln1, ul1, mo1, lv1,
ln2, ul3, mo3, lv2,
vmp
FROM prof_exam_diag_b
WHERE prof_exam_id = ?;"""

SQLT_D2 = """SELECT
diag_id, uv, dn, 
ln3, ul3, mo3,
ln4, ul4, mo4,
in1, ul1, mo1, iv2,
vmp, ms_uid
FROM prof_exam_diag_a
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
        self.ln2 = None
        self.ul3 = None
        self.mo3 = None
        self.lv2 = None
        self.vmp = None

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

                card_diag_b.ln2 = rec[6]
                card_diag_b.ul3 = rec[7]
                card_diag_b.mo3 = rec[8]
                card_diag_b.lv2 = rec[9]
                card_diag_b.vmp = rec[10]
                
                arr.append(card_diag_b)

            self.diag_b_arr = arr
            
    def asXML(self):
        doc = SimpleXmlConstructor()
        arr = self.diag_b_arr
        if len(arr) == 0:
            return doc
        doc.startNode("diagnosisBefore")
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
            
            ln2 = diag_b.ln2
            if ln2 == 1:
                doc.startNode("reabil")
                ul3 = diag_b.ul3
                if ul3 is None: ul3 = 3
                addNode(doc, "condition", str(ul3))
                mo3 = diag_b.mo3
                if mo3 is None: mo3 = 2
                addNode(doc, "organ", str(mo3))
                lv2 = diag_b.lv2
                if lv2 == 2:
                    addNode(doc, "notDone", "1")
                doc.endNode() # reabil

                
            if diag_b.vmp == 1:
                addNode(doc, "vmp", "1")
            else:
                addNode(doc, "vmp", "0")
            doc.endNode() # diagnosis
        doc.endNode() # diagnosisBefore

        return doc

# DIAG_A

class CARD_DIAG_A:
    def __init__(self):
        self.mkb = None
        self.uv = None
        self.dn = None
        self.ln3 = None
        self.ul3 = None
        self.mo3 = None
        self.ln4 = None
        self.ul4 = None
        self.mo4 = None
        self.in1 = None
        self.ul1 = None
        self.mo1 = None
        self.iv2 = None
        self.vmp = None
        self.ms_uid = None

class CARD_DIAG_A_ARR:
    def __init__(self):
        self.idInternal = None
        self.diag_b_arr = None
        
    def initFromDB(self, dbc, exam_id):
        cur = dbc.con.cursor()
        cur.execute(SQLT_D2, (exam_id, ))
        recs = cur.fetchall()
        if recs is None:
            self.__init__()
        else:
            self.idInternal = exam_id
            arr = []
            for rec in recs:
                card_diag_a = CARD_DIAG_A()
                
                card_diag_a.mkb = rec[0]

                card_diag_a.uv = rec[1]
                card_diag_a.dn = rec[2]
                card_diag_a.ln3 = rec[3]
                card_diag_a.ul3 = rec[4]
                card_diag_a.mo3 = rec[5]
                card_diag_a.ln4 = rec[6]
                card_diag_a.ul4 = rec[7]
                card_diag_a.mo4 = rec[8]
                card_diag_a.in1 = rec[9]
                card_diag_a.ul1 = rec[10]
                card_diag_a.mo1 = rec[11]
                card_diag_a.iv2 = rec[12]
                card_diag_a.vmp = rec[13]
                card_diag_a.ms_uid = rec[14]
                
                arr.append(card_diag_a)

            self.diag_a_arr = arr
            
    def asXML(self):
        doc = SimpleXmlConstructor()
        arr = self.diag_a_arr
        if len(arr) == 0:
            addNode(doc, "healthyMKB", "Z00.0")
            return doc
        doc.startNode("diagnosisAfter")
        for diag_a in arr:
            mkb = diag_a.mkb
            if mkb is None: continue
            doc.startNode("diagnosis")
            addNode(doc, "mkb", mkb.strip())
            
            if diag_a.uv == 1:
                addNode(doc, "firstTime", "1")
            else:
                addNode(doc, "firstTime", "0")
        
            dn = diag_a.dn
            if (dn is None) or (dn == 3): dn = 0
            addNode(doc, "dispNablud", str(dn))

            ln3 = diag_a.ln3
            if ln3 == 1:
                doc.startNode("lechen")
                ul3 = diag_a.ul3
                if ul3 is None: ul3 = 1
                addNode(doc, "condition", str(ul3))
                mo3 = diag_a.mo3
                if mo3 is None: mo3 = 2
                addNode(doc, "organ", str(mo3))
                doc.endNode() # lechen

            ln4 = diag_a.ln4
            if ln4 == 1:
                doc.startNode("reabil")
                ul4 = diag_a.ul4
                if ul4 is None: ul4 = 3
                addNode(doc, "condition", str(ul4))
                mo4 = diag_a.mo4
                if mo4 is None: mo4 = 2
                addNode(doc, "organ", str(mo4))
                doc.endNode() # reabil
            
            in1 = diag_a.in1
            if in1 == 1:
                doc.startNode("consul")
                ul1 = diag_a.ul1
                if ul1 is None: ul1 = 1
                addNode(doc, "condition", str(ul1))
                mo1 = diag_a.mo1
                if mo1 is None: mo1 = 1
                addNode(doc, "organ", str(mo1))
                iv2 = diag_a.iv2
                if (iv2 is None) or (iv2 == 2): iv2 = 0
                addNode(doc, "state", str(iv2))
                doc.endNode() # consul
                
            vmp = diag_a.vmp
            if (vmp is None) or (vmp == 2): vmp = 0
            addNode(doc, "needVMP", str(vmp))
            addNode(doc, "needSMP", "0")
	    addNode(doc, "needSKL", "0")
	    
            ms_uid = diag_a.ms_uid
	    addNode(doc, "recommendNext", "Нет")
            doc.endNode() # diagnosis

        doc.endNode() # diagnosisAfter

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


    card_diag_a = CARD_DIAG_A_ARR()
    
    card_id = PROF_EXAM_ID
    
    card_diag_a.initFromDB(dbc, card_id)
    
    asXML = card_diag_a.asXML()
    log.info(asXML.asText())
    