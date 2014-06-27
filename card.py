#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# card.py - PROF_EXAM card (PROF_EXAM_MINOR table)
#

import sys
import logging
from medlib.modules.medobjects.SimpleXmlConstructor import SimpleXmlConstructor
from child_const import TYPE_EXAM_CODE, TYPE_EXAM, TYPE_EXAM0

HOST      = "fb2.ctmed.ru"
DB        = "DBMIS"

CLINIC_ID = 200
#PROF_EXAM_ID = 170
# PROF_EXAM_ID = 6185
PROF_EXAM_ID = 268

SQLT_E1 = """SELECT
date_begin, type_exam_code, height, weight,
fr_code, nfr_code,
p_pf, p_mf, p_ecf, p_rr,
p_ps_code, p_i_code, p_evs_code,
f_p, f_ax, f_fa, f_ma, f_me,
mens1_code, mens2_code, mens3_code,
b_hr_code, b_pg_code
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
        
        self.fr_code  = None
        self.nfr_code = None
        
        self.p_pf = None
        self.p_mf = None
        self.p_ecf = None
        self.p_rr = None
        
        self.p_ps_code = None
        self.p_i_code = None
        self.p_evs_code = None
        
        self.f_p = None
        self.f_ax = None
        self.f_fa = None
        self.f_ma = None
        self.f_me = None
        
        self.mens1_code = None
        self.mens2_code = None
        self.mens3_code = None
        
        self.b_hr_code = None
        self.b_pg_code = None
        
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

            self.fr_code  = rec[4]
            self.nfr_code = rec[5]

            self.p_pf = rec[6]
            self.p_mf = rec[7]
            self.p_ecf = rec[8]
            self.p_rr = rec[9]

            self.p_ps_code = rec[10]
            self.p_i_code = rec[11]
            self.p_evs_code = rec[12]

            self.f_p = rec[13]
            self.f_ax = rec[14]
            self.f_fa = rec[15]
            self.f_ma = rec[16]
            self.f_me = rec[17]

            self.mens1_code = rec[18]
            self.mens2_code = rec[19]
            self.mens3_code = rec[20]

            self.b_hr_code = rec[21]
            self.b_pg_code = rec[22]

            
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
        
        nfr_code = self.nfr_code
        if nfr_code is not None:
            doc.startNode("healthProblems")
            addNode(doc, "problem", str(nfr_code))
            doc.endNode()
            
        if self.p_pf is not None:
            doc.startNode("pshycDevelopment")
            addNode(doc, "poznav", str(self.p_pf))
            addNode(doc, "motor", str(self.p_mf))
            addNode(doc, "emot", str(self.p_ecf))
            addNode(doc, "rech", str(self.p_rr))
            doc.endNode() # pshycDevelopment

        if (self.p_ps_code is not None) and (self.p_ps_code > 0):
            doc.startNode("pshycState")
            addNode(doc, "psihmot", str(self.p_ps_code))
            addNode(doc, "intel", str(self.p_i_code))
            addNode(doc, "emotveg", str(self.p_evs_code))
            doc.endNode() # pshycState

        if self.f_fa is not None:
            doc.startNode("sexFormulaMale")
            addNode(doc, "P", str(self.f_p))
            addNode(doc, "Ax", str(self.f_ax))
            addNode(doc, "Fa", str(self.f_fa))
            doc.endNode() # sexFormulaMale
        else:
            doc.startNode("sexFormulaFemale")
            addNode(doc, "P", str(self.f_p))
            addNode(doc, "Ma", str(self.f_ma))
            addNode(doc, "Ax", str(self.f_ax))
            addNode(doc, "Me", str(self.f_me))
            doc.endNode() # sexFormulaFemale
            
            if (self.mens1_code is not None) and (self.mens1_code > 0):
                doc.startNode("menses")
                addNode(doc, "menarhe", "150") # ???
                doc.startNode("characters")
                addNode(doc, "char", str(self.mens1_code))
                addNode(doc, "char", str(self.mens2_code))
                addNode(doc, "char", str(self.mens3_code))
                doc.endNode() # characters
                doc.endNode() # menses
                
        addNode(doc, "healthGroupBefore", str(self.b_hr_code))
        addNode(doc, "fizkultGroupBefore", str(self.b_pg_code))

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
    