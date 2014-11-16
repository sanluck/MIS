#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# invalid
# card_vaccination.py - PROF_EXAM_MINOR table
#                       PRIV - список прививок
#                       PRIV_N_CODE - Прививка назначена
#                       PRIV_V_CODE - Прививка выполнена
#

import sys
import logging
from medlib.modules.medobjects.SimpleXmlConstructor import SimpleXmlConstructor

HOST      = "fb2.ctmed.ru"
DB        = "DBMIS"

CLINIC_ID = 121
PROF_EXAM_ID = 650598

SQLT_PRIV = """SELECT
priv, priv_n_code, priv_v_code
FROM prof_exam_minor
WHERE prof_exam_id = ?;"""

if __name__ == "__main__":
    LOG_FILENAME = '_card_vaccination.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

PRIV = {}
PRIV[1]  = [1,u'БЦЖ - V']
PRIV[2]  = [2,u'БЦЖ - R1']
PRIV[3]  = [3,u'БЦЖ - R2']
PRIV[4]  = [4,u'Полиомиелит - V1']
PRIV[5]  = [5,u'Полиомиелит - V2']
PRIV[6]  = [6,u'Полиомиелит - V3']
PRIV[7]  = [7,u'Полиомиелит - R1']
PRIV[8]  = [8,u'Полиомиелит - R2']
PRIV[9]  = [9,u'Полиомиелит - R3']
PRIV[10] = [10,u'АКДС - V1']
PRIV[11] = [11,u'АКДС - V2']
PRIV[12] = [12,u'АКДС - V3']
PRIV[13] = [13,u'АКДС - АДСМ']
PRIV[14] = [14,u'АКДС - АДМ']
PRIV[15] = [15,u'Корь - V']
PRIV[16] = [16,u'Корь - R']
PRIV[17] = [17,u'Эпид.паротит - V']
PRIV[18] = [18,u'Эпид.паротит - R']
PRIV[19] = [19,u'Краснуха - V']
PRIV[20] = [20,u'Краснуха - R']
PRIV[21] = [21,u'Гепатит В - V1']
PRIV[22] = [22,u'Гепатит В - V2']
PRIV[23] = [23,u'Гепатит В - V3']

def addNode(doc, nodeName, nodeValue):
    doc.startNode(nodeName)
    doc.putText(nodeValue)
    doc.endNode()

class CARD_PRIV:
    def __init__(self):
	self.idInternal = None
        self.state = None
        self.privs = None

    def initFromDB(self, dbc, exam_id):
        cur = dbc.con.cursor()
        cur.execute(SQLT_PRIV, (exam_id, ))
        rec = cur.fetchone()
        if rec is None:
            self.__init__()
        else:
            self.idInternal = exam_id
	    if rec[1] is None:
		self.state = 1
	    else:
		self.state = rec[1]
	    if rec[0] is None:
		self.privs = []
	    else:
		self.privs = rec[0].split(",")

    def asXML(self):
        doc = SimpleXmlConstructor()
        if (self.idInternal is None): return doc
        doc.startNode("privivki")
        addNode(doc, "state", str(self.state))
	privs = self.privs
	if len(privs) > 0:
	    doc.startNode("privs")
	    for priv in privs:
		if len(priv) > 0:
		    ipriv = int(priv)
		    if ipriv >= 1 and ipriv <= 23:
			ipriv += 5
			spriv = str(ipriv)
			addNode(doc, "priv", spriv)
	    doc.endNode() # privs
        doc.endNode() # privivki

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

    card_priv = CARD_PRIV()

    card_id = PROF_EXAM_ID

    card_priv.initFromDB(dbc, card_id)

    sout = "card_id: {0}".format(card_id)
    asXML = card_priv.asXML()
    xmltxt = asXML.asText()
    sout = "Text len: {0}".format(len(xmltxt))
    log.info(sout)
    log.info(xmltxt)

    dbc.close()
    