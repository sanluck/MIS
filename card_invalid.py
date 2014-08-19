#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# invalid
# card_invalid.py - PROF_EXAM_MINOR table
#                      INV - Инвалидность
#                      INV_TYPE_CODE - Тип инвалидности
#                      INV_DS - Диагноз установления инвалидности
#                      DATE_INV_FIRST - Дата установления инвалидности
#                      DATE_INV_LAST - Дата последнего освидетельствования
#                      ZAB_INV_LIST - Заболевания, обусловившее инвалидность
#                      VNZ_INV_LIST - Виды нарушения здоровья
#

import sys
import logging
from medlib.modules.medobjects.SimpleXmlConstructor import SimpleXmlConstructor

HOST      = "fb2.ctmed.ru"
DB        = "DBMIS"

CLINIC_ID = 124
PROF_EXAM_ID = 63029

SQLT_INV = """SELECT
inv_type_code, date_inv_first, date_inv_last, inv_ds, zab_inv_list, vnz_inv_list
FROM prof_exam_minor
WHERE prof_exam_id = ?;"""

if __name__ == "__main__":
    LOG_FILENAME = '_card_inv.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

INV_GROUP = {}
INV_GROUP_MAX = 33
INV_GROUP[33] = ['S00','T98']
INV_GROUP[32] = ['Q65','Q79']
INV_GROUP[31] = ['Q20','Q28']
INV_GROUP[30] = ['Q00','Q07']
INV_GROUP[29] = ['Q00','Q99']
INV_GROUP[28] = ['P00','P96']
INV_GROUP[27] = ['N00','N99']
INV_GROUP[26] = ['M00','M99']
INV_GROUP[25] = ['L00','L99']
INV_GROUP[24] = ['K00','K93']
INV_GROUP[23] = ['J46','J46']
INV_GROUP[22] = ['J45','J45']
INV_GROUP[21] = ['J00','J99']
INV_GROUP[20] = ['I00','I99']
INV_GROUP[19] = ['H60','H95']
INV_GROUP[18] = ['H60','H59']
INV_GROUP[17] = ['G80','G83']
INV_GROUP[16] = ['G00','G99']
INV_GROUP[15] = ['F70','F79']
INV_GROUP[14] = ['F00','F99']
INV_GROUP[13] = ['E10','E14']
INV_GROUP[10] = ['E00','E90']
INV_GROUP[9]  = ['B24','B24']
INV_GROUP[6]  = ['D50','D89']
INV_GROUP[5]  = ['C00','C48']
INV_GROUP[4]  = ['B20','B24']
INV_GROUP[3]  = ['A50','A53']
INV_GROUP[2]  = ['A15','A19']
INV_GROUP[1]  = ['A00','B99']

def addNode(doc, nodeName, nodeValue):
    doc.startNode(nodeName)
    doc.putText(nodeValue)
    doc.endNode()

def invGroupByDS(DS):
    DSS = DS[:3]
    for i in range(INV_GROUP_MAX, 0, -1):
	if INV_GROUP.has_key(i):
	    DS1 = INV_GROUP[i][0]
	    DS2 = INV_GROUP[i][1]
	    if (DSS >= DS1) and (DSS <= DS2): return str(i)

    return '0'

class CARD_INV:
    def __init__(self):
        self.idInternal     = None
        self.inv_type_code  = None
        self.date_inv_first = None
        self.date_inv_last  = None
        self.inv_ds         = None
        self.zab_inv_list   = None
        self.vnz_inv_list   = None

    def initFromDB(self, dbc, exam_id):
        cur = dbc.con.cursor()
        cur.execute(SQLT_INV, (exam_id, ))
        rec = cur.fetchone()
        if rec is None:
            self.__init__()
        else:
            self.idInternal     = exam_id
            self.inv_type_code  = rec[0]
            self.date_inv_first = rec[1]
            self.date_inv_last  = rec[2]
            self.inv_ds         = rec[3]
            self.zab_inv_list   = rec[4]
            self.vnz_inv_list   = rec[5]

    def asXML(self):
        doc = SimpleXmlConstructor()
        if (self.idInternal is None) or (self.inv_type_code is None) \
           or (self.date_inv_first is None) or (self.date_inv_last is None) \
           or (self.inv_ds is None) or (self.vnz_inv_list is None): return doc
        doc.startNode("invalid")
        addNode(doc, "type", str(self.inv_type_code))
        d1 = self.date_inv_first.strftime("%Y-%m-%d")
        addNode(doc, "dateFirstDetected", d1)
        d2 = self.date_inv_last.strftime("%Y-%m-%d")
        addNode(doc, "dateLastConfirmed", d2)
        doc.startNode("illnesses")
        illness = invGroupByDS(self.inv_ds)
        addNode(doc, "illness", illness)
        doc.endNode() # illnesses
        doc.startNode("defects")
        defects = self.vnz_inv_list
        for defect in defects.split(','):
            addNode(doc, "defect", defect)
        doc.endNode() # defects
        doc.endNode() # invalid

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

    card_invalid = CARD_INV()

    card_id = PROF_EXAM_ID

    card_invalid.initFromDB(dbc, card_id)

    sout = "card_id: {0}".format(card_id)
    asXML = card_invalid.asXML()
    xmltxt = asXML.asText()
    sout = "Text len: {0}".format(len(xmltxt))
    log.info(sout)
    log.info(xmltxt)

    dbc.close()