#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import logging
from medlib.modules.medobjects.SimpleXmlConstructor import SimpleXmlConstructor

from child_const import IDTYPE, IDCATEGORY, SMO_ID, SMO_ID0
from child_const import KLADR0

HOST      = "fb2.ctmed.ru"
DB        = "DBMIS"
CLINIC_ID = 268

PEOPLE_ID = 1403106

SQLT_P1 = """SELECT
lname, fname, mname,
birthday, sex,
document_type_id_fk, document_series, document_number,
insurance_certificate,
medical_insurance_series, medical_insurance_number,
insorg_id,
addr_jure_town_code, addr_jure_country_code
FROM peoples
WHERE people_id = ?;"""

if __name__ == "__main__":
    LOG_FILENAME = '_child.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)


def addNode(doc, nodeName, nodeValue):
    doc.startNode(nodeName)
    doc.putText(nodeValue)
    doc.endNode()

class ADDRESS:
    def __init__(self):
        self.kladrNP = None

class CHILD:
    def __init__(self):
        self.people_id = None
        self.lname = None
        self.fname = None
        self.mname = None
        self.birthday = None
        self.sex = None
        # XML tags 
        self.idCategory = None
        self.idDocument = None
        self.documentSer = None
        self.documentNum = None
        self.snils = None
        self.polisSer = None
        self.polisNum = None
        self.idInsuranceCompany = None
        self.medSanName = None
        self.medSanAddress = None
        self.address = None
        self.cards = None
        
    def initFromDB(self, dbc, people_id):
        cur = dbc.con.cursor()
        cur.execute(SQLT_P1, (people_id, ))
        rec = cur.fetchone()
        if rec is None:
            self.__init__()
        else:
            self.people_id = people_id
            self.lname = rec[0]
            self.fname = rec[1]
            self.mname = rec[2]
            self.birthday = rec[3]
            self.sex = rec[4]
            if rec[5] is None:
                self.idDocument = "14"
            else:
                self.idDocument = str(rec[5])
            if rec[6] is None:
                self.documentSer = ""
            else:
                self.documentSer = rec[6]
            if rec[7] is None:
                self.documentNum = ""
            else:
                self.documentNum = rec[7]
            if rec[8] is None:
                self.snils = ""
            else:
                self.snils = rec[8]

            self.polisSer = rec[9]
            self.polisNum = rec[10]
            
            insorg_id = rec[11]
            if insorg_id is None:
                self.idInsuranceCompany = SMO_ID0
            else:
                try:
                    self.idInsuranceCompany = SMO_ID[insorg_id]
                except:
                    self.idInsuranceCompany = SMO_ID0
            
            addr_jure_town_code = rec[12]
            addr_jure_country_code = rec[13]
            
            address = ADDRESS()
            if addr_jure_town_code is not None:
                address.kladrNP = str(addr_jure_town_code)
            elif addr_jure_country_code is not None:
                address.kladrNP = str(addr_jure_country_code)
            else:
                address.kladrNP = KLADR0
            self.address = address
            
    def asXML(self):
        doc = SimpleXmlConstructor()    
        idInternal = "{0}".format(self.people_id)
        addNode(doc, "idInternal", idInternal)
        addNode(doc, "idType", IDTYPE)
        
        doc.startNode("name")
        addNode(doc, "last", self.lname)
        addNode(doc, "first", self.fname)
        addNode(doc, "middle", self.mname)
        doc.endNode() # name

        if self.sex == u"М":
            idSex = "1"
        else:
            idSex = "2"
        addNode(doc, "idSex", idSex)
        
        bd = self.birthday
        dateOfBirth = "%04d-%02d-%02d" % (bd.year, bd.month, bd.day) 
        addNode(doc, "dateOfBirth", dateOfBirth)

        addNode(doc, "idCategory", IDCATEGORY)
        addNode(doc, "idDocument", self.idDocument)
        addNode(doc, "documentSer", self.documentSer)
        addNode(doc, "documentNum", self.documentNum)
        addNode(doc, "snils", self.snils)
        
        if self.polisSer is not None:
            addNode(doc, "polisSer", self.polisSer.encode('utf-8'))
        if self.polisNum is None:
            addNode(doc, "polisNum", "")
        else:
            addNode(doc, "polisNum", self.polisNum.encode('utf-8'))
        addNode(doc, "idInsuranceCompany", self.idInsuranceCompany)
        
        if self.medSanName is not None:
            addNode(doc, "medSanName", self.medSanName)
        
        if self.medSanAddress is not None:
            addNode(doc, "medSanAddress", self.medSanAddress)
        
        doc.startNode("address")
        if self.address is not None:
            address = self.address
            if address.kladrNP is not None:
                addNode(doc, "kladrNP", address.kladrNP)
            else:
                addNode(doc, "kladrNP", KLADR0)
        else:
            addNode(doc, "kladrNP", KLADR0)
        doc.endNode() # address

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

    child = CHILD()
    
    people_id = PEOPLE_ID
    
    child.initFromDB(dbc, people_id)
    child.medSanName = cname
    child.medSanAddress = caddr
    
    sout = "people_id: {0}".format(people_id)
    log.info(sout)
    childXML = child.asXML()
    log.info(childXML.asText())
    