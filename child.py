#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import logging
from medlib.modules.medobjects.SimpleXmlConstructor import SimpleXmlConstructor

HOST      = "fb2.ctmed.ru"
DB        = "DBMIS"
CLINIC_ID = 22

PEOPLE_ID = 1778741

SQLT_P1 = """SELECT
lname, fname, mname,
birthday, sex
FROM peoples
WHERE people_id = ?;"""

if __name__ == "__main__":
    LOG_FILENAME = '_child.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)


class ADDRRESS:
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
            self.lname = rec[0]
            self.fname = rec[1]
            self.mname = rec[2]
            self.birthday = rec[3]
            self.sex = rec[4]
            
    def asXML(self):
        doc = SimpleXmlConstructor()    
        doc.startNode("idInternal")
        idInternal = "{0}".format(self.people_id)
        doc.putText(idInternal)
        doc.endNode() # idInternal
        doc.startNode("idType")
        idType = "1"
        doc.putText(idType)
        doc.endNode() # idType
        
        doc.startNode("name")
        doc.startNode("last")
        doc.putText(self.lname)
        doc.endNode() # last
        doc.startNode("first")
        doc.putText(self.fname)
        doc.endNode() # first
        doc.startNode("middle")
        doc.putText(self.mname)
        doc.endNode() # middle
        doc.endNode() # name
        
        return doc


if __name__ == "__main__":
    from dbmis_connect2 import DBMIS
    
    sout = "Database: {0}:{1}".format(HOST, DB)
    log.info(sout)

    clinic_id = CLINIC_ID
    dbc = DBMIS(clinic_id, mis_host = HOST, mis_db = DB)

    cname = dbc.name.encode('utf-8')
    
    sout = "clinic_id: {0} clinic_name: {1}".format(clinic_id, cname)
    log.info(sout)
    

    child = CHILD()
    
    people_id = PEOPLE_ID
    
    child.initFromDB(dbc, people_id)
    
    sout = "people_id: {0}".format(people_id)
    log.info(sout)
    childXML = child.asXML()
    log.info(childXML.asText())
    