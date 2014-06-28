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

CLINIC_ID = 268
#PROF_EXAM_ID = 170
# PROF_EXAM_ID = 6185
PROF_EXAM_ID = 423140

SQLT_E1 = """SELECT
date_begin, type_exam_code, height, weight,
fr_code, nfr_code,
p_pf, p_mf, p_ecf, p_rr,
p_ps_code, p_i_code, p_evs_code,
f_p, f_ax, f_fa, f_ma, f_me,
mens1_code, mens2_code, mens3_code,
b_hr_code, b_pg_code,
inv, inv_type_code, inv_ds,
date_inv_first, date_inv_last,
vnz_inv_list,
health_group_code, phys_group_code,
date_end,
head_circ
FROM prof_exam_minor
WHERE prof_exam_id = ?;"""

SQLT_D1 = """SELECT
per.doctor_id_fk,
d.people_id_fk,
p.lname, p.fname, p.mname
FROM prof_exam_results per
LEFT JOIN doctors d ON per.doctor_id_fk = d.doctor_id
LEFT JOIN peoples p ON d.people_id_fk = p.people_id
WHERE per.prof_exam_id_fk = ?
AND per.cc_line = 1;"""

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

def getDoctor(dbc, exam_id):
    doc = SimpleXmlConstructor()
    doc.startNode("zakluchVrachName")
    cur = dbc.con.cursor()
    cur.execute(SQLT_D1, (exam_id, ))
    rec = cur.fetchone()
    
    if rec is None:
        addNode(doc, "last", "Иванов")
        addNode(doc, "first", "Иван")
        addNode(doc, "middle", "Иванович")
    else:
        last = rec[2]
        first = rec[3]
        middle = rec[4]
        if last is not None: addNode(doc, "last", last.encode('utf-8'))
        if first is not None: addNode(doc, "first", first.encode('utf-8'))
        if middle is not None: addNode(doc, "middle", middle.encode('utf-8'))

    doc.endNode() # zakluchVrachName
    
    return doc
    
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

        self.inv = None
        self.inv_type_code = None
        self.inv_ds = None
        self.date_inv_first = None
        self.date_inv_last = None
        self.vnz_inv_list = None
        
        self.health_group_code = None
        self.phys_group_code = None
        
        self.date_end = None
	
        self.head_circ = None
	
	self.age = None
        
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
            
            self.inv = rec[23]
            self.inv_type_code = rec[24]
            self.inv_ds = rec[25]
            self.date_inv_first = rec[26]
            self.date_inv_last = rec[27]
            self.vnz_inv_list = rec[28]

            self.health_group_code = rec[29]
            self.phys_group_code = rec[30]

            self.date_end = rec[31]
	    
            self.head_circ = rec[32]
	    
            
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
        addNode(doc, "weight", str(weight)+".000")
	
	headSize = self.head_circ
	
	if (headSize is not None) and (headSize > 0):
	    addNode(doc, "headSize", str(headSize))
        
        nfr_code = self.nfr_code
        if nfr_code is not None:
            doc.startNode("healthProblems")
            addNode(doc, "problem", str(nfr_code))
            doc.endNode()
        
        age = self.age
	
        if age is not None:
	    if (age < 5):
		doc.startNode("pshycDevelopment")
		p_pf = self.p_pf
		if p_pf is None: p_pf = 0
		addNode(doc, "poznav", str(p_pf))
		p_mf = self.p_mf
		if p_mf is None: p_mf = 0
		addNode(doc, "motor", str(p_mf))
		p_ecf = self.p_ecf
		if p_ecf is None: p_ecf = 0
		addNode(doc, "emot", str(p_ecf))
		p_rr = self.p_rr
		if p_rr is None: p_rr = 0
		addNode(doc, "rech", str(p_rr))
		doc.endNode() # pshycDevelopment
	    if (age >= 5):
		doc.startNode("pshycState")
		p_ps_code = self.p_ps_code
		if p_ps_code is None: p_ps_code = 1
		addNode(doc, "psihmot", str(p_ps_code))
		p_i_code = self.p_i_code
		if p_i_code is None: p_i_code = 1
		addNode(doc, "intel", str(p_i_code))
		p_evs_code = self.p_evs_code
		if p_evs_code is None: p_evs_code = 1
		addNode(doc, "emotveg", str(p_evs_code))
		doc.endNode() # pshycState
	elif (self.p_pf is not None) and (self.p_pf > 0):
            doc.startNode("pshycDevelopment")
            addNode(doc, "poznav", str(self.p_pf))
            p_mf = self.p_mf
            if p_mf is None: p_mf = 0
            addNode(doc, "motor", str(p_mf))
            p_ecf = self.p_ecf
            if p_ecf is None: p_ecf = 0
            addNode(doc, "emot", str(p_ecf))
	    p_rr = self.p_rr
	    if p_rr is None: p_rr = 0
            addNode(doc, "rech", str(p_rr))
            doc.endNode() # pshycDevelopment
	elif (self.p_ps_code is not None) and (self.p_ps_code > 0):
            doc.startNode("pshycState")
            addNode(doc, "psihmot", str(self.p_ps_code))
            p_i_code = self.p_i_code
            if p_i_code is None: p_i_code = 1
            addNode(doc, "intel", str(p_i_code))
            p_evs_code = self.p_evs_code
            if p_evs_code is None: p_evs_code = 1
            addNode(doc, "emotveg", str(p_evs_code))
            doc.endNode() # pshycState

        if self.f_fa is not None:
            doc.startNode("sexFormulaMale")
            addNode(doc, "P", str(self.f_p))
            addNode(doc, "Ax", str(self.f_ax))
            addNode(doc, "Fa", str(self.f_fa))
            doc.endNode() # sexFormulaMale
        elif self.f_p is not None:
            doc.startNode("sexFormulaFemale")
            addNode(doc, "P", str(self.f_p))
            addNode(doc, "Ma", str(self.f_ma))
            addNode(doc, "Ax", str(self.f_ax))
            f_me = self.f_me
            if f_me is None: f_me = 0
            addNode(doc, "Me", str(f_me))
            doc.endNode() # sexFormulaFemale
            
            mens1_code = self.mens1_code
            if (f_me > 0) and (mens1_code is not None) and (mens1_code > 0):
                doc.startNode("menses")
                addNode(doc, "menarhe", "150") # ???
                doc.startNode("characters")
                if mens1_code is None: 
		    mens1_code = 1
		elif mens1_code not in (1,2):
		    mens1_code = 1
                addNode(doc, "char", str(mens1_code))
                mens2_code = self.mens2_code
                if mens2_code is None: 
		    mens2_code = 5
		else:
		    mens2_code += 2
		    if mens2_code not in (3,4,5): 
			mens2_code = 5
		    elif mens2_code == 4:
			mens2_code = 5
		    elif mens2_code == 5:
			mens2_code = 4
                addNode(doc, "char", str(mens2_code))
                mens3_code = self.mens3_code
                if mens3_code is None: 
		    mens3_code = 7
		else:
		    mens3_code += 5
		    if mens3_code not in (6,7): mens2_code = 7
                addNode(doc, "char", str(mens3_code))
                doc.endNode() # characters
                doc.endNode() # menses
        
        b_hr_code = self.b_hr_code
        if b_hr_code not in (1,2,3,4,5): b_hr_code = 1     
        addNode(doc, "healthGroupBefore", str(b_hr_code))
	b_pg_code = self.b_pg_code
	if b_pg_code == 5:
	    b_pg_code = -1
	elif b_pg_code not in (1,2,3,4):
	    b_pg_code = 1
        addNode(doc, "fizkultGroupBefore", str(b_pg_code))

        return doc
    
    def z_asXML(self):
	
        doc = SimpleXmlConstructor()

	health_group_code = self.health_group_code
	phys_group_code = self.phys_group_code

	de = self.date_end
	
	if health_group_code is None: 
	    health_group_code = 1
	elif health_group_code not in (1,2,3,4,5):
	    health_group_code = 1
	addNode(doc, "healthGroup", str(health_group_code))
	
	if phys_group_code is None: 
	    phys_group_code = 1
	elif phys_group_code == 5:
	    phys_group_code = -1
	elif phys_group_code not in (1,2,3,4):
	    phys_group_code = 1
	addNode(doc, "fizkultGroup", str(phys_group_code))
	
	if de is None:
	    zakluchDate = "2014-06-30"
	else:
	    zakluchDate = "%04d-%02d-%02d" % (de.year, de.month, de.day)
	    
	addNode(doc, "zakluchDate", zakluchDate)
	    
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
    
    z_XML = card.z_asXML()
    log.info(z_XML.asText())
    
    d_XML = getDoctor(dbc, card_id)
    log.info(d_XML.asText())
    