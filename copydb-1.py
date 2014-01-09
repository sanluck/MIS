#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# copydb-1.py - copy TABLE from DBMIS
#
#

import logging
import sys, codecs
from cStringIO import StringIO

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_copydb1.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

CLINIC_ID = 22
HOST = "fb2.ctmed.ru"
DB   = "DBMIS"

TABLE = "DATABASE_VERSION"

HOST2 = "fb2.ctmed.ru"
DB2   = "LBMIS"

class DB_REC:
    
    def __init__(self, rec):
        self.unit_name           = rec[0]
        self.version_number      = rec[1]
        self.workstation         = rec[2]
        self.version_string      = rec[3]
        self.version_string_type = rec[4]
        self.unit_context        = rec[5]

        if self.unit_context is None:
            unit_len = 0
        else:
            unit_len = len(self.unit_context)
        
        self.unit_len = unit_len
        
        sout = "unit_name: {0} len = {1}".format(self.unit_name.encode('utf-8'), unit_len)
        log.debug( sout )
    
    def insert2(self, cur):
        
        unit_name           = self.unit_name
        version_number      = self.version_number
        workstation         = self.workstation
        version_string      = self.version_string
        version_string_type = self.version_string_type
        unit_context        = self.unit_context
        
#        if workstation         is None: workstation = u'Null'
#        if version_string      is None: version_string = u'Null'
#        if version_string_type is None: version_string_type = u'Null'
        if unit_context        is None: unit_context = u'Null'
        
        s_sqlt = """INSERT INTO {0} (unit_name, version_number, workstation, version_string, version_string_type, unit_context) VALUES (?, ?, ?, ?, ?, ?)
        """
        
        s_sql = s_sqlt.format(TABLE)
        cur.execute(s_sql, (unit_name, version_number, workstation, version_string, version_string_type, StringIO(unit_context)))


if __name__ == "__main__":
    
    from dbmis_connect2 import DBMIS

    log.info("======================= Copy Table (1) =============================")
    
    try:
        dbc = DBMIS(CLINIC_ID, mis_host = HOST, mis_db = DB)
    except Exception, e:
        sout = "Can't open database {0}:{1}".format(HOST, DB)
        log.error(sout)
        sout = "{0}".format(e)
        log.error(sout)
        sys.exit(1)
        
    cur = dbc.con.cursor()
    
    s_sql = "SELECT COUNT(*) FROM {0}".format(TABLE)
    
    try:
        cur.execute(s_sql)
        rec = cur.fetchone()
    except Exception, e:
        sout = "Can't count records for table {0}".format(TABLE)
        log.error(sout)
        sout = "{0}".format(e)
        log.error(sout)
        sys.exit(1)
    
    sout = "Database {0}:{1}".format(HOST, DB)
    log.info( sout )
    sout = "Table {0} has got {1} records".format(TABLE, rec[0])
    log.info( sout )

    try:
        dbc2 = DBMIS(CLINIC_ID, mis_host = HOST2, mis_db = DB2)
    except Exception, e:
        sout = "Can't open database {0}:{1}".format(HOST2, DB2)
        log.error(sout)
        sout = "{0}".format(e)
        log.error(sout)
        sys.exit(1)
        
    cur2 = dbc2.con.cursor()

    s_sql2 = "DELETE FROM {0}".format(TABLE)
    cur2.execute(s_sql2)
    dbc2.con.commit()
    
    # Retrieval using the "file-like" methods of BlobReader:
    s_sql = "SELECT * FROM {0}".format(TABLE)
    cur.execute(s_sql)
    cur.set_stream_blob('A') # Note the capital letter
    
    results = cur.fetchall()
    
    for rec in results:
        
        db_rec = DB_REC(rec)
        
        unit_name = db_rec.unit_name
        unit_len  = db_rec.unit_len

        sout = "unit_name: {0} len = {1}".format(unit_name.encode('utf-8'), unit_len)
        log.info( sout )
        
        db_rec.insert2(cur2)
    
    
    dbc.close()
    dbc2.con.commit()
    dbc2.close()
    sys.exit(0)
