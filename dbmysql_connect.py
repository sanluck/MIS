#!/usr/bin/env python
# encoding: utf8

import sys
import time
import exceptions
import logging

import MySQLdb

DBHOST = 'ct216.ctmed.ru'
DBUSER = 'root'
DBPASS = '18283848'
DBNAME = 'mis'
CHARSET = 'utf8'

log = logging.getLogger(__name__)

class DBMY:
    'Base class for DBMIS connection'
    
    def __init__(self, host=DBHOST, user=DBUSER, passwd=DBPASS, db=DBNAME):
        
        try:
            self.con = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset=CHARSET, use_unicode=True)
            self.cur = self.con.cursor()
        except:
            exctype, value = sys.exc_info()[:2]
            log.warn( 'DBMY init error: %s' % value.message )
            self.con.close()
            raise
        sout = "Connection to %s:%s has been established" % (DBHOST, DBNAME)
        log.debug(sout)
            
    def execute(self, ssql):
        try:
            curt = self.con.cursor()
            curt.execute(ssql)
            self.con.commit()
            return curt
        except:
            exctype, value = sys.exc_info()[:2]
            log.warn( 'DBMY execute error: %s' % value.message )
            self.con.rollback()
            self.con.close()
            raise
    
    def close(self):
        self.con.close()

if __name__ == "__main__":
    dbmy = DBMY()
    cursor = dbmy.con.cursor()
    ssql = "SELECT COUNT(*) FROM clinical_checkups"
    cursor.execute(ssql)
    result = cursor.fetchone()
    rcount = result[0]
    sout = "Table <clinical_checkups> has got {0} records".format(rcount)
    print sout
    dbmy.close()
 
