#!/usr/bin/env python
# encoding: utf8
#
#
# test1_get_refbooklist: get list of avaible refbooks
#                        insert into mis.cdmarf$refbooklist
#

import sys
import logging

import collections
from sets import Set

try:
    import cdmarf
except ImportError:
    sys.path.append("../..")
    import cdmarf

from cdmarf.cdmarf_soap import makeSOAPRequest
from cdmarf.classes.refbook import GetRefbookList
from cdmarf.classes.refitems import ReferenceList, ReferenceItem

from dbmysql_connect import DBMY


if __name__=="__main__":
    LOG_FILENAME = '_test1_getrefbooklist.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)


log = logging.getLogger(__name__)



#==========
def test_getrefbooklist():
    
    dbmy = DBMY()


    s_sqlt = """INSERT INTO
    cdmarf$refbooklist
    (code, name, description, table_name)
    VALUES
    ('{0}', '{1}', '{2}', '{3}');"""
    
    s_sqlft = """SELECT id
    FROM cdmarf$refbooklist
    WHERE code = '{0}';
    """

    
    cursor = dbmy.con.cursor()
    
    req = GetRefbookList()
    
    r = makeSOAPRequest(req)
    assert isinstance(r, ReferenceList)
    for elm in r.iter():
        #print elm.debugString()
        print "{0}-{1}".format(elm.code, elm.name)
        code        = elm.code
        name        = elm.name
        description = elm.description
        table_name  = elm.table_name
        
        s_sql = s_sqlft.format(code)
        cursor.execute(s_sql)
        rec = cursor.fetchone()
        
        if rec is None:
            s_sql = s_sqlt.format(code, name, description, table_name)
            cursor.execute(s_sql)
        
    dbmy.con.commit()
    dbmy.close()

if __name__ == '__main__':
    
    test_getrefbooklist()
