#!/usr/bin/env python
# encoding: utf8

from insorginfoitem import InsorgInfoItem
from medlib.exceptions import MedLibException

class InsorgNotFoundException(MedLibException):
    pass

def singleton(cls):
    instances = {}
    def getinstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]
    return getinstance

@singleton
class InsorgInfoList:
    def __init__(self):

        m = []

        # -- DB BEGIN --
        
        # Insorg code, Insorg name, INN, OGRN, OKATO

        m.append( [0, u'', None, None, None] )
        
        m.append( [1, u'АЛТАЙСКИЙ ФИЛИАЛ ООО "СТРАХОВАЯ МЕДИЦИНСКАЯ КОМПАНИЯ РЕСО-МЕД"', 5035000265, 1025004642519, None] )
        m.append( [2, u'ФИЛИАЛ ЗАО "КАПИТАЛЪ МЕДИЦИНСКОЕ СТРАХОВАНИЕ" В Г.БАРНАУЛЕ"', 0, None, None] )
        m.append( [3, u'ФИЛИАЛ "АЛТАЙСКИЙ" ЗАО "СТРАХОВАЯ ГРУППА СПАССКИЕ ВОРОТА-М"', 7717044533,  1027739449913, None] )
        m.append( [4, u'БАРНАУЛЬСКИЙ ФИЛИАЛ ОАО СМО "СИБИРЬ"', 4205080364,  1054205020682, None] )
        m.append( [5, u'ФИЛИАЛ ЗАО МЕДИЦИНСКАЯ СТРАХОВАЯ КОМПАНИЯ "СОЛИДАРНОСТЬ ДЛЯ ЖИЗНИ" В А', 7704103750,  1027739815245, None] )
        m.append( [6, u'ФИЛИАЛ ООО "РГС-МЕДИЦИНА" - "РОСГОССТРАХ-АЛТАЙ-МЕДИЦИНА"', 7813171100,  1027806865481, None] )
        m.append( [7, u'ООО СТРАХОВАЯ МЕДИЦИНСКАЯ КОМПАНИЯ "СЕДАР-М"', 2221065779,  1042201925083, 01401363000] )
        m.append( [8, u'ООО СТРАХОВАЯ КОМПАНИЯ "ИНТЕРМЕДСЕРВИС-СИБИРЬ"', 2224052640,  1022201534134, 01401367000] )
        # -- DB END --
        
        self.idxByCode = {}
        self.idxByINN  = {}
        for elm in m:
            item = InsorgInfoItem( elm[0], elm[1], elm[2], elm[3], elm[4] )
            self.idxByCode[ elm[0] ] = item
            self.idxByINN[ elm[2] ] = item
            
    def insorgInfoByCode(self, code):
        if not code in self.idxByCode:
            raise InsorgNotFoundException( "code={0}".format(code) )
        
        return self.idxByCode[code]
    
    def insorgCodeByMisId(self, mis_lpu_id):
        if not mis_lpu_id in self.idxByMisId:
            raise MoNotFoundException("mis_lpu_id={0}".format(mis_lpu_id))
        
        return self.idxByMisId[mis_lpu_id].code
    
    def __getitem__(self, code):
        return self.insorgInfoByCode( code )
    
    def isCodePresent(self, code):
        return code in self.idxByCode


    
def testInsorgInfoList():
    db1 = InsorgInfoList()
    
    insorg = db1[8]
    print insorg.name, insorg.ogrn
    
    
if __name__=="__main__":
    testInsorgInfoList()
    
    