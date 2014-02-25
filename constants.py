#!/usr/bin/env python
# encoding: utf8

import logging

log = logging.getLogger(__name__)

GENDER_MALE = "M"
GENDER_FEMALE = "F"

class MedLibException(Exception):
    pass

class MoNotFoundException(MedLibException):
    pass

class MGZ_Item:
    def __init__(self, code, name):
        self.code = code
        self.name = name
        
    def __unicode__(self):
        return u"MGZ({0},{1})".format( self.code, self.name)
    
class MGZ_List:
    def __init__(self):

        m = []

        # -- DB BEGIN --
        # MGZ code, MGZ name

        m.append( [0, u'Барнаульская'] )
        m.append( [1, u'Рубцовская'] )
        m.append( [2, u'Каменская'] )
        m.append( [3, u'Заринская'] )
        m.append( [4, u'Славгородская'] )
        m.append( [5, u'Алейская'] )
        m.append( [6, u'Бийская'] )

        # -- DB END --
        
        self.idxByCode = {}
        for elm in m:
            item = MGZ_Item( elm[0], elm[1] )
            self.idxByCode[ elm[0] ] = item

    def mgzInfoByCode(self, code):
        if not code in self.idxByCode:
            raise MoNotFoundException( "code={0}".format(code) )
        
        return self.idxByCode[code]
 

    def __getitem__(self, code):
        return self.mgzInfoByCode( code )

def testMGZ_List():
    db1 = MGZ_List()
    
    mgz = db1[3]
    print mgz.name

class CMGZ_Item:
    # принадлежность клиник к МГЗ
    def __init__(self, clinic_id, mgz_code, clinic_name = None):
        self.clinic_id = clinic_id
	self.clinic_name = clinic_name
        self.mgz_code = mgz_code

class CMGZ_List:
    def __init__(self):

        m = []

        # -- DB BEGIN --
        # clinci_id, mgz_code

        # Барнаульская зона
        m.append( [51,  0, 'ГБ № 1'] ) # ГБ № 1
        m.append( [222, 0, 'ГБ № 3'] ) # ГБ № 3
        m.append( [229, 0, 'ГБ № 4'] ) # ГБ № 4
        m.append( [215, 0, 'ГБ № 5'] ) # ГБ № 5
        m.append( [227, 0, 'ГБ № 6'] ) # ГБ № 6
        m.append( [228, 0, 'ГБ № 8'] ) # ГБ № 8
        m.append( [224, 0, 'ГБ № 9'] ) # ГБ № 9
        m.append( [223, 0, 'ГБ № 10'] )# ГБ № 10
        m.append( [225, 0, 'ГБ № 12'] )# ГБ № 12
        

        # -- DB END --
        
        self.idxByCode = {}
	self.idxByNumber = []
        for elm in m:
            item = CMGZ_Item( elm[0], elm[1] )
            self.idxByCode[ elm[0] ] = item
            self.idxByNumber.append(item)

    def mgzInfoByCode(self, code):
        if not code in self.idxByCode:
            raise MoNotFoundException( "code={0}".format(code) )
        
        return self.idxByCode[code]
 

    def __getitem__(self, code):
        return self.mgzInfoByCode( code )


    def get_from_xlsx(self, fname = 'cmgz.xlsx'):
        import xlrd
        
        workbook = xlrd.open_workbook(fname)
        worksheets = workbook.sheet_names()
        
        m = []
        zn = -1
        for wsh in worksheets:
            zn += 1
            worksheet = workbook.sheet_by_name(wsh)
            curr_row = 0
            num_rows = worksheet.nrows - 1
            num_clinics = 0
            while curr_row < num_rows:
                curr_row += 1
                # Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
                c1_type = worksheet.cell_type(curr_row, 0)
                if c1_type != 2: continue
                clinic_id = int(worksheet.cell_value(curr_row, 0))
                c2_type = worksheet.cell_type(curr_row, 1)
		if c2_type == 1:
			clinic_name = worksheet.cell_value(curr_row, 1)
		else:
			clinic_name = None
                m.append( [clinic_id, zn, clinic_name] )
                num_clinics += 1
            
            sout = "MGZ: {0} Clinics Number: {1}".format(wsh.encode('utf-8'), num_clinics)
            log.info( sout )
        
        self.idxByCode = {}
        self.idxByNumber = []
	
        for elm in m:
            item = CMGZ_Item( elm[0], elm[1], elm[2])
            self.idxByCode[ elm[0] ] = item
            self.idxByNumber.append(item)
        

def testCMGZ_List():
    db2 = CMGZ_List()
    db2.get_from_xlsx('cmgz.xlsx')
    
    cmgz = db2[222]
    print cmgz.clinic_id, cmgz.clinic_name, cmgz.mgz_code
    
    n_lpu = len(db2.idxByNumber)
    n_bmgz = 0
    for cmgz in db2.idxByNumber:
	clinic_id = cmgz.clinic_id
	mgz_code  = cmgz.mgz_code
	if mgz_code == 0: n_bmgz += 1
    
    sout = "MGZ 0 has got {0} clinics from {1}".format(n_bmgz, n_lpu)
    print sout
	    
    
if __name__=="__main__":
    LOG_FILENAME = '_conctants.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)
    
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)    

    testMGZ_List()
    testCMGZ_List()
