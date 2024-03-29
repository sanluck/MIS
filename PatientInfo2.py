#!/usr/bin/env python
# encoding: utf8

import constants

#
# Код сгенерирован с помощью fldsconv.py 
#

class PatientInfo2:
       
    def __init__(self):
        ## Autogenerated ON
        self.people_id = None
        self.lname = None
        self.fname = None
        self.mname = None
        self.sex = None
        self.document_type_id_fk = None
        self.document_type_name = None
        self.document_series = None
        self.document_number = None
        self.phone = None
        self.birthday = None
        self.inn = None
        self.insurance_certificate = None
        self.medical_insurance_series = None
        self.medical_insurance_number = None
        self.medical_insurance_region_id_fk = None
        self.medical_insurance_region_name = None
        self.medical_insurance_company_name = None
        self.settlement_type_id_fk = None
        self.settlement_type_name = None
        self.social_status_id_fk = None
        self.social_status_name = None
        self.branch_id_fk = None
        self.branch_name = None
        self.work_place = None
        self.addr_jure_region_code = None
        self.addr_jure_region_name = None
        self.addr_jure_town_code = None
        self.addr_jure_town_name = None
        self.addr_jure_town_socr = None
        self.addr_jure_town_correct = None
        self.addr_jure_area_code = None
        self.addr_jure_area_name = None
        self.addr_jure_area_socr = None
        self.addr_jure_area_correct = None
        self.addr_jure_country_code = None
        self.addr_jure_country_name = None
        self.addr_jure_country_socr = None
        self.addr_jure_country_correct = None
        self.addr_jure_street_code = None
        self.addr_jure_street_name = None
        self.addr_jure_street_socr = None
        self.addr_jure_street_correct = None
        self.addr_jure_house = None
        self.addr_jure_corps = None
        self.addr_jure_flat = None
        self.addr_fact_region_code = None
        self.addr_fact_region_name = None
        self.addr_fact_town_code = None
        self.addr_fact_town_name = None
        self.addr_fact_town_socr = None
        self.addr_fact_town_correct = None
        self.addr_fact_area_code = None
        self.addr_fact_area_name = None
        self.addr_fact_area_socr = None
        self.addr_fact_area_correct = None
        self.addr_fact_country_code = None
        self.addr_fact_country_name = None
        self.addr_fact_country_socr = None
        self.addr_fact_country_correct = None
        self.addr_fact_street_code = None
        self.addr_fact_street_name = None
        self.addr_fact_street_socr = None
        self.addr_fact_street_correct = None
        self.addr_fact_house = None
        self.addr_fact_corps = None
        self.addr_fact_flat = None
        self.birthplace = None
        self.medical_card_series = None
        self.medical_card_number = None
        self.tfoms_code_id_fk = None
        self.tfoms_name = None
        self.addr_fact_full = None
        self.document_who = None
        self.document_when = None
        self.is_new_policy = None
        self.work_code_id = None
        self.insorg_id = None
        self.social_status_unemployed_id_fk = None
        self.social_status_unemployed_name = None
        self.social_status_front_card = None
        self.last_flgr = None
        self.territory_id_fk = None
        self.special_case = None
        self.delegate_status_id_fk = None
        self.delegate_lname = None
        self.delegate_fname = None
        self.delegate_mname = None
        self.insorg_ogrn = None
        self.citizenship = None
        self.policy_begin = None
        self.policy_end = None
        self.streets_ul_id = None
        self.settlement_types_np_id = None
        self.private_consent = None
        self.blood_group = None
        self.clinic_contract_id_fk = None
        self.p_payment_type_id_fk = None
        self.double_person = None
        self.mobile_phone = None
        ## Autogenerated OFF
        self.clinic_id = None
        self.area_number = None
        self.area_people_id = None
 
    def initFromRec(self, rec):
        #
        ## Autogenerated ON
        self.people_id = rec[0]
        self.lname = rec[1]
        self.fname = rec[2]
        self.mname = rec[3]
        self.sex = rec[4]
        self.document_type_id_fk = rec[5]
        self.document_series = rec[6]
        self.document_number = rec[7]
        self.birthday = rec[8]
        self.inn = rec[9]
        self.insurance_certificate = rec[10]
        self.medical_insurance_series = rec[11]
        self.medical_insurance_number = rec[12]
        self.insorg_id = rec[13]
        self.phone = rec[14]
        self.mobile_phone = rec[15]
        self.birthplace = rec[16]
        ## Autogenerated OFF
        
    def initFromDb(self, db, patientId):
        SQL_TEMPLATE_PEOPLE = """SELECT
        people_id,
        lname,
        fname,
        mname,
        sex,
        document_type_id_fk,
        document_series,
        document_number,
        birthday,
        inn,
        insurance_certificate,
        medical_insurance_series,
        medical_insurance_number,
        insorg_id,
        phone,
        mobile_phone,
        birthplace
        FROM PEOPLES WHERE PEOPLE_ID = {0};"""        
        ssql = SQL_TEMPLATE_PEOPLE.format(patientId)
        db.execute(ssql)
        rec = db.cur.fetchone()
        if rec <> None: self.initFromRec(rec)
        
        SQL_TEMPLATE_AP = """SELECT 
ap.area_people_id, ap.area_id_fk,
a.clinic_area_id_fk, a.area_number,
ca.clinic_id_fk
FROM area_peoples ap 
LEFT JOIN areas a ON ap.area_id_fk = a.area_id
LEFT JOIN clinic_areas ca ON a.clinic_area_id_fk = ca.clinic_area_id
WHERE ap.people_id_fk = {0} AND ca.basic_speciality = 1 AND  ap.date_end is Null;"""
        ssql = SQL_TEMPLATE_AP.format(patientId)
        db.execute(ssql)
        rec = db.cur.fetchone()
        if rec <> None:
            self.clinic_id = rec[4]
            self.area_number = rec[3]
            self.area_people_id = rec[0]

    def initFromCur(self, cur, patientId):
        SQL_TEMPLATE_PEOPLE = """SELECT
        people_id,
        lname,
        fname,
        mname,
        sex,
        document_type_id_fk,
        document_series,
        document_number,
        birthday,
        inn,
        insurance_certificate,
        medical_insurance_series,
        medical_insurance_number,
        insorg_id,
        phone,
        mobile_phone,
        birthplace
        FROM PEOPLES WHERE PEOPLE_ID = {0};"""        
        ssql = SQL_TEMPLATE_PEOPLE.format(patientId)
        cur.execute(ssql)
        rec = cur.fetchone()
        if rec <> None: self.initFromRec(rec)
        
        SQL_TEMPLATE_AP = """SELECT 
ap.area_people_id, ap.area_id_fk,
a.clinic_area_id_fk, a.area_number,
ca.clinic_id_fk
FROM area_peoples ap 
LEFT JOIN areas a ON ap.area_id_fk = a.area_id
LEFT JOIN clinic_areas ca ON a.clinic_area_id_fk = ca.clinic_area_id
WHERE ap.people_id_fk = {0} AND ca.basic_speciality = 1 AND  ap.date_end is Null;"""
        ssql = SQL_TEMPLATE_AP.format(patientId)
        cur.execute(ssql)
        rec = cur.fetchone()
        if rec <> None:
            self.clinic_id = rec[4]
            self.area_number = rec[3]
            self.area_people_id = rec[0]
        

    def asString(self):
        ## Autogenerated ON
        res = []
        if self.people_id : res.append( u"people_id={0}".format(self.people_id) )
        if self.lname : res.append( u"lname={0}".format(self.lname) )
        if self.fname : res.append( u"fname={0}".format(self.fname) )
        if self.mname : res.append( u"mname={0}".format(self.mname) )
        if self.sex : res.append( u"sex={0}".format(self.sex) )
        if self.document_type_id_fk : res.append( u"document_type_id_fk={0}".format(self.document_type_id_fk) )
        if self.document_type_name : res.append( u"document_type_name={0}".format(self.document_type_name) )
        if self.document_series : res.append( u"document_series={0}".format(self.document_series) )
        if self.document_number : res.append( u"document_number={0}".format(self.document_number) )
        if self.phone : res.append( u"phone={0}".format(self.phone) )
        if self.birthday : res.append( u"birthday={0}".format(self.birthday) )
        if self.inn : res.append( u"inn={0}".format(self.inn) )
        if self.insurance_certificate : res.append( u"insurance_certificate={0}".format(self.insurance_certificate) )
        if self.medical_insurance_series : res.append( u"medical_insurance_series={0}".format(self.medical_insurance_series) )
        if self.medical_insurance_number : res.append( u"medical_insurance_number={0}".format(self.medical_insurance_number) )
        if self.medical_insurance_region_id_fk : res.append( u"medical_insurance_region_id_fk={0}".format(self.medical_insurance_region_id_fk) )
        if self.medical_insurance_region_name : res.append( u"medical_insurance_region_name={0}".format(self.medical_insurance_region_name) )
        if self.medical_insurance_company_name : res.append( u"medical_insurance_company_name={0}".format(self.medical_insurance_company_name) )
        if self.settlement_type_id_fk : res.append( u"settlement_type_id_fk={0}".format(self.settlement_type_id_fk) )
        if self.settlement_type_name : res.append( u"settlement_type_name={0}".format(self.settlement_type_name) )
        if self.social_status_id_fk : res.append( u"social_status_id_fk={0}".format(self.social_status_id_fk) )
        if self.social_status_name : res.append( u"social_status_name={0}".format(self.social_status_name) )
        if self.branch_id_fk : res.append( u"branch_id_fk={0}".format(self.branch_id_fk) )
        if self.branch_name : res.append( u"branch_name={0}".format(self.branch_name) )
        if self.work_place : res.append( u"work_place={0}".format(self.work_place) )
        if self.addr_jure_region_code : res.append( u"addr_jure_region_code={0}".format(self.addr_jure_region_code) )
        if self.addr_jure_region_name : res.append( u"addr_jure_region_name={0}".format(self.addr_jure_region_name) )
        if self.addr_jure_town_code : res.append( u"addr_jure_town_code={0}".format(self.addr_jure_town_code) )
        if self.addr_jure_town_name : res.append( u"addr_jure_town_name={0}".format(self.addr_jure_town_name) )
        if self.addr_jure_town_socr : res.append( u"addr_jure_town_socr={0}".format(self.addr_jure_town_socr) )
        if self.addr_jure_town_correct : res.append( u"addr_jure_town_correct={0}".format(self.addr_jure_town_correct) )
        if self.addr_jure_area_code : res.append( u"addr_jure_area_code={0}".format(self.addr_jure_area_code) )
        if self.addr_jure_area_name : res.append( u"addr_jure_area_name={0}".format(self.addr_jure_area_name) )
        if self.addr_jure_area_socr : res.append( u"addr_jure_area_socr={0}".format(self.addr_jure_area_socr) )
        if self.addr_jure_area_correct : res.append( u"addr_jure_area_correct={0}".format(self.addr_jure_area_correct) )
        if self.addr_jure_country_code : res.append( u"addr_jure_country_code={0}".format(self.addr_jure_country_code) )
        if self.addr_jure_country_name : res.append( u"addr_jure_country_name={0}".format(self.addr_jure_country_name) )
        if self.addr_jure_country_socr : res.append( u"addr_jure_country_socr={0}".format(self.addr_jure_country_socr) )
        if self.addr_jure_country_correct : res.append( u"addr_jure_country_correct={0}".format(self.addr_jure_country_correct) )
        if self.addr_jure_street_code : res.append( u"addr_jure_street_code={0}".format(self.addr_jure_street_code) )
        if self.addr_jure_street_name : res.append( u"addr_jure_street_name={0}".format(self.addr_jure_street_name) )
        if self.addr_jure_street_socr : res.append( u"addr_jure_street_socr={0}".format(self.addr_jure_street_socr) )
        if self.addr_jure_street_correct : res.append( u"addr_jure_street_correct={0}".format(self.addr_jure_street_correct) )
        if self.addr_jure_house : res.append( u"addr_jure_house={0}".format(self.addr_jure_house) )
        if self.addr_jure_corps : res.append( u"addr_jure_corps={0}".format(self.addr_jure_corps) )
        if self.addr_jure_flat : res.append( u"addr_jure_flat={0}".format(self.addr_jure_flat) )
        if self.addr_fact_region_code : res.append( u"addr_fact_region_code={0}".format(self.addr_fact_region_code) )
        if self.addr_fact_region_name : res.append( u"addr_fact_region_name={0}".format(self.addr_fact_region_name) )
        if self.addr_fact_town_code : res.append( u"addr_fact_town_code={0}".format(self.addr_fact_town_code) )
        if self.addr_fact_town_name : res.append( u"addr_fact_town_name={0}".format(self.addr_fact_town_name) )
        if self.addr_fact_town_socr : res.append( u"addr_fact_town_socr={0}".format(self.addr_fact_town_socr) )
        if self.addr_fact_town_correct : res.append( u"addr_fact_town_correct={0}".format(self.addr_fact_town_correct) )
        if self.addr_fact_area_code : res.append( u"addr_fact_area_code={0}".format(self.addr_fact_area_code) )
        if self.addr_fact_area_name : res.append( u"addr_fact_area_name={0}".format(self.addr_fact_area_name) )
        if self.addr_fact_area_socr : res.append( u"addr_fact_area_socr={0}".format(self.addr_fact_area_socr) )
        if self.addr_fact_area_correct : res.append( u"addr_fact_area_correct={0}".format(self.addr_fact_area_correct) )
        if self.addr_fact_country_code : res.append( u"addr_fact_country_code={0}".format(self.addr_fact_country_code) )
        if self.addr_fact_country_name : res.append( u"addr_fact_country_name={0}".format(self.addr_fact_country_name) )
        if self.addr_fact_country_socr : res.append( u"addr_fact_country_socr={0}".format(self.addr_fact_country_socr) )
        if self.addr_fact_country_correct : res.append( u"addr_fact_country_correct={0}".format(self.addr_fact_country_correct) )
        if self.addr_fact_street_code : res.append( u"addr_fact_street_code={0}".format(self.addr_fact_street_code) )
        if self.addr_fact_street_name : res.append( u"addr_fact_street_name={0}".format(self.addr_fact_street_name) )
        if self.addr_fact_street_socr : res.append( u"addr_fact_street_socr={0}".format(self.addr_fact_street_socr) )
        if self.addr_fact_street_correct : res.append( u"addr_fact_street_correct={0}".format(self.addr_fact_street_correct) )
        if self.addr_fact_house : res.append( u"addr_fact_house={0}".format(self.addr_fact_house) )
        if self.addr_fact_corps : res.append( u"addr_fact_corps={0}".format(self.addr_fact_corps) )
        if self.addr_fact_flat : res.append( u"addr_fact_flat={0}".format(self.addr_fact_flat) )
        if self.birthplace : res.append( u"birthplace={0}".format(self.birthplace) )
        if self.medical_card_series : res.append( u"medical_card_series={0}".format(self.medical_card_series) )
        if self.medical_card_number : res.append( u"medical_card_number={0}".format(self.medical_card_number) )
        if self.tfoms_code_id_fk : res.append( u"tfoms_code_id_fk={0}".format(self.tfoms_code_id_fk) )
        if self.tfoms_name : res.append( u"tfoms_name={0}".format(self.tfoms_name) )
        if self.addr_fact_full : res.append( u"addr_fact_full={0}".format(self.addr_fact_full) )
        if self.document_who : res.append( u"document_who={0}".format(self.document_who) )
        if self.document_when : res.append( u"document_when={0}".format(self.document_when) )
        if self.is_new_policy : res.append( u"is_new_policy={0}".format(self.is_new_policy) )
        if self.work_code_id : res.append( u"work_code_id={0}".format(self.work_code_id) )
        if self.insorg_id : res.append( u"insorg_id={0}".format(self.insorg_id) )
        if self.social_status_unemployed_id_fk : res.append( u"social_status_unemployed_id_fk={0}".format(self.social_status_unemployed_id_fk) )
        if self.social_status_unemployed_name : res.append( u"social_status_unemployed_name={0}".format(self.social_status_unemployed_name) )
        if self.social_status_front_card : res.append( u"social_status_front_card={0}".format(self.social_status_front_card) )
        if self.last_flgr : res.append( u"last_flgr={0}".format(self.last_flgr) )
        if self.territory_id_fk : res.append( u"territory_id_fk={0}".format(self.territory_id_fk) )
        if self.special_case : res.append( u"special_case={0}".format(self.special_case) )
        if self.delegate_status_id_fk : res.append( u"delegate_status_id_fk={0}".format(self.delegate_status_id_fk) )
        if self.delegate_lname : res.append( u"delegate_lname={0}".format(self.delegate_lname) )
        if self.delegate_fname : res.append( u"delegate_fname={0}".format(self.delegate_fname) )
        if self.delegate_mname : res.append( u"delegate_mname={0}".format(self.delegate_mname) )
        if self.insorg_ogrn : res.append( u"insorg_ogrn={0}".format(self.insorg_ogrn) )
        if self.citizenship : res.append( u"citizenship={0}".format(self.citizenship) )
        if self.policy_begin : res.append( u"policy_begin={0}".format(self.policy_begin) )
        if self.policy_end : res.append( u"policy_end={0}".format(self.policy_end) )
        if self.streets_ul_id : res.append( u"streets_ul_id={0}".format(self.streets_ul_id) )
        if self.settlement_types_np_id : res.append( u"settlement_types_np_id={0}".format(self.settlement_types_np_id) )
        if self.private_consent : res.append( u"private_consent={0}".format(self.private_consent) )
        if self.blood_group : res.append( u"blood_group={0}".format(self.blood_group) )
        if self.clinic_contract_id_fk : res.append( u"clinic_contract_id_fk={0}".format(self.clinic_contract_id_fk) )
        if self.p_payment_type_id_fk : res.append( u"p_payment_type_id_fk={0}".format(self.p_payment_type_id_fk) )
        if self.double_person : res.append( u"double_person={0}".format(self.double_person) )
        if self.mobile_phone : res.append( u"mobile_phone={0}".format(self.mobile_phone) )
        return "\n".join(res)
        ## Autogenerated OFF
    
    def getGender(self):
        if self.sex == u'Ж':
            return constants.GENDER_FEMALE
        if self.sex == u'М':
            return constants.GENDER_MALE
        # TODO: ???
        return constants.GENDER_MALE
    
    def patientNameAsArray(self):
        return [ self.lname, self.fname, self.mname ]
    
    def patientId(self):
        return str(self.insurance_certificate)

    def patientAddressAsArray(self):
        res = []
        
        street = []
        if self.addr_jure_street_name:
            street.append( u"&{0}".format(self.addr_jure_street_name) )
        else:
            street.append( u"&" )

        if self.addr_jure_house:
            street.append( u"&{0}".format(self.addr_jure_house) )
        else:
            street.append( u"&" )
        
        if self.addr_jure_corps:
            street.append( u"{0}".format(self.addr_jure_corps) )
            
        if self.addr_jure_flat:
            street.append( u",кв.{0}".format(self.addr_jure_flat) )
            
        res.append( "".join(street) )


        if self.addr_jure_area_name and self.addr_jure_area_socr:
            res.append( u"{0} {1}".format(self.addr_jure_area_name, self.addr_jure_area_socr) )
        elif self.addr_jure_area_name:
            res.append( u"{0}".format(self.addr_jure_area_name) )
        else:
            res.append( u"" )

        if self.addr_jure_town_name:
            res.append( u"{0}".format(self.addr_jure_town_name) )
        elif self.addr_jure_country_name and self.addr_jure_country_socr:
            res.append( u"{0} {1}".format(self.addr_jure_country_socr, self.addr_jure_country_name) )
        elif self.addr_jure_country_name:
            res.append( u"{0}".format(self.addr_jure_country_name) )
        else:
            res.append( u"" )
        
        if self.addr_jure_region_name:
            res.append( u"{0}".format(self.addr_jure_region_name) )
        else:
            res.append( u"" )

        res.append( u"656000" )
        
        res.append( u"Россия" )

       
        street = []
        street.append( u"P~" )

        if self.addr_fact_street_name:
            street.append( u"&{0}".format(self.addr_fact_street_name) )
        else:
            street.append( u"&" )

        if self.addr_fact_house:
            street.append( u"&{0}".format(self.addr_fact_house) )
        else:
            street.append( u"&" )
        
        if self.addr_fact_corps:
            street.append( u"{0}".format(self.addr_fact_corps) )
            
        if self.addr_fact_flat:
            street.append( u",кв.{0}".format(self.addr_fact_flat) )
            
        res.append( "".join(street) )


        if self.addr_fact_area_name and self.addr_fact_area_socr:
            res.append( u"{0} {1}".format(self.addr_fact_area_name, self.addr_fact_area_socr) )
        elif self.addr_fact_area_name:
            res.append( u"{0}".format(self.addr_fact_area_name) )
        else:
            res.append( u"" )

        if self.addr_fact_town_name:
            res.append( u"{0}".format(self.addr_fact_town_name) )
        elif self.addr_fact_country_name and self.addr_fact_country_socr:
            res.append( u"{0} {1}".format(self.addr_fact_country_socr, self.addr_fact_country_name) )
        elif self.addr_fact_country_name:
            res.append( u"{0}".format(self.addr_fact_country_name) )
        else:
            res.append( u"" )
        
        if self.addr_fact_region_name:
            res.append( u"{0}".format(self.addr_fact_region_name) )
        else:
            res.append( u"" )

        res.append( u"656000" )
        
        res.append( u"Россия" )

        res.append( u"C" )
       
        return res
    
    def dateOfBirthday(self):
        # TODO: check
        return self.birthday.strftime("%Y%m%d") if self.birthday else "19000101" 
    
    def phoneNumber(self):
        # TODO: check
        phone = "000000"
        if self.phone:
            phone = self.phone
        elif self.mobile_phone:
            phone = self.mobile_phone
        
        return phone
        



if __name__ == '__main__':
    from dbmis_connect2 import DBMIS
    db = DBMIS()
    obj = PatientInfo2()
    obj.initFromDb(db, 122)
    print obj.asString()
    print obj.patientId()
    print "^".join( obj.patientAddressAsArray() )
    print obj.dateOfBirthday()
    print obj.phoneNumber()
    print obj.clinic_id, obj.area_number
