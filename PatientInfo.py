#!/usr/bin/env python
# encoding: utf8

import logging
import constants

DOC_TYPES = {1:u"1",
             2:u"2",
             3:u"3",
             4:u"4",
             5:u"5",
             6:u"6",
             7:u"7",
             8:u"8",
             9:u"9",
             10:u"10",
             11:u"11",
             12:u"12",
             13:u"13",
             14:u"14",
             15:u"15",
             16:u"16",
             17:u"17",
             18:u"18"
             }

SKIP_OGRN  = True # Do not put OGRN into IBR

ASSIGN_ATT = True
ADATE_ATT  = '20141001'

STEP = 1000
log = logging.getLogger(__name__)

#
# Код сгенерирован с помощью fldsconv.py
#

class PatientInfo:

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
        self.enp = None

    def initFromRec(self, rec):
        ## vw_peoples
        ## Autogenerated ON
        self.people_id = rec[0]
        self.lname = rec[1]
        self.fname = rec[2]
        self.mname = rec[3]
        self.sex = rec[4]
        self.document_type_id_fk = rec[5]
        self.document_type_name = rec[6]
        self.document_series = rec[7]
        self.document_number = rec[8]
        self.phone = rec[9]
        self.birthday = rec[10]
        self.inn = rec[11]
        self.insurance_certificate = rec[12]
        self.medical_insurance_series = rec[13]
        self.medical_insurance_number = rec[14]
        self.medical_insurance_region_id_fk = rec[15]
        self.medical_insurance_region_name = rec[16]
        self.medical_insurance_company_name = rec[17]
        self.settlement_type_id_fk = rec[18]
        self.settlement_type_name = rec[19]
        self.social_status_id_fk = rec[20]
        self.social_status_name = rec[21]
        self.branch_id_fk = rec[22]
        self.branch_name = rec[23]
        self.work_place = rec[24]
        self.addr_jure_region_code = rec[25]
        self.addr_jure_region_name = rec[26]
        self.addr_jure_town_code = rec[27]
        self.addr_jure_town_name = rec[28]
        self.addr_jure_town_socr = rec[29]
        self.addr_jure_town_correct = rec[30]
        self.addr_jure_area_code = rec[31]
        self.addr_jure_area_name = rec[32]
        self.addr_jure_area_socr = rec[33]
        self.addr_jure_area_correct = rec[34]
        self.addr_jure_country_code = rec[35]
        self.addr_jure_country_name = rec[36]
        self.addr_jure_country_socr = rec[37]
        self.addr_jure_country_correct = rec[38]
        self.addr_jure_street_code = rec[39]
        self.addr_jure_street_name = rec[40]
        self.addr_jure_street_socr = rec[41]
        self.addr_jure_street_correct = rec[42]
        self.addr_jure_house = rec[43]
        self.addr_jure_corps = rec[44]
        self.addr_jure_flat = rec[45]
        self.addr_fact_region_code = rec[46]
        self.addr_fact_region_name = rec[47]
        self.addr_fact_town_code = rec[48]
        self.addr_fact_town_name = rec[49]
        self.addr_fact_town_socr = rec[50]
        self.addr_fact_town_correct = rec[51]
        self.addr_fact_area_code = rec[52]
        self.addr_fact_area_name = rec[53]
        self.addr_fact_area_socr = rec[54]
        self.addr_fact_area_correct = rec[55]
        self.addr_fact_country_code = rec[56]
        self.addr_fact_country_name = rec[57]
        self.addr_fact_country_socr = rec[58]
        self.addr_fact_country_correct = rec[59]
        self.addr_fact_street_code = rec[60]
        self.addr_fact_street_name = rec[61]
        self.addr_fact_street_socr = rec[62]
        self.addr_fact_street_correct = rec[63]
        self.addr_fact_house = rec[64]
        self.addr_fact_corps = rec[65]
        self.addr_fact_flat = rec[66]
        self.birthplace = rec[67]
        self.medical_card_series = rec[68]
        self.medical_card_number = rec[69]
        self.tfoms_code_id_fk = rec[70]
        self.tfoms_name = rec[71]
        self.addr_fact_full = rec[72]
        self.document_who = rec[73]
        self.document_when = rec[74]
        self.is_new_policy = rec[75]
        self.work_code_id = rec[76]
        self.insorg_id = rec[77]
        self.social_status_unemployed_id_fk = rec[78]
        self.social_status_unemployed_name = rec[79]
        self.social_status_front_card = rec[80]
        self.last_flgr = rec[81]
        self.territory_id_fk = rec[82]
        self.special_case = rec[83]
        self.delegate_status_id_fk = rec[84]
        self.delegate_lname = rec[85]
        self.delegate_fname = rec[86]
        self.delegate_mname = rec[87]
        self.insorg_ogrn = rec[88]
        self.citizenship = rec[89]
        self.policy_begin = rec[90]
        self.policy_end = rec[91]
        self.streets_ul_id = rec[92]
        self.settlement_types_np_id = rec[93]
        self.private_consent = rec[94]
        self.blood_group = rec[95]
        self.clinic_contract_id_fk = rec[96]
        self.p_payment_type_id_fk = rec[97]
        self.double_person = rec[98]
        self.mobile_phone = rec[99]
        ## Autogenerated OFF
        self.enp = None

    def initFromRec0(self, rec):
        ## peoples table
        ## Autogenerated ON
        self.people_id = rec[0]
        self.lname = rec[1]
        self.fname = rec[2]
        self.mname = rec[3]
        self.sex = rec[4]
        self.document_type_id_fk = rec[5]
        self.document_series = rec[6]
        self.document_number = rec[7]
        self.phone = rec[8]
        self.birthday = rec[9]
        self.inn = rec[10]
        self.insurance_certificate = rec[11]
        self.medical_insurance_series = rec[12]
        self.medical_insurance_number = rec[13]
        self.medical_insurance_region_id_fk = rec[14]
        self.medical_insurance_company_name = rec[15]
        self.settlement_type_id_fk = rec[16]
        self.social_status_id_fk = rec[17]
        self.branch_id_fk = rec[18]
        self.addr_jure_region_code = rec[19]
        self.addr_jure_town_code = rec[20]
        self.addr_jure_town_name = rec[21]
        self.addr_jure_town_socr = rec[22]
        self.addr_jure_area_code = rec[23]
        self.addr_jure_area_name = rec[24]
        self.addr_jure_area_socr = rec[25]
        self.addr_jure_country_code = rec[26]
        self.addr_jure_country_name = rec[27]
        self.addr_jure_country_socr = rec[28]
        self.addr_jure_street_code = rec[29]
        self.addr_jure_street_name = rec[30]
        self.addr_jure_street_socr = rec[31]
        self.addr_jure_house = rec[32]
        self.addr_jure_flat = rec[33]
        self.addr_fact_region_code = rec[34]
        self.addr_fact_town_code = rec[35]
        self.addr_fact_town_name = rec[36]
        self.addr_fact_town_socr = rec[37]
        self.addr_fact_area_code = rec[38]
        self.addr_fact_area_name = rec[39]
        self.addr_fact_area_socr = rec[40]
        self.addr_fact_country_code = rec[41]
        self.addr_fact_country_name = rec[42]
        self.addr_fact_country_socr = rec[43]
        self.addr_fact_street_code = rec[44]
        self.addr_fact_street_name = rec[45]
        self.addr_fact_street_socr = rec[46]
        self.addr_fact_house = rec[47]
        self.addr_fact_flat = rec[48]
        self.work_place = rec[49]
        self.tfoms_code_id_fk = rec[50]
        self.addr_fact_full = rec[51]
        self.document_who = rec[52]
        self.document_when = rec[53]
        self.is_new_policy = rec[54]
        self.work_code_id = rec[55]
        self.insorg_id = rec[56]
        self.social_status_unemployed_id_fk = rec[57]
        self.social_status_front_card = rec[58]
        self.last_flgr = rec[59]
        self.territory_id_fk = rec[60]
        self.special_case = rec[61]
        self.delegate_status_id_fk = rec[62]
        self.delegate_lname = rec[63]
        self.delegate_fname = rec[64]
        self.delegate_mname = rec[65]
        self.insorg_ogrn = rec[66]
        self.citizenship = rec[67]
        self.policy_begin = rec[68]
        self.policy_end = rec[69]
        self.streets_ul_id = rec[70]
        self.settlement_types_np_id = rec[71]
        self.private_consent = rec[72]
        self.blood_group = rec[73]
        self.p_payment_type_id_fk = rec[74]
        self.clinic_contract_id_fk = rec[75]
        self.birthplace = rec[76]
        self.addr_jure_corps = rec[77]
        self.addr_fact_corps = rec[78]
        self.double_person = rec[79]
        self.mobile_phone = rec[80]
        self.sms_status = rec[81]

        ## Autogenerated OFF
        self.enp = None


    def initFromDb(self, db, patientId):
        rec = db.get_people(patientId)
        if rec is not None:
            self.initFromRec(rec)

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

def p1(patient, insorg, CLINIC_OGRN = u""):
    import datetime
    now = datetime.datetime.now()
    s_now = u"%04d-%02d-%02d" % (now.year, now.month, now.day)

    res = []
    res.append( u"{0}".format(patient.people_id) )
    res.append( u"{0}".format(patient.lname.strip().upper()) )
    res.append( u"{0}".format(patient.fname.strip().upper()) )
    if patient.mname == None:
        res.append(u"")
    else:
        res.append( u"{0}".format(patient.mname.strip().upper()) )
    dr = patient.birthday
    sdr = u"%04d-%02d-%02d" % (dr.year, dr.month, dr.day)
    res.append(sdr)
    sex = patient.sex
    if sex == u"М":
        res.append(u"1")
    else:
        res.append(u"2")
    doc_type_id = patient.document_type_id_fk
    if doc_type_id == None:
        sdt = u"14"
    elif DOC_TYPES.has_key(doc_type_id):
        sdt = DOC_TYPES[doc_type_id]
    else:
        sdt = u""
    res.append(sdt)
    if patient.document_series == None:
        ds = u""
    else:
        ds = patient.document_series
    res.append(ds)
    if patient.document_number == None:
        dn = u""
    else:
        dn = patient.document_number
    res.append(dn)

    if patient.insurance_certificate == None:
        SNILS = u""
    else:
        SNILS = patient.insurance_certificate
    res.append(SNILS)

    ogrn = insorg.ogrn
    if ogrn == None or ogrn == 0 or SKIP_OGRN:
        insorg_ogrn = u""
    else:
        insorg_ogrn = u"{0}".format(ogrn)
    res.append(insorg_ogrn)

    okato = insorg.okato
    if okato == None or okato == 0 or SKIP_OGRN:
        insorg_okato = u""
    else:
        insorg_okato = u"{0}".format(ogrn)
    res.append(insorg_okato)

    # medical_insurance_series (s_mis) & medical_insurance_number (s_min)
    sss = patient.medical_insurance_series
    if sss == None:
        s_mis = u""
    else:
        s_mis = u"{0}".format(sss)

    sss = patient.medical_insurance_number
    if sss == None:
        s_min = u""
    else:
        s_min = u"{0}".format(sss)

    enp = u""
    if len(s_mis) == 0:
        tdpfs = u"3" # Полис ОМС единого образца
        enp = s_min
        smin = u""
    elif s_mis[0] in (u"0", u"1", u"2", u"3", u"4", u"5", u"6", u"7", u"8", u"9"):
        tdpfs = u"2" # Временное свидетельство, ....
    else:
        tdpfs = u"1" # Полис ОМС старого образца


    # ENP
    if patient.enp is not None:
        enp = patient.enp

    res.append(enp)

    res.append(tdpfs)

    res.append(s_mis)

    res.append(s_min)

    # medical care start
    res.append(s_now)
    # medical care end
    res.append(s_now)

    # MO  OGRN
    res.append(CLINIC_OGRN)
    # HEALTHCARE COST
    res.append(u"")

    return u"|".join(res)

def p2join(ar_in):
    str_r = u''
    nnn = len(ar_in)
    i = 0
    for sss in ar_in:
        i += 1
        if sss is None:
            if i < nnn: str_r += u';'
        else:
            if  i < nnn:
                str_r += u'"' + sss + u'";'
            else:
                str_r += u'"' + sss + u'"'
    return str_r

def p2(patient, MCOD = None, MOTIVE_ATT = 2, DATE_ATT = None):
# new format
    import datetime
    import re

    now = datetime.datetime.now()
    s_now = u"%04d-%02d-%02d" % (now.year, now.month, now.day)

    # medical_insurance_series (s_mis) & medical_insurance_number (s_min)
    sss = patient.medical_insurance_series
    if sss == None:
        s_mis = u""
    else:
        s_mis = u"{0}".format(sss)

    sss = patient.medical_insurance_number
    if sss == None:
        s_min = None
    else:
        s_min = u"{0}".format(sss)

    enp = None
    if len(s_mis) == 0:
        tdpfs = u"П" # Полис ОМС бумажный единого образца
        enp = s_min
        misn = None
    elif s_mis[0] in (u"0", u"1", u"2", u"3", u"4", u"5", u"6", u"7", u"8", u"9"):
        tdpfs = u"В" # Временное свидетельство, ....
        misn = s_min
    else:
        tdpfs = u"С" # Полис ОМС старого образца
        if s_min is None:
            misn  = s_mis
        else:
            misn  = s_mis + u" № " + s_min

    if patient.enp is not None:
        enp = patient.enp

    res = []
    # 1
    res.append(tdpfs)
    # 2
    res.append(misn)
    # 3
    res.append(enp)

    # 4
    res.append( u"{0}".format(patient.lname.strip().upper()) )
    # 5
    res.append( u"{0}".format(patient.fname.strip().upper()) )
    # 6
    if patient.mname == None:
        res.append(None)
    else:
        res.append( u"{0}".format(patient.mname.strip().upper()) )

    # 7
    dr = patient.birthday
    sdr = u"%04d%02d%02d" % (dr.year, dr.month, dr.day)
    res.append(sdr)

    # 8
    if patient.birthplace is None:
        res.append(None)
    else:
        res.append( u"{0}".format(patient.birthplace.strip().upper()) )

    # 9
    doc_type_id = patient.document_type_id_fk
    if doc_type_id is None:
        sdt = u"14"
    elif DOC_TYPES.has_key(doc_type_id):
        sdt = DOC_TYPES[doc_type_id]
    else:
        sdt = None
    res.append(sdt)

    # 10
    if patient.document_series is None:
        dsn = None
    else:
        dsn = patient.document_series

    if patient.document_number is not None:
        if dsn is None:
            dsn = patient.document_number
        else:
            dsn += u" № " + patient.document_number
    res.append(dsn)

    # 11
    if patient.document_when is None:
        res.append(None)
    else:
        dw = patient.document_when
        sdr = u"%04d%02d%02d" % (dw.year, dw.month, dw.day)
        res.append(sdr)

    # 12
    if patient.document_who is None:
        res.append(None)
    else:
        res.append(patient.document_who.strip().upper())

    # 13
    if patient.insurance_certificate is None:
        SNILS = None
    else:
        s_pattern = re.compile(u"[0-9][0-9][0-9]-[0-9][0-9][0-9]-[0-9][0-9][0-9] [0-9][0-9]")
        s_snils = patient.insurance_certificate
        if s_pattern.match(s_snils):
            SNILS = s_snils
        else:
            SNILS = None

    res.append(SNILS)

    # 14
    if MCOD is None:
        s_mcod = None
    else:
        s_mcod = str(MCOD)
    res.append(s_mcod)

    # 15
    if MOTIVE_ATT not in (1,2,3,99):
        res.append(None)
    else:
        res.append(str(MOTIVE_ATT))

    # 16
    res.append(None)

    # 17
    if ASSIGN_ATT:
        res.append(ADATE_ATT)
    elif DATE_ATT is None:
        res.append(None)
    else:
        dw = DATE_ATT
        sdr = u"%04d%02d%02d" % (dw.year, dw.month, dw.day)
        res.append(sdr)

    # 18
    res.append(None)

    return p2join(res)


def get_patient_list(recs):

    patient_list = []

    p_ids = []
    nnn   = 0
    for rec in recs:
        p_id = rec[0]
        if p_id in p_ids: continue
        p_ids.append(p_id)

        p_obj = PatientInfo()
        p_obj.initFromRec(rec)
        patient_list.append(p_obj)
        nnn += 1

    return patient_list


if __name__ == '__main__':
    from  medlib.modules.dbmis_connect import DBMIS
    db = DBMIS()
    obj = PatientInfo()
    obj.initFromDb(db, 122)
    print obj.asString()
    print obj.patientId()
    print "^".join( obj.patientAddressAsArray() )
    print obj.dateOfBirthday()
    print obj.phoneNumber()
