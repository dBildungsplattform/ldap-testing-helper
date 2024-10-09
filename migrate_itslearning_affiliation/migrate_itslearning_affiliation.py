from datetime import datetime, timedelta
import os
import sys
import numpy as np
import pandas as pd
from helper import get_access_token, get_is_itslearning_lehrer_or_admin_group_object, get_rolle_id, get_school_dnr_uuid_mapping, log, parse_dn
from ldif import LDIFParser

from migrate_persons.person_helper import create_kontext_api_call, get_orgaid_by_dnr

class BuildItslearningGroupsDFLDIFParser(LDIFParser):
    def __init__(self, input_file):
        super().__init__(input_file)
        self.number_of_found_itslearning_admin_or_lehrer_groups = 0
        self.islearning_groups = []

    def handle(self, dn, entry):
        parsed_dn = parse_dn(dn)
        is_itslearning_group_object = get_is_itslearning_lehrer_or_admin_group_object(parsed_dn, entry)
        if is_itslearning_group_object == True:
            self.number_of_found_itslearning_admin_or_lehrer_groups += 1
            log(f"Identified Itslearning Group Nr: {self.number_of_found_itslearning_admin_or_lehrer_groups}")
            log(f"{parsed_dn}")
            log(f"")
            self.islearning_groups.append(entry)
            
                
def migrate_itslearning_affiliation_data(log_output_dir, create_kontext_post_endpoint, orgas_get_endpoint, roles_get_endpoint, input_path_ldap):
    log(f"Start method migrate_itslearning_affiliation with Input {log_output_dir}, {create_kontext_post_endpoint}, {orgas_get_endpoint}, {roles_get_endpoint}, {input_path_ldap}")
    
    error_log = []
    number_of_create_kontext_api_calls = 0
    number_of_create_kontext_api_error_responses = 0
    
    log(f"Start BuildPersonDFLDIFParser")
    with open(input_path_ldap, 'rb') as input_file:
        parser = BuildItslearningGroupsDFLDIFParser(input_file)
        parser.parse()
    df_ldap = pd.DataFrame(parser.islearning_groups)
    log(f"Finished BuildPersonDFLDIFParser")
    
    access_token = get_access_token()
    token_acquisition_time = datetime.now()
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': 'Bearer ' + access_token
    }
    
    school_uuid_dnr_mapping = get_school_dnr_uuid_mapping(orgas_get_endpoint)
    rolleid_itslearning_admin = get_rolle_id(roles_get_endpoint, 'itslearning Admin')
    rolleid_itslearning_lehrer = get_rolle_id(roles_get_endpoint, 'itslearning Lehrkraft')
    log('Using The Following RolleIds:')
    log(f'itslearning Admin: {rolleid_itslearning_admin}, itslearning Lehrkraft: {rolleid_itslearning_lehrer}')
    
    for index, row in df_ldap.iterrows():
        if index % 20 == 0:
            elapsed_time = datetime.now() - token_acquisition_time
            if elapsed_time > timedelta(minutes=3):
                access_token = get_access_token()
                headers['Authorization'] = 'Bearer ' + access_token
                token_acquisition_time = datetime.now()
        log(row['memberUid'])
        cn = [singleCn.decode('utf-8') if isinstance(singleCn, bytes) else singleCn for singleCn in row['cn']][0]
        memberUid_list = []
        if row['memberUid'] is not None and (not isinstance(row['memberUid'], float) or not np.isnan(row['memberUid'])):
            memberUid = [
                singleMemberOf.decode('utf-8') if isinstance(singleMemberOf, bytes) else singleMemberOf
                for singleMemberOf in row['memberUid']
            ]
            for singleMemberUid in memberUid:
                try:
                    memberUid_list.append(singleMemberUid)
                except IndexError:
                    log(f"Warning: Malformed entry found: {singleMemberUid}")
                    continue
            
        dnr = cn.replace('lehrer-','').replace('admins-','')
        orga_id = get_orgaid_by_dnr(school_uuid_dnr_mapping, dnr)
        rolle_id = None
        if(cn.startswith('lehrer-')):
            rolle_id = rolleid_itslearning_lehrer
        if(cn.startswith('admins-')):
            rolle_id = rolleid_itslearning_admin
            
        for username in memberUid_list:   
            if(username == None or orga_id == None or rolle_id == None):
                error_log.append({
                'dnr': dnr,
                'orga_id': orga_id,
                'rolle_id':rolle_id,
                'username':username,
                'error_response_body': '',
                'status_code': '',
                'typ':'NO_REQUEST_MADE',
                'details':'Either username or orga_id or rolle_id is missing'
                })
                continue
            
            response_create_kontext = create_kontext_api_call(
                migration_run_type='ITSLEARNING',
                create_kontext_post_endpoint=create_kontext_post_endpoint, 
                headers=headers, 
                person_id=None, 
                username=username, 
                organisation_id=orga_id, 
                rolle_id=rolle_id, 
                email=None
            )
            number_of_create_kontext_api_calls += 1
            if response_create_kontext.status_code == 401:
                log(f"Create-Kontext-Request - 401 Unauthorized error")
                sys.exit()
            elif response_create_kontext.status_code != 201:
                number_of_create_kontext_api_error_responses += 1
                error_log.append({
                'dnr': dnr,
                'orga_id': orga_id,
                'rolle_id':rolle_id,
                'username':username,
                'error_response_body': response_create_kontext.json(),
                'status_code': response_create_kontext.status_code,
                'typ':'API_ERROR',
                'details':''
                })
                log(f"Create Kontext API Error Response: {response_create_kontext.json()}")
                continue
            
    log("")
    log("###STATISTICS###")
    log("")
    log(f"Number of found Itslearning Admin or Lehrer Groups: {parser.number_of_found_itslearning_admin_or_lehrer_groups}")
    log(f"Number of API Calls: {number_of_create_kontext_api_calls}")
    log(f'Number of API Error Responses: {number_of_create_kontext_api_error_responses}')
        
    error_df = pd.DataFrame(error_log)
    os.makedirs(log_output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    excel_path = os.path.join(log_output_dir, f'migrate_itslearning_affiliation_{timestamp}.xlsx')

    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            error_df.to_excel(writer, sheet_name='errors', index=False)
        log(f"Log responses have been saved to '{excel_path}'.")
        log(f"Check the current working directory: {os.getcwd()}")
    except Exception as e:
        log(f"An error occurred while saving the Excel file: {e}")
    
    log("")
    log("End method migrate_itslearning_affiliation_data")