from datetime import datetime, timedelta
import sys
import numpy as np
import pandas as pd
from helper import create_kontext_api_call, get_access_token, get_orgaid_by_dnr, get_rolle_id, get_school_dnr_uuid_mapping, log, save_to_excel
from migrate_itslearning_affiliation.itslearning_affiliation_ldif_parser import BuildItslearningGroupsDFLDIFParser
          
ROLE_NAME_ITSLEARNING_LEHRKRAFT = 'itslearning-Lehrkraft'
ROLE_NAME_ITSLEARNING_ADMIN = 'itslearning-Administrator'
                         
def migrate_itslearning_affiliation_data(log_output_dir, api_backend_dbiam_personenkontext, api_backend_organisationen, api_backend_rolle, api_backend_orga_root_children, input_ldap_complete_path):
    log(f"Start method migrate_itslearning_affiliation with Input {log_output_dir}, {api_backend_dbiam_personenkontext}, {api_backend_organisationen}, {api_backend_rolle}, {api_backend_orga_root_children}, {input_ldap_complete_path}")
    
    log_api_errors = []
    log_missing_request_data = []
    number_of_create_kontext_api_calls = 0
    number_of_create_kontext_api_error_responses = 0
    
    log(f"Start BuildPersonDFLDIFParser")
    with open(input_ldap_complete_path, 'rb') as input_file:
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
    
    school_uuid_dnr_mapping = get_school_dnr_uuid_mapping(api_backend_organisationen, api_backend_orga_root_children)
    rolleid_itslearning_admin = get_rolle_id(api_backend_rolle, ROLE_NAME_ITSLEARNING_ADMIN)
    rolleid_itslearning_lehrer = get_rolle_id(api_backend_rolle, ROLE_NAME_ITSLEARNING_LEHRKRAFT)
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
                log_missing_request_data.append({
                'dnr': dnr,
                'orga_id': orga_id,
                'rolle_id':rolle_id,
                'username':username,
                'details':'Either username or orga_id or rolle_id is missing. No API Request was made'
                })
                continue
            
            response_create_kontext = create_kontext_api_call(
                migration_run_type='ITSLEARNING',
                api_backend_dbiam_personenkontext=api_backend_dbiam_personenkontext, 
                headers=headers, 
                person_id=None, 
                username=username, 
                organisation_id=orga_id, 
                rolle_id=rolle_id, 
                email=None,
                befristung_valid_jsdate=None
            )
            number_of_create_kontext_api_calls += 1
            if response_create_kontext.status_code == 401:
                log(f"Create-Kontext-Request - 401 Unauthorized error")
                sys.exit()
            elif response_create_kontext.status_code != 201:
                number_of_create_kontext_api_error_responses += 1
                log_api_errors.append({
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
    
    # RUN STATISTICS      
    log("")
    log("###STATISTICS###")
    log("")
    log(f"Number of found Itslearning Admin or Lehrer Groups: {parser.number_of_found_itslearning_admin_or_lehrer_groups}")
    log(f"Number of API Calls: {number_of_create_kontext_api_calls}")
    log(f'Number of API Error Responses: {number_of_create_kontext_api_error_responses}')
        
    # EXCEL LOGGING   
    log_data = {
        'Skipped_Dueto_Missing_Request_Data': pd.DataFrame(log_missing_request_data),
        'Failed_Api_Create_Itslearning_Kontexts': pd.DataFrame(log_api_errors)
        }
    save_to_excel(log_data, log_output_dir, 'migrate_itslearning_affiliation')
    log("")
    log("End method migrate_itslearning_affiliation_data")