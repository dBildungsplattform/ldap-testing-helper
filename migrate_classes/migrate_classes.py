from datetime import datetime, timedelta
import sys
import time
import pandas as pd
import requests
from helper import get_access_token, get_orgaid_by_dnr, get_school_dnr_uuid_mapping, log, save_to_excel
from migrate_classes.classes_ldif_parser import BuildClassesDFLDIFParser

def migrate_class_data(log_output_dir, api_backend_organisationen, api_backend_orga_root_children, input_ldap_complete_path):
    log(f"Start method migrate_school_data with Input {log_output_dir}, {api_backend_orga_root_children}, {api_backend_organisationen}, {input_ldap_complete_path}")
    
    log_api_errors = []
    log_missing_schools = []
    number_of_api_calls = 0
    number_of_api_error_responses = 0
    
    with open(input_ldap_complete_path, 'rb') as input_file:
        parser = BuildClassesDFLDIFParser(input_file)
        parser.parse()
    df_ldap = pd.DataFrame(parser.classes).drop_duplicates()
    df_ldap[0] = df_ldap[0].str.strip()

    split_cols = df_ldap[0].str.split('-', n=1, expand=True)
    df_ldap['school_dnr'] = split_cols[0]
    df_ldap['class_name'] = split_cols[1]

    access_token = get_access_token()
    token_acquisition_time = datetime.now()
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': 'Bearer ' + access_token
    }
    school_uuid_dnr_mapping = get_school_dnr_uuid_mapping(api_backend_organisationen, api_backend_orga_root_children)

    for index, row in df_ldap.iterrows():
        if index % 50 == 0:
            elapsed_time = datetime.now() - token_acquisition_time
            if elapsed_time > timedelta(minutes=4):
                access_token = get_access_token()
                headers['Authorization'] = 'Bearer ' + access_token
                token_acquisition_time = datetime.now()
                log(f"Refreshed Authorization: {headers['Authorization']}")
        
        orga_id_parent_school = get_orgaid_by_dnr(school_uuid_dnr_mapping, row['school_dnr'])
        if orga_id_parent_school == None:
            log(f"Failed Importing Class {row['class_name']} for school {row['school_dnr']}")
            log_missing_schools.append({
                'class_name': row['class_name'],
                'school_dnr': row['school_dnr'],
                'Description': 'The School for this class could not be found in the DB. No API Request was made'
            })
            continue
        
        post_data = {
            "kennung": None,
            "name": row['class_name'],
            "administriertVon": orga_id_parent_school,
            "zugehoerigZu": orga_id_parent_school,
            "typ": "KLASSE"
        }
      
        attempt = 1
        while attempt < 5:
            try:
                response = requests.post(api_backend_organisationen, json=post_data, headers=headers)
                break
            except requests.RequestException as e:
                attempt += 1
                log(f"Create Class Request Attempt {attempt} failed: {e}. Retrying...")
                time.sleep(5*attempt) #Exponential Backoff    
        
        number_of_api_calls += 1
        if response.status_code == 401:
            log(f"Create-Kontext-Request - 401 Unauthorized error")
            sys.exit()
        elif response.status_code != 201:
            log(f"Failed Importing Class {row['class_name']} for school {row['school_dnr']}")
            number_of_api_error_responses += 1
            log_api_errors.append({
                'class_name': row['class_name'],
                'school_uuid':orga_id_parent_school,
                'school_dnr': row['school_dnr'],
                'error_response_body': response.json(),
                'status_code': response.status_code
            })
        else:
            log(f"Successfully Imported Class {row['class_name']} for school {row['school_dnr']}")
    
    # RUN STATISTICS 
    log("")
    log("### CLASSES RUN STATISTICS START ###")
    log("")
    log(f"Number of found Classes: {len(parser.classes)}")
    log(f"Number of API Calls: {number_of_api_calls}")
    log(f'Number of API Error Responses: {number_of_api_error_responses}')
    log("")
    log("### CLASSES RUN STATISTICS END ###")
    log("")
    
    # EXCEL LOGGING   
    log_data = {
        'Failed_Api_Create_Class': pd.DataFrame(log_api_errors),
        'Skipped_DueTo_Missing_Parent_Schools': pd.DataFrame(log_missing_schools)
    }
    save_to_excel(log_data, log_output_dir, 'log_migrate_classes')    
    log("")
    log("End method migrate_classes_data")