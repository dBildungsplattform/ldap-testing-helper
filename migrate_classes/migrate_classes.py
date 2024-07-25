from datetime import datetime, timedelta
import os
import sys
import pandas as pd
import requests
from helper import get_access_token, get_is_class_object, get_school_dnr_uuid_mapping, parse_dn
from ldif import LDIFParser, LDIFWriter

from migrate_persons.person_helper import get_orgaid_by_dnr

class BuildClassesDFLDIFParser(LDIFParser):
    def __init__(self, input_file):
        super().__init__(input_file)
        self.number_of_found_classes = 0
        self.classes = []

    def handle(self, dn, entry):
        parsed_dn = parse_dn(dn)
        is_class_object = get_is_class_object(parsed_dn)
        if is_class_object:
            self.number_of_found_classes += 1
            self.classes.append(parsed_dn['cn'][0])

def migrate_class_data(log_ouput_dir, post_organisation_endpoint, schools_get_endpoint, input_path_ldap):
    print(f"Start Migration Classes Data")
    
    error_log = []
    number_of_api_calls = 0
    number_of_api_error_responses = 0
    
    with open(input_path_ldap, 'rb') as input_file:
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
    school_uuid_dnr_mapping = get_school_dnr_uuid_mapping(schools_get_endpoint)

    for index, row in df_ldap.iterrows():
        if index % 50 == 0:
            elapsed_time = datetime.now() - token_acquisition_time
            if elapsed_time > timedelta(minutes=4):
                access_token = get_access_token()
                headers['Authorization'] = 'Bearer ' + access_token
                token_acquisition_time = datetime.now()
                print(f"{datetime.now()} : Refreshed Authorization: {headers['Authorization']}")
        
        orga_id_parent_school = get_orgaid_by_dnr(school_uuid_dnr_mapping, row['school_dnr'])
        if orga_id_parent_school == None:
            print(f"Failed Importing Class {row['class_name']} for school {row['school_dnr']}")
            error_log.append({
                'class_name': row['class_name'],
                'school_uuid':'MISSING',
                'school_dnr': row['school_dnr'],
                'error_response_body': '-',
                'status_code': 'PARENT SCHOOL FOR DNR NOT FOUND'
            })
        
        post_data = {
            "kennung": None,
            "name": row['class_name'],
            "administriertVon": orga_id_parent_school,
            "zugehoerigZu": orga_id_parent_school,
            "typ": "KLASSE"
        }
        response = requests.post(post_organisation_endpoint, json=post_data, headers=headers)
        number_of_api_calls += 1
        if response.status_code == 401:
            print(f"{datetime.now()} : Create-Kontext-Request - 401 Unauthorized error")
            sys.exit()
        elif response.status_code != 201:
            print(f"Failed Importing Class {row['class_name']} for school {row['school_dnr']}")
            number_of_api_error_responses += 1
            error_log.append({
                'class_name': row['class_name'],
                'school_uuid':orga_id_parent_school,
                'school_dnr': row['school_dnr'],
                'error_response_body': response.json(),
                'status_code': response.status_code
            })
        else:
            print(f"Successfully Imported Class {row['class_name']} for school {row['school_dnr']}")
            
    print("")
    print("###STATISTICS###")
    print("")
    print(f"Number of found Classes: {len(parser.classes)}")
    print(f"Number of API Calls: {number_of_api_calls}")
    print(f'Number of API Error Responses: {number_of_api_error_responses}')
    print("")
    print("End Migration Class Data")
    
    print(error_log)
            
    os.makedirs(log_ouput_dir, exist_ok=True)
    excel_path = os.path.join(log_ouput_dir, 'migrate_classes_log.xlsx')
    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            error_log.to_excel(writer, sheet_name='errors', index=False)
        print(f"Log responses have been saved to '{excel_path}'.")
        print(f"Check the current working directory: {os.getcwd()}")
    except Exception as e:
        print(f"An error occurred while saving the Excel file: {e}")