import sys
import time
import pandas as pd
import requests
from openpyxl import load_workbook
from helper import get_access_token, get_oeffentlich_and_ersatz_uuid, log, save_to_excel
from migrate_schools.migrate_schools_helper import classify_dnr
from migrate_schools.school_ldif_parser import BuildSchoolDFLDIFParser

def migrate_school_data(log_output_dir, api_backend_organisationen, api_backend_orga_root_children, input_excel_schools_complete_path, input_ldap_complete_path):
    log(f"Start method migrate_school_data with Input {log_output_dir}, {api_backend_organisationen}, {api_backend_orga_root_children}, {input_excel_schools_complete_path}, {input_ldap_complete_path}")
    
    log_api_errors = []
    number_of_api_calls = 0
    number_of_api_error_responses = 0
    
    (oeffentlich_uuid, ersatz_uuid) = get_oeffentlich_and_ersatz_uuid(api_backend_orga_root_children)
    log(f"")
    log(f"Using Oeffentliche Schulen ParentKnoten: {oeffentlich_uuid}")
    log(f"Using Ersatzzschule Parent Knoten: {ersatz_uuid}")

    workbook = load_workbook(filename=input_excel_schools_complete_path)
    sheet = workbook.active
    data = sheet.values
    columns = next(data)[0:]  # first row is the header
    df_excel = pd.DataFrame(data, columns=columns)
    
    with open(input_ldap_complete_path, 'rb') as input_file:
        parser = BuildSchoolDFLDIFParser(input_file)
        parser.parse()
    df_ldap = pd.DataFrame(parser.schools, columns=['dnr'])
    df_ldap = df_ldap.drop_duplicates(subset='dnr')
    merged_df = pd.merge(df_ldap, df_excel, on='dnr', how='left')
    merged_df['Classification'] = merged_df['dnr'].apply(classify_dnr)
    
    access_token = get_access_token()
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': 'Bearer ' + access_token
    }
    

    for index, row in merged_df.iterrows():
        if(row['Classification'] == 'OEFFENTLICH' or row['Classification'] == 'SONSTIGE'):
            parentUUID = oeffentlich_uuid
        elif(row['Classification'] == 'ERSATZ'):
            parentUUID = ersatz_uuid
        else:
            raise Exception('Each School must have a Classifier')
        
        name_value = "Unbekannt LDAP Import" if pd.isna(row['name1']) else row['name1']
        email_value = None if pd.isna(row['imehl']) else row['imehl']
        post_data = {
                    "kennung": row['dnr'],
                    "name": name_value,
                    "administriertVon": parentUUID,
                    "emailAdress":email_value,
                    "typ": "SCHULE"
        }
        
        attempt = 1
        while attempt < 5:
            try:
                response = requests.post(api_backend_organisationen, json=post_data, headers=headers)
                break
            except requests.RequestException as e:
                attempt += 1
                log(f"Create School Request Attempt {attempt} failed: {e}. Retrying...")
                time.sleep(5*attempt) #Exponential Backof    
        
        number_of_api_calls += 1
        if response.status_code == 401:
            log(f"Create-School-Request - 401 Unauthorized error")
            sys.exit()
        elif response.status_code != 201:
            log(f"Failed Importing School {row['dnr']} with name {name_value} and emailAdress {email_value}")
            number_of_api_error_responses += 1
            log_api_errors.append({
                'dnr': row['dnr'],
                'name':name_value,
                'administriert_von':parentUUID,
                'error_response_body': response.json(),
                'status_code': response.status_code
            })
        else:
            log(f"Successfully Imported School {row['dnr']} with name {name_value} and emailAdress {email_value}")
    
    # RUN STATISTICS 
    log("")
    log("###STATISTICS###")
    log("")
    log(f"Number of found Schools: {parser.number_of_found_schools}")
    log(f"Number of API Calls: {number_of_api_calls}")
    log(f'Number of API Error Responses: {number_of_api_error_responses}')
     
    # EXCEL LOGGING    
    log_data = {'Failed_Api_Create_School': pd.DataFrame(log_api_errors)}
    save_to_excel(log_data, log_output_dir, 'log_migrate_schools')
    log("")
    log("End method migrate_school_data")
