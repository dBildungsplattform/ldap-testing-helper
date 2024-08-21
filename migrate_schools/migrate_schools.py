from base64 import b64decode
from datetime import datetime
import os
import sys
import pandas as pd
import requests
from openpyxl import load_workbook
from helper import get_access_token, get_oeffentlich_and_ersatz_uuid, parse_dn, get_is_school_object
from ldif import LDIFParser, LDIFWriter
import ldap.dn

def classify_dnr(dnr):
    if dnr.startswith('070'):
        return 'OEFFENTLICH'
    elif dnr.startswith('07998') or dnr.startswith('079'):
        return 'ERSATZ'
    else:
        return 'SONSTIGE'

class BuildSchoolDFLDIFParser(LDIFParser):
    def __init__(self, input_file):
        super().__init__(input_file)
        self.number_of_found_schools = 0
        self.entries_list = []
        self.schools = []

    def handle(self, dn, entry):
        parsed_dn = parse_dn(dn)
        is_school_object = get_is_school_object(parsed_dn)
        if is_school_object:
            self.number_of_found_schools += 1
            self.schools.append(parsed_dn['ou'][0])

def migrate_school_data(log_output_dir, post_organisation_endpoint, get_oeff_and_ersatz_UUID_endpoint, input_path_excel, input_path_ldap):
    print(f"Start Migration School Data with Input {post_organisation_endpoint}, {get_oeff_and_ersatz_UUID_endpoint}, {input_path_excel}, {input_path_ldap}")
    
    error_log = []
    number_of_api_calls = 0
    number_of_api_error_responses = 0
    
    (oeffentlich_uuid, ersatz_uuid) = get_oeffentlich_and_ersatz_uuid(get_oeff_and_ersatz_UUID_endpoint)
    print(f"Using Oeffentliche Schulen Knoten: {oeffentlich_uuid}")
    print(f"Using Ersatzzschule Knoten: {ersatz_uuid}")

    workbook = load_workbook(filename=input_path_excel)
    sheet = workbook.active
    data = sheet.values
    columns = next(data)[0:]  # first row is the header
    df_excel = pd.DataFrame(data, columns=columns)
    
    with open(input_path_ldap, 'rb') as input_file:
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
        post_data = {
                    "kennung": row['dnr'],
                    "name": name_value,
                    "administriertVon": parentUUID,
                    "typ": "SCHULE"
        }
        response = requests.post(post_organisation_endpoint, json=post_data, headers=headers)
        number_of_api_calls += 1
        if response.status_code == 401:
            print(f"{datetime.now()} : Create-School-Request - 401 Unauthorized error")
            sys.exit()
        elif response.status_code != 201:
            number_of_api_error_responses += 1
            error_log.append({
                'dnr': row['dnr'],
                'name':name_value,
                'administriert_von':parentUUID,
                'error_response_body': response.json(),
                'status_code': response.status_code
            })
        else:
            print(f"Successfully Imported School {row['dnr']} with name {name_value}")
    
    print("")
    print("###STATISTICS###")
    print("")
    print(f"Number of found Schools: {parser.number_of_found_schools}")
    print(f"Number of API Calls: {number_of_api_calls}")
    print(f'Number of API Error Responses: {number_of_api_error_responses}')
    
    print("")
    print("End Migration School Data")
    print(error_log)
    
    error_df = pd.DataFrame(error_log)
    os.makedirs(log_output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    excel_path = os.path.join(log_output_dir, f'migrate_schools_log_{timestamp}.xlsx')

    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            error_df.to_excel(writer, sheet_name='errors', index=False)
        print(f"Log responses have been saved to '{excel_path}'.")
        print(f"Check the current working directory: {os.getcwd()}")
    except Exception as e:
        print(f"An error occurred while saving the Excel file: {e}")
