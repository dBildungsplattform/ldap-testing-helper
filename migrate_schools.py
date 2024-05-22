from base64 import b64decode
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

def migrate_school_data(post_endpoint, get_oeff_and_ersatz_UUID_endpoint, input_path_excel, input_path_ldap):
    print(f"Start Migration School Data with Input {post_endpoint}, {get_oeff_and_ersatz_UUID_endpoint}, {input_path_excel}, {input_path_ldap}")
    
    if not post_endpoint:
        raise ValueError("POST Endpoint For cannot be null or empty")
    if not get_oeff_and_ersatz_UUID_endpoint:
        raise ValueError("GET Oeff & Ersatz UUID Endpoint For cannot be null or empty")
    if not input_path_excel:
        raise ValueError("Input path for Excel cannot be null or empty")
    if not input_path_ldap:
        raise ValueError("Input path for LDAP cannot be null or empty")
    
    access_token = get_access_token()
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': 'Bearer ' + access_token
    }
    print(f"Using Authorization: {headers['Authorization']}")
    
    (oeffentlich_uuid, ersatz_uuid) = get_oeffentlich_and_ersatz_uuid(get_oeff_and_ersatz_UUID_endpoint)
    print(f"Using Oeffentliche Schulen Knoten: {oeffentlich_uuid}")
    print(f"Using Ersatzzschule Knoten: {ersatz_uuid}")

    workbook = load_workbook(filename=input_path_excel)
    sheet = workbook.active
    data = sheet.values
    columns = next(data)[0:]  # first row is the header
    df_excel = pd.DataFrame(data, columns=columns)
    
    with open(input_path_ldap, 'rb') as input_file:
        number_of_api_calls = 0
        number_of_api_error_responses = 0
        parser = BuildSchoolDFLDIFParser(input_file)
        parser.parse()
        
        df_ldap = pd.DataFrame(parser.schools, columns=['dnr'])
        merged_df = pd.merge(df_ldap, df_excel, on='dnr', how='left')
        merged_df['Classification'] = merged_df['dnr'].apply(classify_dnr)
  
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
            response = requests.post(post_endpoint, json=post_data, headers=headers)
            number_of_api_calls += 1
            if(response.status_code != 201):
                number_of_api_error_responses += 1
                print("Failing Body:")
                print(post_data)

            print(f"Request for row {index} with school: {row['dnr']} returned status code {response.status_code}")
        
        print("")
        print("###STATISTICS###")
        print("")
        print(f"Number of found Schools: {parser.number_of_found_schools}")
        print(f"Number of API Calls: {number_of_api_calls}")
        print(f'Number of API Error Responses: {number_of_api_error_responses}')
        
        print("")
        print("End Migration School Data")
