from base64 import b64decode
import os
import sys
import pandas as pd
import requests
from openpyxl import load_workbook
from helper import get_access_token, get_oeffentlich_and_ersatz_uuid, parse_dn, get_is_school_object, get_is_uid_object
from ldif import LDIFParser, LDIFWriter
import ldap.dn
from datetime import datetime, timedelta

class BuildPersonDFLDIFParser(LDIFParser):
    def __init__(self, input_file):
        super().__init__(input_file)
        self.number_of_found_persons = 0
        self.entries_list = []

    def handle(self, dn, entry):
        parsed_dn = parse_dn(dn)
        is_person = get_is_uid_object(parsed_dn)
        if is_person:
            self.number_of_found_persons += 1
            uid = parsed_dn['uid'][0]
            givenName = entry.get('givenName', [None])[0]
            sn = entry.get('sn', [None])[0]
            krb5PrincipalName = entry.get('krb5PrincipalName', [None])[0]
            userPassword = entry.get('userPassword', [None])[0]
            
            new_entry = {
                'uid': uid,
                'givenName': givenName,
                'sn': sn,
                'krb5PrincipalName': krb5PrincipalName,
                'userPassword': userPassword
            }
            self.entries_list.append(new_entry)
            print(f"Identified Person Nr: {self.number_of_found_persons}")

def get_new_access_token():
    access_token = get_access_token()
    if access_token is None:
        raise ValueError("Could Not Get Access Token")
    return access_token


def migrate_person_data(post_endpoint, input_path_ldap):
    print(f"Start Migration School Data with Input {post_endpoint}, {input_path_ldap}")
    
    if not post_endpoint:
        raise ValueError("POST Endpoint For cannot be null or empty")
    if not input_path_ldap:
        raise ValueError("Input path for LDAP File cannot be null or empty")
    
    with open(input_path_ldap, 'rb') as input_file:
        parser = BuildPersonDFLDIFParser(input_file)
        parser.parse()
        
    df_ldap = pd.DataFrame(parser.entries_list)
    
    access_token = get_new_access_token()
    token_acquisition_time = datetime.now()
    
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': 'Bearer ' + access_token
    }
    print(f"Using Authorization: {headers['Authorization']}")
    
    print(f"{datetime.now()} : Starting Api Requests")
    number_of_api_calls = 0
    number_of_skipped_api_calls = 0
    number_of_api_error_responses = 0
    
    # DataFrame to store failed API responses
    failed_responses = []

    for index, row in df_ldap.iterrows():
        if index % 50 == 0:
            elapsed_time = datetime.now() - token_acquisition_time
            if elapsed_time > timedelta(minutes=4):
                access_token = get_new_access_token()
                headers['Authorization'] = 'Bearer ' + access_token
                token_acquisition_time = datetime.now()
                print(f"{datetime.now()} : Refreshed Authorization: {headers['Authorization']}")
                
        email = row['krb5PrincipalName'].decode('utf-8') if isinstance(row['krb5PrincipalName'], bytes) else row['krb5PrincipalName']
        sn = row['sn'].decode('utf-8') if isinstance(row['sn'], bytes) else row['sn']
        given_name = row['givenName'].decode('utf-8') if isinstance(row['givenName'], bytes) else row['givenName']
        username = row['uid'].decode('utf-8') if isinstance(row['uid'], bytes) else row['uid']
        hashed_password = row['userPassword'].decode('utf-8') if isinstance(row['userPassword'], bytes) else row['userPassword']
        
        is_skip = 'admin' in (sn or '').lower() or \
             'iqsh' in (sn or '').lower() or \
             'fvm-admin' in (sn or '').lower()
        
        if is_skip == True:
            number_of_skipped_api_calls += 1
            failed_responses.append({
                    'email': email,
                    'familienname': sn,
                    'vorname': given_name,
                    'username': username,
                    'hashedPassword': hashed_password,
                    'error_response_body': 'Skipped, No Migration Desired',
                    'status_code': 'NO_MIGRATION'
                })
            print(f"Request for row {index} Skipped, NO_MIGRATION")
        else:
            post_data = {
                "email": email,
                "name": {
                    "familienname": sn,
                    "vorname": given_name,
                },
                "username": username,
                "hashedPassword": hashed_password,
            }
            response = requests.post(post_endpoint, json=post_data, headers=headers)
            number_of_api_calls += 1
            if response.status_code == 401:
                print(f"{datetime.now()} : 401 Unauthorized error")
                sys.exit()
            if response.status_code != 201:
                number_of_api_error_responses += 1
                print("Failing Body (write to Dataframe):")
                print(post_data)
                failed_responses.append({
                    'email': post_data['email'],
                    'familienname': post_data['name']['familienname'],
                    'vorname': post_data['name']['vorname'],
                    'username': post_data['username'],
                    'hashedPassword': post_data['hashedPassword'],
                    'error_response_body': response.json(),
                    'status_code': response.status_code
                })
            print(f"Request for row {index} returned status code {response.status_code}")
            
    print("")
    print("###STATISTICS###")
    print("")
    print(f"Number of found Persons: {parser.number_of_found_persons}")
    print(f"Number of Actively Skipped API Calls: {number_of_skipped_api_calls}")
    print(f"Number of API Calls: {number_of_api_calls}")
    print(f'Number of API Error Responses: {number_of_api_error_responses}')
    print("")
    print("End Migration Person Data")
    
    # Convert the list of failed responses to a DataFrame and save to an Excel file
    failed_responses_df = pd.DataFrame(failed_responses)
    output_dir = '/usr/src/app/output'
    os.makedirs(output_dir, exist_ok=True)
    excel_path = os.path.join(output_dir, 'failed_api_responses.xlsx')
    try:
        failed_responses_df.to_excel(excel_path, index=False)
        print(f"Failed API responses have been saved to '{excel_path}'.")
        print(f"Check the current working directory: {os.getcwd()}")
    except Exception as e:
        print(f"An error occurred while saving the Excel file: {e}")


