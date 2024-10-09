from datetime import datetime
import hashlib
import time
import os
import pandas as pd
import requests
from dotenv import load_dotenv
import ldap.dn

def log(x):
    print(f"{datetime.now()} : {x}", flush=True)

def get_access_token():
    token_url = os.getenv('TOKEN_URL')
    payload = {
        'client_id': os.getenv('CLIENT_ID'),
        'client_secret': os.getenv('CLIENT_SECRET'),
        'username': os.getenv('USERNAME'),
        'password': os.getenv('PASSWORD'),
        'grant_type': os.getenv('GRANT_TYPE')
    }
    token_headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    attempt = 1
    while attempt < 5:
        try:
            token_response = requests.post(token_url, data=payload, headers=token_headers)
            token_response.raise_for_status()  # Raises an error for bad responses (4xx or 5xx)
            token_response_json = token_response.json()
            token = token_response_json.get('access_token')
            if token:
                return token
            else:
                raise Exception("Access token not found in the response.")
        except requests.RequestException as e:
            attempt += 1
            print(f"Get Access Token Attempt {attempt} failed: {e}. Retrying...")
            time.sleep(5*attempt)
    
    raise Exception("Max retries exceeded. Failed to obtain access token.")

def get_oeffentlich_and_ersatz_uuid(get_oeff_and_ersatz_UUID_endpoint):
        access_token = get_access_token()
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer ' + access_token
        }
        response = requests.get(get_oeff_and_ersatz_UUID_endpoint, headers=headers)
        response_json = response.json()
        return (response_json.get('oeffentlich').get('id'),response_json.get('ersatz').get('id'))
    
def get_school_dnr_uuid_mapping(get_organisation_endpoint):
        access_token = get_access_token()
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer ' + access_token
        }
        
        response = requests.get(get_organisation_endpoint+'?typ=SCHULE', headers=headers)
        response.raise_for_status()
        response_json = response.json()
        df = pd.DataFrame(response_json)
        df = df[['id', 'kennung']].rename(columns={'kennung': 'dnr'})
        return df
    
def get_class_nameAndAdministriertvon_uuid_mapping(get_organisation_endpoint):
        access_token = get_access_token()
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer ' + access_token
        }
        response = requests.get(get_organisation_endpoint+'?typ=KLASSE', headers=headers)
        response.raise_for_status()
        response_json = response.json()
        df = pd.DataFrame(response_json)
        df = df[['id', 'name', 'administriertVon']]
        return df
    
def get_rolle_id(get_role_endpoint, name):
    access_token = get_access_token()
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': 'Bearer ' + access_token
    }
    
    role_map = {
        'SuS': 'SuS',
        'Lehrkraft': 'Lehrkraft',
        'Schuladmin': 'Schuladmin',
        'Schulbegleitung': 'Schulbegleitung',
        'itslearning Admin': 'itslearning Admin',
        'itslearning Lehrkraft': 'itslearning Lehrkraft'
    }
    
    if name not in role_map:
        raise Exception("Invalid Role Name")
    
    response = requests.get(f"{get_role_endpoint}?searchStr={role_map[name]}", headers=headers)
    response.raise_for_status()
    response_json = response.json()
    
    if len(response_json) < 1:
        raise Exception("Response does not contain any elements")
    if len(response_json)>0:
        log(f"At least one result, filtering by name exactly: {name}")
        for element in response_json:
            if element['name'] == name:
                return element['id']
        raise Exception(f"Role {name} not found")
    
    return response_json[0].get('id')
    


def parse_dn(dn):
    parsed_dn = ldap.dn.str2dn(dn)
    dn_attributes = {}
    for rdn in parsed_dn:
        for attr, value, attr_type in rdn:
            attr = attr.lower()
            if attr not in dn_attributes:
                dn_attributes[attr] = []
            dn_attributes[attr].append(value)
    return dn_attributes

def get_is_school_object(parsedDN):
    if ('ou' in parsedDN) and ('uid' not in parsedDN) and ('cn' not in parsedDN) and ('dc' in parsedDN) and (parsedDN['dc'][0] == 'schule-sh') and (parsedDN['dc'][1] == 'de'):
        return True
    return False

def get_is_class_object(parsedDN):
    if ('ou' in parsedDN) and ('uid' not in parsedDN) and ('cn' in parsedDN) and (len(parsedDN['cn']) == 4) and ('klassen' in parsedDN['cn']) and ('schueler' in parsedDN['cn']) and ('groups' in parsedDN['cn']):
        return True
    return False

def get_is_uid_object(parsedDN):
    if 'uid' in parsedDN:
        return True
    return False

def get_is_itslearning_lehrer_or_admin_group_object(parsedDN, entry):
    if ('cn' in parsedDN) and ('groups' in parsedDN['cn']) and (
        any(cn_item.startswith('admins-') for cn_item in parsedDN['cn']) or 
        any(cn_item.startswith('lehrer-') for cn_item in parsedDN['cn'])
    ):
        if 'enabledServiceProviderIdentifierGroup' in entry:
            decoded_identifiers = [
                identifier_group.decode('utf-8') if isinstance(identifier_group, bytes) else identifier_group 
                for identifier_group in entry['enabledServiceProviderIdentifierGroup']
            ]
            if any('https://eu1.itslearning.com' in identifier_group for identifier_group in decoded_identifiers):
                return True
    return False

def get_hash_sha256_for_file(file_path):
    sha256 = hashlib.sha256()

    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            sha256.update(chunk)

    return sha256.hexdigest()