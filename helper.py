import os
import pandas as pd
import requests
from dotenv import load_dotenv
import ldap.dn

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
    print(token_url)
    print(payload)
    token_response = requests.post(token_url, data=payload, headers=token_headers)
    token_response_json = token_response.json()
    print(token_response_json)
    token = token_response_json.get('access_token')
    return token

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
        'Schulbegleitung': 'Schulbegleitung'
    }
    
    if name not in role_map:
        raise Exception("Invalid Role Name")
    
    response = requests.get(f"{get_role_endpoint}?searchStr={role_map[name]}", headers=headers)
    response.raise_for_status()
    response_json = response.json()
    
    if len(response_json) != 1:
        raise Exception("Response does not contain exactly one element")
    
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