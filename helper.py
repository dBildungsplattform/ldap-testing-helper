import os
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

    token_response = requests.post(token_url, data=payload, headers=token_headers)
    token_response_json = token_response.json()
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