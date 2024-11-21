from datetime import datetime
import hashlib
import itertools
import tempfile
import time
import os
import pandas as pd
import requests
import ldap.dn

# This File contains  all Helpers that can be used by all 4 modules (migrate_schools, migrate_classes, migrate_persons & migrate_itslearning_affiliation)

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

def get_oeffentlich_and_ersatz_uuid(api_backend_orga_root_children):
        access_token = get_access_token()
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer ' + access_token
        }
        response = requests.get(api_backend_orga_root_children, headers=headers)
        response_json = response.json()
        return (response_json.get('oeffentlich').get('id'),response_json.get('ersatz').get('id'))
    
def get_school_dnr_uuid_mapping(get_organisation_endpoint, api_backend_orga_root_children):
    
    (oeffentlich_uuid, ersatz_uuid) = get_oeffentlich_and_ersatz_uuid(api_backend_orga_root_children)

    access_token = get_access_token()
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': 'Bearer ' + access_token
    }
    
    response = requests.get(get_organisation_endpoint+'?typ=SCHULE', headers=headers)
    response.raise_for_status()
    response_json = response.json()
    df = pd.DataFrame(response_json)
    df = df[['id', 'kennung', 'administriertVon']].rename(columns={'kennung': 'dnr'})
    
    def determine_school_type(uuid):
        if uuid == oeffentlich_uuid:
            print(F"{uuid} : OEFFENTLICH")
            return 'OEFFENTLICH'
        elif uuid == ersatz_uuid:
            print(F"{uuid} : ERSATZ")
            return 'ERSATZ'
        else:
            raise ValueError(f"Unrecognized UUID for 'administriertVon': {uuid}")
    
    df['school_type'] = df['administriertVon'].apply(determine_school_type)
    df = df.drop(columns=['administriertVon'])
    print(df)
    return df
    
def get_class_name_and_administriertvon_uuid_mapping(get_organisation_endpoint):
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
    
def get_orgaid_by_dnr(mapping_df, dnr_to_search_for):
    result = mapping_df.loc[mapping_df['dnr'] == dnr_to_search_for, 'id']
    if not result.empty:
        return result.iloc[0]
    else:
        log(f"No matching id found for dnr: {dnr_to_search_for}")
        return None
    
def get_rolle_id(get_role_endpoint, name):
    access_token = get_access_token()
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': 'Bearer ' + access_token
    }
    
    response = requests.get(f"{get_role_endpoint}?searchStr={name}", headers=headers)
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

def get_is_school_object(parsedDN, entry):
    objectClass = [
        cls.decode('utf-8') if isinstance(cls, bytes) else cls
        for cls in entry.get('objectClass', [])
    ]
    if (
        'ou' in parsedDN and
        'uid' not in parsedDN and
        'cn' not in parsedDN and
        'dc' in parsedDN and
        'ucsschoolOrganizationalUnit' in objectClass
    ):
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

def parse_and_convert_tojsdate(date_string):
    date_format = "%Y%m%d%H%M%SZ"
    try:
        dt = datetime.strptime(date_string, date_format)
        js_date_string = dt.isoformat() + 'Z'
        return js_date_string

    except ValueError:
        raise ValueError(f"Invalid date format: '{date_string}'. Expected format: '{date_format}'.")
    
def save_to_excel(log_data_dict, log_output_dir, filename_prefix, sheet_names=None):
    """
    Saves provided log data into an Excel file with multiple sheets (if applicable).

    :param log_data_dict: A dictionary where keys are DataFrame names and values are DataFrames to be saved.
    :param log_output_dir: Directory where the log file will be saved.
    :param filename_prefix: Prefix for the generated Excel file.
    :param sheet_names: Optional list of sheet names to use for the DataFrames. If None, default keys of log_data_dict are used.
    """
    os.makedirs(log_output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    excel_path = os.path.join(log_output_dir, f'{filename_prefix}_{timestamp}.xlsx')

    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            for i, (df_name, df) in enumerate(log_data_dict.items()):
                sheet_name = sheet_names[i] if sheet_names and i < len(sheet_names) else df_name
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        log(f"Log responses have been saved to '{excel_path}'.")
        log(f"Check the current working directory: {os.getcwd()}")
    except Exception as e:
        log(f"An error occurred while saving the Excel file: {e}")
        
def create_kontext_api_call(migration_run_type, api_backend_dbiam_personenkontext, headers, person_id, username, organisation_id, rolle_id, email, befristung_valid_jsdate):
    post_data_create_kontext = {
        "personId": person_id,
        "username":username,
        "organisationId": organisation_id,
        "rolleId": rolle_id,
        "email":email,
        "befristung":befristung_valid_jsdate,
        "migrationRunType":migration_run_type
    }
    log(f"Create Kontext Request Body: {post_data_create_kontext}")
    
    attempt = 1
    while attempt < 5:
        try:
            response_create_kontext = requests.post(api_backend_dbiam_personenkontext, json=post_data_create_kontext, headers=headers)
            return response_create_kontext
        except requests.RequestException as e:
            attempt += 1
            log(f"Create Kontext Request Attempt {attempt} failed: {e}. Retrying...")
            time.sleep(5*attempt) #Exponential Backoff
    
    raise Exception("Max retries exceeded. The request failed.")

def enable_itslearning_for_orga_api_call(headers, organisation_base_endpoint, organisation_id):
    put_data = {}
    url = f"{organisation_base_endpoint}/{organisation_id}/enable-for-its-learning"
    attempt = 1
    while attempt < 5:
        try:
            response_create_kontext = requests.put(url, json=put_data, headers=headers)
            return response_create_kontext
        except requests.RequestException as e:
            attempt += 1
            log(f"Enable Itslearning Request Attempt {attempt} failed: {e}. Retrying...")
            time.sleep(5*attempt) #Exponential Backoff
    
    raise Exception("Max retries exceeded. The request failed.")


def chunk_input_file(personsDataInputLDAP):
    temp_file_paths = []  # Store paths instead of file handles
    temp_files = (tempfile.NamedTemporaryFile(mode="w+t", prefix="personen_import", delete=False) for _ in itertools.count())

    MAX_DATASETS_PER_FILE = 100000
    datasetCounter = 0
    current_tmp_file = next(temp_files)
    temp_file_paths.append(current_tmp_file.name)
    with open(personsDataInputLDAP) as ldifFile:
        for line in ldifFile:
            if line == "\n":
                datasetCounter += 1
            if datasetCounter < MAX_DATASETS_PER_FILE:
                current_tmp_file.write(line)
            else:
                datasetCounter = 0
                current_tmp_file.close()
                current_tmp_file = next(temp_files)
                temp_file_paths.append(current_tmp_file.name)
    current_tmp_file.close()
    return temp_file_paths
