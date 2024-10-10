import time
import requests
import ldap.dn

from helper import log

def get_schools_dnr_for_create_admin_kontext(filtered_memberOf):
    admin_contexts = [mo.split('-', 1)[1] for mo in filtered_memberOf if mo.startswith('admins-')]
    return admin_contexts

def get_schools_dnr_for_create_schueler_kontext(filtered_memberOf):
    schueler_contexts = [mo.split('-', 1)[1] for mo in filtered_memberOf if mo.startswith('schueler-')]
    return schueler_contexts

def get_schools_dnr_for_create_schuelbegleiter_kontext(filtered_memberOf):
    schulbegleitungs_contexts = [mo.split('-', 1)[0] for mo in filtered_memberOf if mo.endswith('-Schulbegleitung')]
    return schulbegleitungs_contexts

def get_schools_dnr_for_create_lehrer_kontext(filtered_memberOf):
    lehrer_contexts = [mo.split('-', 1)[1] for mo in filtered_memberOf if mo.startswith('lehrer-')]
    return lehrer_contexts


def create_person_api_call(create_person_post_endpoint, headers, person_id, sn, given_name, username, hashed_password, kopersnr):
    post_data_create_person = {
        "personId":person_id,
        "familienname": sn,
        "vorname": given_name,
        "username": username,
        "hashedPassword": hashed_password,
        "personalnummer":kopersnr
    }
    log(f"Create Person Request Body: {post_data_create_person}")
    attempt = 1
    while attempt < 5:
        try:
            response_create_person = requests.post(create_person_post_endpoint, json=post_data_create_person, headers=headers)
            return response_create_person
        except requests.RequestException as e:
            attempt += 1
            log(f"Create Person Request Attempt {attempt} failed: {e}. Retrying...")
            time.sleep(5*attempt) #Exponential Backof
    
    raise Exception("Max retries exceeded. The request failed.")

def create_kontext_api_call(migration_run_type, create_kontext_post_endpoint, headers, person_id, username, organisation_id, rolle_id, email):
    post_data_create_kontext = {
        "personId": person_id,
        "username":username,
        "organisationId": organisation_id,
        "rolleId": rolle_id,
        "email":email,
        "migrationRunType":migration_run_type
    }
    log(f"Create Kontext Request Body: {post_data_create_kontext}")
    
    attempt = 1
    while attempt < 5:
        try:
            response_create_kontext = requests.post(create_kontext_post_endpoint, json=post_data_create_kontext, headers=headers)
            return response_create_kontext
        except requests.RequestException as e:
            attempt += 1
            log(f"Create Kontext Request Attempt {attempt} failed: {e}. Retrying...")
            time.sleep(5*attempt) #Exponential Backof
    
    raise Exception("Max retries exceeded. The request failed.")

def get_orgaid_by_dnr(df, dnr):
    result = df.loc[df['dnr'] == dnr, 'id']
    if not result.empty:
        return result.iloc[0]
    else:
        log(f"No matching id found for dnr: {dnr}")
        return None
    
def get_orgaid_by_className_and_administriertvon(df, name, administriert_von, username_created_kontext_for):
    result = df.loc[(df['name'].str.lower() == name.lower()) & (df['administriertVon'] == administriert_von), 'id']
    if not result.empty:
        return result.iloc[0]
    else:
        log(f"No matching class_id found for classname: {name} and administriert_von: {administriert_von}, when trying to create class kontext for username: {username_created_kontext_for}")
        return None
    
    
def convert_data_from_row(row, other_log):
        email = row['krb5PrincipalName'].decode('utf-8') if isinstance(row['krb5PrincipalName'], bytes) else row['krb5PrincipalName']
        sn = row['sn'].decode('utf-8') if isinstance(row['sn'], bytes) else row['sn']
        given_name = row['givenName'].decode('utf-8') if isinstance(row['givenName'], bytes) else row['givenName']
        kopersnr = row['ucsschoolRecordUID'].decode('utf-8') if isinstance(row['ucsschoolRecordUID'], bytes) else row['ucsschoolRecordUID']
        username = row['uid'].decode('utf-8') if isinstance(row['uid'], bytes) else row['uid']
        hashed_password = row['userPassword'].decode('utf-8') if isinstance(row['userPassword'], bytes) else row['userPassword']
        entry_uuid = row['entryUUID'].decode('utf-8') if isinstance(row['entryUUID'], bytes) else row['entryUUID']
        memberOf = [singleMemberOf.decode('utf-8') if isinstance(singleMemberOf, bytes) else singleMemberOf for singleMemberOf in row['memberOf']]
        
        memberOf_list = []
        for singleMemberOf in memberOf:
            try:
                memberOf_list.append(ldap.dn.str2dn(singleMemberOf)[0][0][1])
            except IndexError:
                log(f"Warning: Malformed DN entry found: {singleMemberOf}")
                other_log.append({
                    'type':'MALFORMED_MEMBER_OF',
                    'memberOfs':memberOf,
                    'singleMemberOfCausingError':singleMemberOf
                })
                continue
        return (entry_uuid, email, sn, given_name, kopersnr, username, hashed_password, memberOf_list)
    

def get_combinded_school_kontexts_to_create_for_person(filtered_memberOf, roleid_sus, roleid_lehrkraft, roleid_schuladmin, roleid_schulbegleitung, school_uuid_dnr_mapping):
    admin_kontexts = get_schools_dnr_for_create_admin_kontext(filtered_memberOf)
    lehrer_kontexts = get_schools_dnr_for_create_lehrer_kontext(filtered_memberOf)
    schueler_kontexts = get_schools_dnr_for_create_schueler_kontext(filtered_memberOf)
    schuelbegleitungs_kontexts = get_schools_dnr_for_create_schuelbegleiter_kontext(filtered_memberOf)
    combined_school_kontexts = [{'dnr': dnr, 'orgaId': get_orgaid_by_dnr(school_uuid_dnr_mapping, dnr), 'roleId': roleid_sus, 'type':'SCHUELER'} for dnr in schueler_kontexts]
    combined_school_kontexts += [{'dnr': dnr, 'orgaId': get_orgaid_by_dnr(school_uuid_dnr_mapping, dnr), 'roleId': roleid_lehrkraft, 'type':'LEHRER'} for dnr in lehrer_kontexts]
    combined_school_kontexts += [{'dnr': dnr, 'orgaId': get_orgaid_by_dnr(school_uuid_dnr_mapping, dnr), 'roleId': roleid_schuladmin, 'type':'ADMIN'} for dnr in admin_kontexts]
    combined_school_kontexts += [{'dnr': dnr, 'orgaId': get_orgaid_by_dnr(school_uuid_dnr_mapping, dnr), 'roleId': roleid_schulbegleitung, 'type':'SCHULBEGLEITUNG'} for dnr in schuelbegleitungs_kontexts]
    
    return combined_school_kontexts

def log_skip(skipped_persons, is_skip_because_fvmadmin, 
    is_skip_because_iqsh, is_skip_because_deactive_and_not_lehrer, is_skip_because_schueler_without_klasse,
    number_of_total_skipped_api_calls, number_of_fvmadmin_skipped_api_calls,
    number_of_iqsh_skipped_api_calls, number_of_deactive_skipped_api_calls, number_of_schueler_without_klassen_skipped_api_calls,
    username, email, sn, given_name, memberOf_raw):
    
    number_of_total_skipped_api_calls += 1
    
    if is_skip_because_fvmadmin:
        status_code = 'NO_MIGRATION_FVM_ADMIN'
        number_of_fvmadmin_skipped_api_calls += 1
    elif is_skip_because_iqsh:
        status_code = 'NO_MIGRATION_IQSH'
        number_of_iqsh_skipped_api_calls += 1
    elif is_skip_because_deactive_and_not_lehrer:
        status_code = 'NO_MIGRATION_DEACTIVATED_ACCOUNT'
        number_of_deactive_skipped_api_calls += 1
    elif(is_skip_because_schueler_without_klasse):
        status_code = 'NO_MIGRATION_SCHUELER_WITHOUT_KLASSE'
        number_of_schueler_without_klassen_skipped_api_calls += 1
    skipped_persons.append({
            'username': username,
            'email': email,
            'familienname': sn,
            'vorname': given_name,
            'skipped_reason': status_code,
            'member_of_raw': memberOf_raw
        })
    return (number_of_total_skipped_api_calls, number_of_fvmadmin_skipped_api_calls, number_of_iqsh_skipped_api_calls, number_of_deactive_skipped_api_calls, number_of_schueler_without_klassen_skipped_api_calls)