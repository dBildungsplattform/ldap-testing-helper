import time
import requests
import ldap.dn

from helper import get_orgaid_by_dnr, log

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


def create_person_api_call(api_backend_personen, headers, person_id, sn, given_name, username, hashed_password, kopersnr):
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
            response_create_person = requests.post(api_backend_personen, json=post_data_create_person, headers=headers)
            return response_create_person
        except requests.RequestException as e:
            attempt += 1
            log(f"Create Person Request Attempt {attempt} failed: {e}. Retrying...")
            time.sleep(5*attempt) #Exponential Backoff
    
    raise Exception("Max retries exceeded. The request failed.")

def get_orgaid_by_dnr(df, dnr):
    result = df.loc[df['dnr'] == dnr, 'id']
    if not result.empty:
        return result.iloc[0]
    else:
        log(f"No matching id found for dnr: {dnr}")
        return None
    
def get_orgaid_by_className_and_administriertvon(mapping_df, class_name, administriert_von):
    result = mapping_df.loc[(mapping_df['name'].str.lower() == class_name.lower()) & (mapping_df['administriertVon'] == administriert_von), 'id']
    if not result.empty:
        return result.iloc[0]
    else:
        log(f"No matching class_id found for classname: {class_name} and administriert_von: {administriert_von}")
        return None
    
    
def convert_data_from_row(row, other_log):
        email = row['krb5PrincipalName'].decode('utf-8') if isinstance(row['krb5PrincipalName'], bytes) else row['krb5PrincipalName']
        sn = row['sn'].decode('utf-8') if isinstance(row['sn'], bytes) else row['sn']
        given_name = row['givenName'].decode('utf-8') if isinstance(row['givenName'], bytes) else row['givenName']
        kopersnr = row['ucsschoolRecordUID'].decode('utf-8') if isinstance(row['ucsschoolRecordUID'], bytes) else row['ucsschoolRecordUID']
        username = row['uid'].decode('utf-8') if isinstance(row['uid'], bytes) else row['uid']
        hashed_password = row['userPassword'].decode('utf-8') if isinstance(row['userPassword'], bytes) else row['userPassword']
        entry_uuid = row['entryUUID'].decode('utf-8') if isinstance(row['entryUUID'], bytes) else row['entryUUID']
        befristung = row['krb5ValidEnd'].decode('utf-8') if isinstance(row['krb5ValidEnd'], bytes) else row['krb5ValidEnd']
        memberOf = [singleMemberOf.decode('utf-8') if isinstance(singleMemberOf, bytes) else singleMemberOf for singleMemberOf in row['memberOf']]
        
        memberOf_list = []
        for singleMemberOf in memberOf:
            try:
                memberOf_list.append(ldap.dn.str2dn(singleMemberOf)[0][0][1])
            except IndexError:
                log(f"Warning: Malformed DN entry found: {singleMemberOf}")
                other_log.append({
                    'username':username,
                    'type':'MALFORMED_MEMBER_OF',
                    'memberOfs':memberOf,
                    'singleMemberOfCausingError':singleMemberOf
                })
                continue
        return (entry_uuid, email, sn, given_name, kopersnr, username, befristung, hashed_password, memberOf_list)
    

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