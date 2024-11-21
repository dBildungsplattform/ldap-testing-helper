import time
import requests
import ldap.dn

from helper import get_orgaid_by_dnr, log

def get_schools_dnr_for_create_admin_kontext(filtered_memberOf):
    admin_contexts = [mo.split('-', 1)[1] for mo in filtered_memberOf if mo.startswith('admins-')]
    return admin_contexts

def get_schools_dnr_for_create_schueler_kontext(filtered_memberOf, unfiltered_memberOf): #Wenn Schüler aber Klasse Schulbegleiter Zugeordnet, dann ist man nicht Schüler sonern SBGL
    schueler_contexts = [mo.split('-', 1)[1] for mo in filtered_memberOf if mo.startswith('schueler-')]
    schueler_contexts = [
        dnr for dnr in schueler_contexts 
        if not any(um.startswith(f"{dnr}-Schulbegleitung") for um in unfiltered_memberOf)
    ]
    return schueler_contexts

def get_schools_dnr_for_create_schuelbegleiter_kontext(filtered_memberOf, unfiltered_memberOf): #Wenn Schüler aber Klasse Schulbegleiter Zugeordnet, dann ist man nicht Schüler sonern SBGL
    schueler_contexts = [mo.split('-', 1)[1] for mo in filtered_memberOf if mo.startswith('schueler-')]
    result = [
        dnr for dnr in schueler_contexts 
        if any(um.startswith(f"{dnr}-Schulbegleitung") for um in unfiltered_memberOf)
    ]
    return result

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
    result = df.loc[df['dnr'].str.lower() == dnr.lower(), 'id']
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
        email = row['mailPrimaryAddress'].decode('utf-8') if isinstance(row['mailPrimaryAddress'], bytes) else row['mailPrimaryAddress']
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
    

def get_combinded_school_kontexts_to_create_for_person(
    created_person_id,
    filtered_memberOf,
    unfiltered_memberOf,
    roleid_itslearning_sus, 
    roleid_lehrkraft,
    roleid_lehrkraft_ersatz, 
    roleid_schuladmin_oeffentlich,
    roleid_schuladmin_ersatz,
    roleid_schulbegleitung, 
    school_uuid_dnr_mapping
):
    def getCorrectLehrkaftRolleId(dnr):
        school_type_value = school_uuid_dnr_mapping.loc[school_uuid_dnr_mapping['dnr'] == dnr, 'school_type'].values
        if school_type_value == None or school_type_value.size == 0 or school_type_value[0] is None:
            log(f"Could Not determine CorrectLehrkaftRolleId for personId: {created_person_id}")
            return None
        else:
            school_type = school_type_value[0]
            if school_type == 'ERSATZ':
                return roleid_lehrkraft_ersatz
            else:
                return roleid_lehrkraft
        
    def getCorrectSchuladminRolleId(dnr):
        school_type_value = school_uuid_dnr_mapping.loc[school_uuid_dnr_mapping['dnr'] == dnr, 'school_type'].values
        if school_type_value == None or school_type_value.size == 0 or school_type_value[0] is None:
            log(f"Could Not determine CorrectSchuladminRolleId for personId: {created_person_id}")
            return None
        else:
            school_type = school_type_value[0]
            if school_type == 'ERSATZ':
                return roleid_schuladmin_ersatz
            else:
                return roleid_schuladmin_oeffentlich
    
    admin_kontexts = get_schools_dnr_for_create_admin_kontext(filtered_memberOf)
    lehrer_kontexts = get_schools_dnr_for_create_lehrer_kontext(filtered_memberOf)
    schueler_kontexts = get_schools_dnr_for_create_schueler_kontext(filtered_memberOf, unfiltered_memberOf)
    schuelbegleitungs_kontexts = get_schools_dnr_for_create_schuelbegleiter_kontext(filtered_memberOf, unfiltered_memberOf)
    combined_school_kontexts = [{'dnr': dnr, 'orgaId': get_orgaid_by_dnr(school_uuid_dnr_mapping, dnr), 'roleId': roleid_itslearning_sus, 'type':'SCHUELER'} for dnr in schueler_kontexts]
    combined_school_kontexts += [{'dnr': dnr, 'orgaId': get_orgaid_by_dnr(school_uuid_dnr_mapping, dnr), 'roleId': getCorrectLehrkaftRolleId(dnr), 'type':'LEHRER'} for dnr in lehrer_kontexts]
    combined_school_kontexts += [{'dnr': dnr, 'orgaId': get_orgaid_by_dnr(school_uuid_dnr_mapping, dnr), 'roleId': getCorrectSchuladminRolleId(dnr), 'type':'ADMIN'} for dnr in admin_kontexts]
    combined_school_kontexts += [{'dnr': dnr, 'orgaId': get_orgaid_by_dnr(school_uuid_dnr_mapping, dnr), 'roleId': roleid_schulbegleitung, 'type':'SCHULBEGLEITUNG'} for dnr in schuelbegleitungs_kontexts]
    
    return combined_school_kontexts

def log_skip(skipped_persons, is_skip_because_fvmadmin, 
    is_skip_because_iqsh, is_skip_because_deactive_and_not_lehrer_or_admin, is_skip_because_schueler_without_klasse,
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
    elif is_skip_because_deactive_and_not_lehrer_or_admin:
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
