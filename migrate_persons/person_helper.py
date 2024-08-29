from datetime import datetime
import time
import os
import sys
import pandas as pd
import requests
import ldap.dn

from helper import get_access_token, log

def get_schools_dnr_for_create_admin_kontext(filtered_memberOf):
    admin_contexts = [mo.split('-', 1)[1] for mo in filtered_memberOf if mo.startswith('admin-')]
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


def create_person_api_call(create_person_post_endpoint, headers, email, sn, given_name, username, hashed_password, kopersnr):
    post_data_create_person = {
        "email": email,
        "name": {
            "familienname": sn,
            "vorname": given_name,
        },
        "username": username,
        "hashedPassword": hashed_password,
        "personalnummer":kopersnr
    }
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

def create_kontext_api_call(create_kontext_post_endpoint, headers, personId, organisationId, rolleId):
    post_data_create_kontext = {
        "personId": personId,
        "organisationId": organisationId,
        "rolleId": rolleId
    }
    
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
    result = df.loc[(df['name'].lower() == name.lower()) & (df['administriertVon'] == administriert_von), 'id']
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
        return (email, sn, given_name, kopersnr, username, hashed_password, memberOf_list)
    

def get_combinded_school_kontexts_to_create_for_person(filtered_memberOf, roleid_sus, roleid_lehrkraft, roleid_schuladmin, roleid_schulbegleitung, school_uuid_dnr_mapping):
    admin_kontexts = get_schools_dnr_for_create_admin_kontext(filtered_memberOf)
    lehrer_kontexts = get_schools_dnr_for_create_lehrer_kontext(filtered_memberOf)
    schueler_kontexts = get_schools_dnr_for_create_schueler_kontext(filtered_memberOf)
    schuelbegleitungs_kontexts = get_schools_dnr_for_create_schuelbegleiter_kontext(filtered_memberOf)
    combined_school_kontexts = [{'dnr': dnr, 'orgaId': get_orgaid_by_dnr(school_uuid_dnr_mapping, dnr), 'roleId': roleid_sus} for dnr in schueler_kontexts]
    combined_school_kontexts += [{'dnr': dnr, 'orgaId': get_orgaid_by_dnr(school_uuid_dnr_mapping, dnr), 'roleId': roleid_lehrkraft} for dnr in lehrer_kontexts]
    combined_school_kontexts += [{'dnr': dnr, 'orgaId': get_orgaid_by_dnr(school_uuid_dnr_mapping, dnr), 'roleId': roleid_schuladmin} for dnr in admin_kontexts]
    combined_school_kontexts += [{'dnr': dnr, 'orgaId': get_orgaid_by_dnr(school_uuid_dnr_mapping, dnr), 'roleId': roleid_schulbegleitung} for dnr in schuelbegleitungs_kontexts]
    
    return combined_school_kontexts

def log_skip(skipped_persons, is_skip_because_lehreradmin, is_skip_because_fvmadmin, 
    is_skip_because_iqsh, is_skip_because_deactive_and_not_lehrer, is_skip_because_schueler_without_klasse,
    number_of_total_skipped_api_calls, number_of_lehreradmin_skipped_api_calls, number_of_fvmadmin_skipped_api_calls,
    number_of_iqsh_skipped_api_calls, number_of_deactive_skipped_api_calls, number_of_schueler_without_klassen_skipped_api_calls,
    username, email, sn, given_name, memberOf_raw):
    
    number_of_total_skipped_api_calls += 1
    
    if is_skip_because_lehreradmin:
        status_code = 'NO_MIGRATION_LEHRERADMIN'
        number_of_lehreradmin_skipped_api_calls += 1
    elif is_skip_because_fvmadmin:
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
    return (number_of_total_skipped_api_calls, number_of_lehreradmin_skipped_api_calls, number_of_fvmadmin_skipped_api_calls, number_of_iqsh_skipped_api_calls, number_of_deactive_skipped_api_calls, number_of_schueler_without_klassen_skipped_api_calls)

def execute_merge(potential_merge_admins, potential_merge_into_lehrer, create_kontext_post_endpoint, personenkontexte_for_person_get_endpoint, school_uuid_dnr_mapping, roleid_lehrkraft, roleid_schuladmin):
    log('Now Starting With Lehrer - Admin Merge')
    migration_log = []
    number_of_potential_merge_from_admins = len(potential_merge_admins)
    number_of_successfully_merged_any_context_from_admin = 0
    
    number_of_no_corressponding_leher_found = 0
    number_of_corressponding_lehrer_found_but_missing_school_kontext = 0
    number_of_create_merge_kontext_api_calls = 0
    number_of_create_merge_kontext_api_error_response = 0
    
    for potential_merge_admin in potential_merge_admins:
        is_successfully_merged_any_context_from_admin = False
        matching_lehrer_person = None
        for lehrer in potential_merge_into_lehrer:
            if potential_merge_admin['given_name'] == lehrer['username']:
                matching_lehrer_person = lehrer
                break
        if matching_lehrer_person is None:
            migration_log.append({
                    'admin_username': potential_merge_admin['username'],
                    'admin_email': potential_merge_admin['email'],
                    'admin_familienname': potential_merge_admin['sn'],
                    'admin_vorname': potential_merge_admin['given_name'],
                    'admin_memberOf':potential_merge_admin['memberOf_raw'],
                    'lehrer_username': '',
                    'lehrer_email': '',
                    'lehrer_person_id': '',
                    'status':'COULD_NOT_MERGE_COMPLETLY',
                    'status_description':'No Corresponding Lehrer Found For This Admin Person'
            })
            number_of_no_corressponding_leher_found += 1
            continue

        dnr_list_mergefrom_admin_has_admin_role = [mo.split('-', 1)[1] for mo in potential_merge_admin['memberOf_list'] if mo.startswith('admins-')]
        access_token = get_access_token()
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer ' + access_token
        }
        if(len(dnr_list_mergefrom_admin_has_admin_role) > 0):
            response = requests.get(
                f"{personenkontexte_for_person_get_endpoint}/{matching_lehrer_person['person_id']}", 
                headers=headers
            )
            kontexts_for_merge_into_lehrer = response.json()
            for dnr_for_mergefrom_admin in dnr_list_mergefrom_admin_has_admin_role:
                orga_id_for_mergefrom_admin = get_orgaid_by_dnr(school_uuid_dnr_mapping, dnr_for_mergefrom_admin)
                merge_into_lehrer_has_correct_context_on_dnr = False
                for context in kontexts_for_merge_into_lehrer:
                    if context['organisationId'] == orga_id_for_mergefrom_admin and context['rolleId'] == roleid_lehrkraft:
                        merge_into_lehrer_has_correct_context_on_dnr = True
                if(merge_into_lehrer_has_correct_context_on_dnr == False):
                    migration_log.append({
                        'admin_username': potential_merge_admin['username'],
                        'admin_email': potential_merge_admin['email'],
                        'admin_familienname': potential_merge_admin['sn'],
                        'admin_vorname': potential_merge_admin['given_name'],
                        'admin_memberOf':potential_merge_admin['memberOf_raw'],
                        'lehrer_username': matching_lehrer_person['username'],
                        'lehrer_email': matching_lehrer_person['email'],
                        'lehrer_person_id': matching_lehrer_person['person_id'],
                        'status':'COULD_NOT_MERGE_SINGLE',
                        'status_description':f'Found Lehrer Has No Lehrer-Kontext on School with Id {orga_id_for_mergefrom_admin}, But Admin Role Was Found For This School'
                    })
                    number_of_corressponding_lehrer_found_but_missing_school_kontext += 1
                    continue
                
                response_create_merge_kontext = create_kontext_api_call(create_kontext_post_endpoint, headers, matching_lehrer_person['person_id'], orga_id_for_mergefrom_admin, roleid_schuladmin)
                number_of_create_merge_kontext_api_calls += 1
                if response_create_merge_kontext.status_code == 401:
                    log(f"Create-Kontext-Request - 401 Unauthorized error")
                    sys.exit()
                elif response_create_merge_kontext.status_code != 201:
                    number_of_create_merge_kontext_api_error_response += 1
                    migration_log.append({
                        'admin_username': potential_merge_admin['username'],
                        'admin_email': potential_merge_admin['email'],
                        'admin_familienname': potential_merge_admin['sn'],
                        'admin_vorname': potential_merge_admin['given_name'],
                        'admin_memberOf':potential_merge_admin['memberOf_raw'],
                        'lehrer_username': matching_lehrer_person['username'],
                        'lehrer_email': matching_lehrer_person['email'],
                        'lehrer_person_id': matching_lehrer_person['person_id'],
                        'status':'COULD_NOT_MERGE_SINGLE',
                        'status_description':f'Api Returned Error Response: {response_create_merge_kontext.json()}'
                    })
                else:
                    migration_log.append({
                        'admin_username': potential_merge_admin['username'],
                        'admin_email': potential_merge_admin['email'],
                        'admin_familienname': potential_merge_admin['sn'],
                        'admin_vorname': potential_merge_admin['given_name'],
                        'admin_memberOf':potential_merge_admin['memberOf_raw'],
                        'lehrer_username': matching_lehrer_person['username'],
                        'lehrer_email': matching_lehrer_person['email'],
                        'lehrer_person_id': matching_lehrer_person['person_id'],
                        'status':'MERGED_SINGLE',
                        'status_description': f"Merged Adminrole from user {potential_merge_admin['username']} on school with Id {orga_id_for_mergefrom_admin} Into Lehrer {matching_lehrer_person['username']} on same school"
                    })
                    is_successfully_merged_any_context_from_admin = True
        if is_successfully_merged_any_context_from_admin is True:
            number_of_successfully_merged_any_context_from_admin += 1
    return (migration_log,number_of_potential_merge_from_admins,number_of_successfully_merged_any_context_from_admin,
            number_of_no_corressponding_leher_found,number_of_corressponding_lehrer_found_but_missing_school_kontext,
            number_of_create_merge_kontext_api_calls,number_of_create_merge_kontext_api_error_response)

def print_standard_run_statistics(number_of_found_persons, number_of_total_skipped_api_calls, number_of_lehreradmin_skipped_api_calls, 
                     number_of_fvmadmin_skipped_api_calls, number_of_iqsh_skipped_api_calls, number_of_deactive_skipped_api_calls,
                     number_of_schueler_without_klassen_skipped_api_calls,
                     number_of_deactive_lehrer_api_calls, number_of_create_person_api_calls, number_of_create_person_api_error_responses, 
                     number_of_create_kontext_api_calls, number_of_create_kontext_api_error_responses, number_of_create_school_kontext_api_calls, 
                     number_of_create_school_kontext_api_error_responses, number_of_create_class_kontext_api_calls, number_of_create_class_kontext_api_error_responses):
    log("")
    log("")
    log("### STANDARD RUN STATISTICS ###")
    log("")
    log(f"Number of found Persons: {number_of_found_persons}")
    log(f"Number of Total Skipped Persons: {number_of_total_skipped_api_calls}")
    log(f"                Of That #ADMIN Persons: {number_of_lehreradmin_skipped_api_calls}")
    log(f"                Of That FVM-Admin Persons: {number_of_fvmadmin_skipped_api_calls}")
    log(f"                Of That IQSH Persons: {number_of_iqsh_skipped_api_calls}")
    log(f"                Of That Deactive Non LehrerPersons: {number_of_deactive_skipped_api_calls}")
    log(f"                Of That Schueler Without Any Klasse: {number_of_schueler_without_klassen_skipped_api_calls}")
    log(f"Number of Create-Person API Calls: {number_of_create_person_api_calls}")
    log(f"                Of That Deactive Lehrer Persons: {number_of_deactive_lehrer_api_calls}")
    log(f'Number of Create-Person API Error Responses: {number_of_create_person_api_error_responses}')
    log(f"Number of Total Create-Kontext API Calls: {number_of_create_kontext_api_calls}")
    log(f"                Of That School Kontexts: {number_of_create_school_kontext_api_calls}")
    log(f"                Of That Class Kontexts: {number_of_create_class_kontext_api_calls}")
    log(f'Number of TotalCreate-Kontext API Error Responses: {number_of_create_kontext_api_error_responses}')
    log(f"                Of That School Kontexts: {number_of_create_school_kontext_api_error_responses}")
    log(f"                Of That Class Kontexts: {number_of_create_class_kontext_api_error_responses}")
    log("")
    
def print_merge_run_statistics(number_of_potential_merge_from_admins,number_of_successfully_merged_any_context_from_admin, 
                               number_of_no_corressponding_leher_found,number_of_corressponding_lehrer_found_but_missing_school_kontext,
                               number_of_create_merge_kontext_api_calls,number_of_create_merge_kontext_api_error_response):
    log("")
    log("")
    log("### MERGE RUN STATISTICS ###")
    log("")
    log(f"Number of Potential Mergable Admins: {number_of_potential_merge_from_admins}")
    log(f"                Of That Any Kontext Has Been Merged Successfully: {number_of_successfully_merged_any_context_from_admin}")
    log(f"Number Where No Coressponding Lehrer Has Been Found: {number_of_no_corressponding_leher_found}")
    log(f"Number Where Coressponding Lehrer Has Been Found, But Is Missing Lehrer Role On Desired Merge School: {number_of_corressponding_lehrer_found_but_missing_school_kontext}")
    log(f"Number of Total Create Merge Kontext Api Calls: {number_of_create_merge_kontext_api_calls}")
    log(f"Number of Total Create Merge Kontext Api Error Responses: {number_of_create_merge_kontext_api_error_response}")
    log("")
    
def create_and_save_log_excel(log_output_dir, skipped_persons, failed_responses_create_person, failed_responses_create_kontext, schueler_on_school_without_klasse, other_log, migration_log):
    # Convert the list of failed responses to a DataFrame and save to an Excel file
    skipped_persons_df = pd.DataFrame(skipped_persons)
    failed_responses_create_person_df = pd.DataFrame(failed_responses_create_person)
    failed_responses_create_kontext_df = pd.DataFrame(failed_responses_create_kontext)
    schueler_on_school_without_klasse_df = pd.DataFrame(schueler_on_school_without_klasse)
    other_log_df = pd.DataFrame(other_log)
    migration_log_df = pd.DataFrame(migration_log)
    
    os.makedirs(log_output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    excel_path = os.path.join(log_output_dir, f'migrate_persons_log_{timestamp}.xlsx')
    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            skipped_persons_df.to_excel(writer, sheet_name='Skipped_Persons', index=False)
            failed_responses_create_person_df.to_excel(writer, sheet_name='Failed_Api_Create_Person', index=False)
            failed_responses_create_kontext_df.to_excel(writer, sheet_name='Failed_Api_Create_Kontext', index=False)
            schueler_on_school_without_klasse_df.to_excel(writer, sheet_name='Schueler_On_School_No_Class', index=False)
            migration_log_df.to_excel(writer, sheet_name='Migration', index=False)
            other_log_df.to_excel(writer, sheet_name='Other', index=False)
        log(f"Failed API responses have been saved to '{excel_path}'.")
        log(f"Check the current working directory: {os.getcwd()}")
    except Exception as e:
        log(f"An error occurred while saving the Excel file: {e}")