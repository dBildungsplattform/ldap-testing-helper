import os
import pandas as pd
import requests
import ldap.dn

def get_schools_dnr_for_create_admin_kontext(filtered_memberOf):
    admin_contexts = [mo.split('-', 1)[1] for mo in filtered_memberOf if mo.startswith('admin-')]
    return admin_contexts

def get_schools_dnr_for_create_schueler_kontext(filtered_memberOf):
    schueler_contexts = [mo.split('-', 1)[1] for mo in filtered_memberOf if mo.startswith('schueler-')]
    return schueler_contexts

def get_schools_dnr_for_create_lehrer_kontext(filtered_memberOf):
    lehrer_contexts = [mo.split('-', 1)[1] for mo in filtered_memberOf if mo.startswith('lehrer-')]
    return lehrer_contexts


def create_person_api_call(create_person_post_endpoint, headers, email, sn, given_name, username, hashed_password):
    post_data_create_person = {
        "email": email,
        "name": {
            "familienname": sn,
            "vorname": given_name,
        },
        "username": username,
        "hashedPassword": hashed_password,
    }
    response_create_person = requests.post(create_person_post_endpoint, json=post_data_create_person, headers=headers)
    return response_create_person

def create_kontext_api_call(create_kontext_post_endpoint, headers, personId, organisationId, rolleId):
    post_data_create_kontext = {
        "personId": personId,
        "organisationId": organisationId,
        "rolleId": rolleId
    }
    response_create_kontext = requests.post(create_kontext_post_endpoint, json=post_data_create_kontext, headers=headers)
    return response_create_kontext

def get_orgaid_by_dnr(df, dnr):
    result = df.loc[df['dnr'] == dnr, 'id']
    if not result.empty:
        return result.iloc[0]
    else:
        print(f"No matching id found for dnr: {dnr}")
        return None
    
    
def convert_data_from_row(row, other_log):
        email = row['krb5PrincipalName'].decode('utf-8') if isinstance(row['krb5PrincipalName'], bytes) else row['krb5PrincipalName']
        sn = row['sn'].decode('utf-8') if isinstance(row['sn'], bytes) else row['sn']
        given_name = row['givenName'].decode('utf-8') if isinstance(row['givenName'], bytes) else row['givenName']
        username = row['uid'].decode('utf-8') if isinstance(row['uid'], bytes) else row['uid']
        hashed_password = row['userPassword'].decode('utf-8') if isinstance(row['userPassword'], bytes) else row['userPassword']
        memberOf = [singleMemberOf.decode('utf-8') if isinstance(singleMemberOf, bytes) else singleMemberOf for singleMemberOf in row['memberOf']]
        
        memberOf_list = []
        for singleMemberOf in memberOf:
            try:
                memberOf_list.append(ldap.dn.str2dn(singleMemberOf)[0][0][1])
            except IndexError:
                print(f"Warning: Malformed DN entry found: {singleMemberOf}")
                other_log.append({
                    'type':'MALFORMED_MEMBER_OF',
                    'memberOfs':memberOf,
                    'singleMemberOfCausingError':singleMemberOf
                })
                continue
        return (email, sn, given_name, username, hashed_password, memberOf_list)
    

def get_combinded_kontexts_to_create(filtered_memberOf, roleid_sus, roleid_lehrkraft, roleid_schuladmin):
    admin_kontexts = get_schools_dnr_for_create_admin_kontext(filtered_memberOf)
    lehrer_kontexts = get_schools_dnr_for_create_lehrer_kontext(filtered_memberOf)
    schueler_kontexts = get_schools_dnr_for_create_schueler_kontext(filtered_memberOf) 
    combined_kontexts = [{'dnr': dnr, 'roleId': roleid_sus} for dnr in schueler_kontexts]
    combined_kontexts += [{'dnr': dnr, 'roleId': roleid_lehrkraft} for dnr in lehrer_kontexts]
    combined_kontexts += [{'dnr': dnr, 'roleId': roleid_schuladmin} for dnr in admin_kontexts]
    return combined_kontexts

def log_skip(
    skipped_persons, is_skip_because_lehreradmin, is_skip_because_fvmadmin, 
    is_skip_because_iqsh, is_skip_because_deactive, 
    number_of_total_skipped_api_calls, number_of_lehreradmin_skipped_api_calls, number_of_fvmadmin_skipped_api_calls,
    number_of_iqsh_skipped_api_calls, number_of_deactive_skipped_api_calls,
    username, email, sn, given_name):
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
    elif is_skip_because_deactive:
        status_code = 'NO_MIGRATION_DEACTIVATED_ACCOUNT'
        number_of_deactive_skipped_api_calls += 1
    skipped_persons.append({
            'username': username,
            'email': email,
            'familienname': sn,
            'vorname': given_name,
            'skipped_reason': status_code
        })
    return (number_of_total_skipped_api_calls, number_of_lehreradmin_skipped_api_calls, number_of_fvmadmin_skipped_api_calls, number_of_iqsh_skipped_api_calls, number_of_deactive_skipped_api_calls)

def print_run_statistics(number_of_found_persons, number_of_total_skipped_api_calls, number_of_lehreradmin_skipped_api_calls, 
                     number_of_fvmadmin_skipped_api_calls, number_of_iqsh_skipped_api_calls, number_of_deactive_skipped_api_calls, 
                     number_of_create_person_api_calls, number_of_create_person_api_error_responses, number_of_create_kontext_api_calls,
                     number_of_create_kontext_api_error_responses):
    print("")
    print("###STATISTICS###")
    print("")
    print(f"Number of found Persons: {number_of_found_persons}")
    print(f"Number of Total Skipped Persons: {number_of_total_skipped_api_calls}")
    print(f"                Skipped #ADMIN Persons: {number_of_lehreradmin_skipped_api_calls}")
    print(f"                Skipped FVM-Admin Persons: {number_of_fvmadmin_skipped_api_calls}")
    print(f"                Skipped IQSH Persons: {number_of_iqsh_skipped_api_calls}")
    print(f"                Skipped Deactive Persons: {number_of_deactive_skipped_api_calls}")
    print(f"Number of Create-Person API Calls: {number_of_create_person_api_calls}")
    print(f'Number of Create-Person API Error Responses: {number_of_create_person_api_error_responses}')
    print(f"Number of Create-Kontext API Calls: {number_of_create_kontext_api_calls}")
    print(f'Number of Create-Kontext API Error Responses: {number_of_create_kontext_api_error_responses}')
    print("")
    print("End Migration Person Data")
    
def create_and_save_log_excel(skipped_persons, failed_responses_create_person, failed_responses_create_kontext, other_log):
    # Convert the list of failed responses to a DataFrame and save to an Excel file
    skipped_persons_df = pd.DataFrame(skipped_persons)
    failed_responses_create_person_df = pd.DataFrame(failed_responses_create_person)
    failed_responses_create_kontext_df = pd.DataFrame(failed_responses_create_kontext)
    other_log_df = pd.DataFrame(other_log)
    
    output_dir = '/usr/src/app/output'
    os.makedirs(output_dir, exist_ok=True)
    excel_path = os.path.join(output_dir, 'log.xlsx')
    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            skipped_persons_df.to_excel(writer, sheet_name='Skipped_Persons', index=False)
            failed_responses_create_person_df.to_excel(writer, sheet_name='Failed_Api_Create_Person', index=False)
            failed_responses_create_kontext_df.to_excel(writer, sheet_name='Failed_Api_Create_Kontext', index=False)
            other_log_df.to_excel(writer, sheet_name='Other', index=False)
        print(f"Failed API responses have been saved to '{excel_path}'.")
        print(f"Check the current working directory: {os.getcwd()}")
    except Exception as e:
        print(f"An error occurred while saving the Excel file: {e}")