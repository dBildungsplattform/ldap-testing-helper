import sys
import pandas as pd
import requests
from helper import get_access_token, get_rolle_id, get_school_dnr_uuid_mapping
from datetime import datetime, timedelta
from migrate_persons.person_helper import convert_data_from_row, create_and_save_log_excel, create_kontext_api_call, create_person_api_call, execute_merge, get_combinded_kontexts_to_create, get_orgaid_by_dnr, log_skip, print_merge_run_statistics, print_standard_run_statistics
from migrate_persons.person_ldif_parser import BuildPersonDFLDIFParser
                
def migrate_person_data(create_person_post_endpoint, create_kontext_post_endpoint, input_path_ldap, schools_get_endpoint, roles_get_endpoint, personenkontexte_for_person_get_endpoint):
    print(f"Start Migration School Data")
    
    with open(input_path_ldap, 'rb') as input_file:
        parser = BuildPersonDFLDIFParser(input_file)
        parser.parse()
    df_ldap = pd.DataFrame(parser.entries_list)
    
    school_uuid_dnr_mapping = get_school_dnr_uuid_mapping(schools_get_endpoint)
    print('Sucessfully Constructed School UUID / Dnr Mapping:')
    print(school_uuid_dnr_mapping)
    roleid_sus = get_rolle_id(roles_get_endpoint, 'SuS')
    roleid_schuladmin = get_rolle_id(roles_get_endpoint, 'Schuladmin')
    roleid_lehrkraft = get_rolle_id(roles_get_endpoint, 'Lehrkraft')
    roleid_schulbegleitung = get_rolle_id(roles_get_endpoint, 'Schulbegleitung')
    print('Sucessfully Retrieved SchoolIds:')
    print(f'SuS: {roleid_sus}, Schuladmin: {roleid_schuladmin}, Lehrkraft: {roleid_lehrkraft}, Schulbegleitung: {roleid_schulbegleitung}')
    
    access_token = get_access_token()
    token_acquisition_time = datetime.now()
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': 'Bearer ' + access_token
    }
    
    print(f"{datetime.now()} : Starting Api Requests")
    
    number_of_total_skipped_api_calls = 0
    number_of_lehreradmin_skipped_api_calls = 0
    number_of_fvmadmin_skipped_api_calls = 0
    number_of_iqsh_skipped_api_calls = 0
    number_of_deactive_skipped_api_calls = 0
    number_of_schueler_without_klassen_skipped_api_calls = 0#
    
    number_of_create_person_api_calls = 0
    number_of_create_kontext_api_calls = 0
    number_of_deactive_lehrer_api_calls = 0
    number_of_create_person_api_error_responses = 0
    number_of_create_kontext_api_error_responses = 0
    
    # DataFrame to store failed API responses
    skipped_persons = []
    failed_responses_create_person = []
    failed_responses_create_kontext = []
    other_log = []
    
    potential_merge_admins = []
    potential_merge_into_lehrer = []

    for index, row in df_ldap.iterrows():
        if index % 50 == 0:
            elapsed_time = datetime.now() - token_acquisition_time
            if elapsed_time > timedelta(minutes=4):
                access_token = get_access_token()
                headers['Authorization'] = 'Bearer ' + access_token
                token_acquisition_time = datetime.now()
                print(f"{datetime.now()} : Refreshed Authorization: {headers['Authorization']}")
                
        (email, sn, given_name, username, hashed_password, memberOf_list) = convert_data_from_row(row, other_log)
        memberOf_raw = [singleMemberOf.decode('utf-8') if isinstance(singleMemberOf, bytes) else singleMemberOf for singleMemberOf in row['memberOf']]
        filtered_memberOf = [mo for mo in memberOf_list if (mo.startswith(('lehrer-', 'schueler-', 'admin-')) or mo.endswith('-Schulbegleitung'))]       
        is_skip_because_potential_merge_admin = ('#admin' in (sn or '').lower()) and ('sekadmin' not in (username or '').lower()) and ('extadmin' not in (username or '').lower()) #PRÜFUNG FUNKTIONIERT NUR IN ORIGINALDATEI, ANDERNFALLS MÜSSTE MAN AUF 3 BUCHSTABEN IM VORNAMEN PRÜFEN
        is_skip_because_fvmadmin = 'fvm-admin' in (sn or '').lower()
        is_skip_because_iqsh = 'iqsh' in (sn or '').lower()
        is_skip_because_deactive_and_not_lehrer = any(mo for mo in filtered_memberOf if mo.endswith('DeaktivierteKonten')) and not any(mo and 'lehrer-DeaktivierteKonten' in mo for mo in filtered_memberOf)  
        is_skip_because_schueler_without_klasse =  any(mo and 'schueler-' in mo for mo in filtered_memberOf) and not any(mo and 'cn=klassen' in mo for mo in memberOf_raw)       
        is_skip = is_skip_because_potential_merge_admin or is_skip_because_fvmadmin or is_skip_because_iqsh or is_skip_because_deactive_and_not_lehrer or is_skip_because_schueler_without_klasse
        
        #SKIP
        if is_skip == True:
            (number_of_total_skipped_api_calls, 
             number_of_lehreradmin_skipped_api_calls, 
             number_of_fvmadmin_skipped_api_calls, 
             number_of_iqsh_skipped_api_calls, 
             number_of_deactive_skipped_api_calls,
             number_of_schueler_without_klassen_skipped_api_calls) = log_skip(skipped_persons, is_skip_because_potential_merge_admin, is_skip_because_fvmadmin, 
                                                        is_skip_because_iqsh, is_skip_because_deactive_and_not_lehrer, is_skip_because_schueler_without_klasse, 
                                                        number_of_total_skipped_api_calls, number_of_lehreradmin_skipped_api_calls, number_of_fvmadmin_skipped_api_calls,
                                                        number_of_iqsh_skipped_api_calls, number_of_deactive_skipped_api_calls, number_of_schueler_without_klassen_skipped_api_calls,
                                                        username, email, sn, given_name, memberOf_raw)
            
            if(is_skip_because_potential_merge_admin):
                potential_merge_admins.append({
                    'username': username,
                    'sn': sn,
                    'given_name': given_name,
                    'memberOf_raw': memberOf_raw,
                    'memberOf_list': memberOf_list,
                })
            continue
        
        #CREATE PERSON
        response_create_person = create_person_api_call(create_person_post_endpoint, headers, email, sn, given_name, username, hashed_password)
        number_of_create_person_api_calls += 1
        if response_create_person.status_code == 401:
            print(f"{datetime.now()} : Create-Person-Request - 401 Unauthorized error")
            sys.exit()
        elif response_create_person.status_code != 201:
            number_of_create_person_api_error_responses += 1
            failed_responses_create_person.append({
                'username': username,
                'email': email,
                'familienname': sn,
                'vorname': given_name,
                'hashedPassword': hashed_password,
                'error_response_body': response_create_person.json(),
                'status_code': response_create_person.status_code
            })
            continue

        if any(mo and 'lehrer-DeaktivierteKonten' in mo for mo in filtered_memberOf) is True: #No Kontexts For Deactive Lehrers
            number_of_deactive_lehrer_api_calls += 1
            continue
        
        #CREATE KONTEXTS FOR PERSON
#        print(f"Create-Person-Request for row {index} / {parser.number_of_found_persons-1} returned status code {response_create_person.status_code}")
        kontexts_for_merge_into_lehrer = response_create_person.json()
        created_person_id = kontexts_for_merge_into_lehrer.get('person', {}).get('id')
        combined_kontexts = get_combinded_kontexts_to_create(filtered_memberOf, roleid_sus, roleid_lehrkraft, roleid_schuladmin, roleid_schulbegleitung)
        for kontext in combined_kontexts:
            orga_id_for_mergefrom_admin = get_orgaid_by_dnr(school_uuid_dnr_mapping, kontext['dnr'])
            response_create_merge_kontext = create_kontext_api_call(create_kontext_post_endpoint, headers, created_person_id, orga_id_for_mergefrom_admin, kontext['roleId'])
            number_of_create_kontext_api_calls += 1
            if response_create_merge_kontext.status_code == 401:
                print(f"{datetime.now()} : Create-Kontext-Request - 401 Unauthorized error")
                sys.exit()
            elif response_create_merge_kontext.status_code != 201:
                number_of_create_kontext_api_error_responses += 1
                failed_responses_create_kontext.append({
                    'username': username,
                    'person_id': created_person_id,
                    'kontext_orgaId':orga_id_for_mergefrom_admin,
                    'kontext_roleId': kontext['roleId'],
                    'error_response_body': response_create_merge_kontext.json(),
                    'status_code': response_create_merge_kontext.status_code
                })
            else:
#                print(f"Create-Kontext-Request returned status code {response_create_kontext.status_code}")
                if(any(mo and 'lehrer' in mo for mo in filtered_memberOf)):
                    potential_merge_into_lehrer.append({
                        'username': username,
                        'person_id': created_person_id,
                        'kontexts':combined_kontexts
                    })
                    
    print_standard_run_statistics(parser.number_of_found_persons, number_of_total_skipped_api_calls, number_of_lehreradmin_skipped_api_calls, 
    number_of_fvmadmin_skipped_api_calls, number_of_iqsh_skipped_api_calls, number_of_deactive_skipped_api_calls, number_of_schueler_without_klassen_skipped_api_calls, 
    number_of_deactive_lehrer_api_calls, number_of_create_person_api_calls, number_of_create_person_api_error_responses, number_of_create_kontext_api_calls,
    number_of_create_kontext_api_error_responses)
    
    (migration_log,number_of_potential_merge_from_admins,number_of_successfully_merged_any_context_from_admin,
    number_of_no_corressponding_leher_found,number_of_corressponding_lehrer_found_but_missing_school_kontext,
    number_of_create_merge_kontext_api_calls,number_of_create_merge_kontext_api_error_response) = execute_merge(potential_merge_admins, potential_merge_into_lehrer, create_kontext_post_endpoint, 
                                                                                                                personenkontexte_for_person_get_endpoint, school_uuid_dnr_mapping, roleid_lehrkraft, 
                                                                                                                roleid_schuladmin)
     
    print_merge_run_statistics(number_of_potential_merge_from_admins,number_of_successfully_merged_any_context_from_admin, 
                               number_of_no_corressponding_leher_found,number_of_corressponding_lehrer_found_but_missing_school_kontext,
                               number_of_create_merge_kontext_api_calls,number_of_create_merge_kontext_api_error_response)
    
    create_and_save_log_excel(skipped_persons, failed_responses_create_person, failed_responses_create_kontext, other_log, migration_log)