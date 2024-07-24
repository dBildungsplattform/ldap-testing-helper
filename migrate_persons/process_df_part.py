from datetime import datetime, timedelta
import sys

from helper import get_access_token
from migrate_persons.person_helper import convert_data_from_row, create_kontext_api_call, create_person_api_call, get_combinded_school_kontexts_to_create_for_person, get_orgaid_by_className_and_administriertvon, log_skip


def process_df_part(thradnr, df_ldap, school_uuid_dnr_mapping, class_nameAndAdministriertvon_uuid_mapping, 
                    roleid_sus, roleid_schuladmin, roleid_lehrkraft, roleid_schulbegleitung, 
                    create_person_post_endpoint, create_kontext_post_endpoint):
    
    access_token = get_access_token()
    token_acquisition_time = datetime.now()
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': 'Bearer ' + access_token
    }
    
    number_of_total_skipped_api_calls = 0
    number_of_lehreradmin_skipped_api_calls = 0
    number_of_fvmadmin_skipped_api_calls = 0
    number_of_iqsh_skipped_api_calls = 0
    number_of_deactive_skipped_api_calls = 0
    number_of_schueler_without_klassen_skipped_api_calls = 0
    
    number_of_create_person_api_calls = 0
    number_of_deactive_lehrer_api_calls = 0
    number_of_create_person_api_error_responses = 0
    
    number_of_create_kontext_api_calls = 0
    number_of_create_kontext_api_error_responses = 0
    
    number_of_create_school_kontext_api_calls = 0
    number_of_create_school_kontext_api_error_responses = 0
    
    number_of_create_class_kontext_api_calls = 0
    number_of_create_class_kontext_api_error_responses = 0
    
    # DataFrame to store failed API responses
    skipped_persons = []
    failed_responses_create_person = []
    failed_responses_create_kontext = []
    schueler_on_school_without_klasse = []
    other_log = []
    
    potential_merge_admins = []
    potential_merge_into_lehrer = []
    
    print(f"{datetime.now()} | T{thradnr} : Starting Api Requests in Thread")

    for index, row in df_ldap.iterrows():
        if(thradnr == 3):
            print(f"    {datetime.now()} | T{thradnr} : Processing DF Index {index}")
        if index % 20 == 0:
            elapsed_time = datetime.now() - token_acquisition_time
            if elapsed_time > timedelta(minutes=3):
                access_token = get_access_token()
                headers['Authorization'] = 'Bearer ' + access_token
                token_acquisition_time = datetime.now()
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
        kontexts_for_merge_into_lehrer = response_create_person.json()
        created_person_id = kontexts_for_merge_into_lehrer.get('person', {}).get('id')
        combined_schul_kontexts = get_combinded_school_kontexts_to_create_for_person(filtered_memberOf, roleid_sus, roleid_lehrkraft, roleid_schuladmin, roleid_schulbegleitung, school_uuid_dnr_mapping)
        for schul_kontext in combined_schul_kontexts:
            response_create_kontext = create_kontext_api_call(create_kontext_post_endpoint, headers, created_person_id, schul_kontext['orgaId'], schul_kontext['roleId'])
            number_of_create_kontext_api_calls += 1
            number_of_create_school_kontext_api_calls += 1
            if response_create_kontext.status_code == 401:
                print(f"{datetime.now()} : Create-Kontext-Request - 401 Unauthorized error")
                sys.exit()
            elif response_create_kontext.status_code != 201:
                number_of_create_kontext_api_error_responses += 1
                number_of_create_school_kontext_api_error_responses += 1
                failed_responses_create_kontext.append({
                    'username': username,
                    'person_id': created_person_id,
                    'kontext_orgaId':schul_kontext['orgaId'],
                    'kontext_roleId': schul_kontext['roleId'],
                    'error_response_body': response_create_kontext.json(),
                    'status_code': response_create_kontext.status_code,
                    'typ': 'SCHULE_API_ERROR'
                })
                continue
            
            if(any(mo and 'lehrer' in mo for mo in filtered_memberOf)):
                potential_merge_into_lehrer.append({
                    'username': username,
                    'person_id': created_person_id,
                    'kontexts':combined_schul_kontexts
                })
            
            
            #KLASSEN FÜR JEDEN SCHULKONTEXT ANLEGEN
            if schul_kontext['roleId'] == roleid_sus:
                klassen_on_school = [mo.split('-', 1)[1].strip() for mo in memberOf_list if mo.startswith(schul_kontext['dnr'])]
                if(len(klassen_on_school) == 0):
                    schueler_on_school_without_klasse.append({
                            'username': username,
                            'person_id': created_person_id,
                            'school_dnr':schul_kontext['dnr'],
                            'school_id': schul_kontext['orgaId'],
                            'description':'The student is at this school, but has no classes here'
                        })
                for klasse in klassen_on_school:
                    orgaId = get_orgaid_by_className_and_administriertvon(class_nameAndAdministriertvon_uuid_mapping, klasse, schul_kontext['orgaId']) #Klasse kann Zweifelfrei über Kombi aus Name + AdministriertVon Identifiziert werden
                    response_create_class_kontext = create_kontext_api_call(create_kontext_post_endpoint, headers, created_person_id, orgaId, roleid_sus)
                    number_of_create_kontext_api_calls += 1
                    number_of_create_class_kontext_api_calls += 1
                    if response_create_class_kontext.status_code == 401:
                        print(f"{datetime.now()} : Create-Kontext-Request - 401 Unauthorized error")
                        sys.exit()
                    elif response_create_class_kontext.status_code != 201:
                        number_of_create_kontext_api_error_responses += 1
                        number_of_create_class_kontext_api_error_responses += 1
                        failed_responses_create_kontext.append({
                            'username': username,
                            'person_id': created_person_id,
                            'kontext_orgaId':orgaId,
                            'kontext_roleId': roleid_sus,
                            'error_response_body': response_create_class_kontext.json(),
                            'status_code': response_create_class_kontext.status_code,
                            'typ': 'KLASSE_API_ERROR'
                        })
                        continue
    return {
        'skipped_persons': skipped_persons,
        'failed_responses_create_person': failed_responses_create_person,
        'failed_responses_create_kontext': failed_responses_create_kontext,
        'schueler_on_school_without_klasse': schueler_on_school_without_klasse,
        'other_log': other_log,
        'potential_merge_admins': potential_merge_admins,
        'potential_merge_into_lehrer': potential_merge_into_lehrer,
        'number_of_total_skipped_api_calls': number_of_total_skipped_api_calls,
        'number_of_lehreradmin_skipped_api_calls': number_of_lehreradmin_skipped_api_calls,
        'number_of_fvmadmin_skipped_api_calls': number_of_fvmadmin_skipped_api_calls,
        'number_of_iqsh_skipped_api_calls': number_of_iqsh_skipped_api_calls,
        'number_of_deactive_skipped_api_calls': number_of_deactive_skipped_api_calls,
        'number_of_schueler_without_klassen_skipped_api_calls': number_of_schueler_without_klassen_skipped_api_calls,
        'number_of_create_person_api_calls': number_of_create_person_api_calls,
        'number_of_deactive_lehrer_api_calls': number_of_deactive_lehrer_api_calls,
        'number_of_create_person_api_error_responses': number_of_create_person_api_error_responses,
        'number_of_create_kontext_api_calls': number_of_create_kontext_api_calls,
        'number_of_create_kontext_api_error_responses': number_of_create_kontext_api_error_responses,
        'number_of_create_school_kontext_api_calls': number_of_create_school_kontext_api_calls,
        'number_of_create_school_kontext_api_error_responses': number_of_create_school_kontext_api_error_responses,
        'number_of_create_class_kontext_api_calls': number_of_create_class_kontext_api_calls,
        'number_of_create_class_kontext_api_error_responses': number_of_create_class_kontext_api_error_responses
    }