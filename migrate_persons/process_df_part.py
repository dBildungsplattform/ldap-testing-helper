from datetime import datetime, timedelta
import sys
from helper import create_kontext_api_call, get_access_token, log, parse_and_convert_tojsdate
from migrate_persons.migrate_persons_helper import convert_data_from_row, create_person_api_call, get_combinded_school_kontexts_to_create_for_person, get_orgaid_by_className_and_administriertvon, log_skip
from helper import get_access_token, log, parse_and_convert_tojsdate

def process_df_part(
    thradnr, 
    df_part, 
    school_uuid_dnr_mapping, 
    class_nameAndAdministriertvon_uuid_mapping, 
    roleid_itslearning_sus, 
    roleid_schuladmin_oeffentlich,
    roleid_schuladmin_ersatz,
    roleid_lehrkraft,
    roleid_lehrkraft_ersatz,
    roleid_schulbegleitung, 
    api_backend_personen, 
    api_backend_dbiam_personenkontext
    ):
    
    log(f"Started Thread: {thradnr}")
    
    access_token = get_access_token()
    token_acquisition_time = datetime.now()
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': 'Bearer ' + access_token
    }
    
    number_of_total_skipped_api_calls = 0
    number_of_fvmadmin_skipped_api_calls = 0
    number_of_iqsh_skipped_api_calls = 0
    number_of_deactive_skipped_api_calls = 0
    number_of_schueler_without_klassen_skipped_api_calls = 0
    number_of_create_person_api_calls = 0
    number_of_deactive_lehrer_api_calls = 0
    number_of_deactive_admin_api_calls = 0
    number_of_create_person_api_error_responses = 0
    number_of_create_kontext_api_calls = 0
    number_of_create_kontext_api_error_responses = 0
    number_of_create_school_kontext_api_calls = 0
    number_of_create_school_kontext_api_error_responses = 0
    number_of_create_class_kontext_api_calls = 0
    number_of_create_class_kontext_api_error_responses = 0
    number_of_migrated_persons_with_befristung_found = 0
    
    # DataFrames to store logs
    log_skipped_persons = []
    log_failed_responses_create_person = []
    log_failed_responses_create_kontext = []
    log_schueler_on_school_without_klasse = []
    log_invalid_befristung = []
    log_other = []

    for index, row in df_part.iterrows():
        if index % 20 == 0:
            elapsed_time = datetime.now() - token_acquisition_time
            if elapsed_time > timedelta(minutes=3):
                access_token = get_access_token()
                headers['Authorization'] = 'Bearer ' + access_token
                token_acquisition_time = datetime.now()
        (entry_uuid, email, sn, given_name, kopersnr, username, befristung, hashed_password, memberOf_list) = convert_data_from_row(row, log_other)
        memberOf_raw = [singleMemberOf.decode('utf-8') if isinstance(singleMemberOf, bytes) else singleMemberOf for singleMemberOf in row['memberOf']]
        filtered_memberOf = [mo for mo in memberOf_list if (mo.startswith(('lehrer-', 'schueler-', 'admins-')))]
        is_skip_because_fvmadmin = 'fvm-admin' in (sn or '').lower()
        is_skip_because_deactive_and_not_lehrer_or_admin = any(mo for mo in filtered_memberOf if mo.lower().endswith('deaktiviertekonten')) and not any(mo and 'lehrer-deaktiviertekonten' in mo.lower() for mo in filtered_memberOf) and not any(mo and 'admins-deaktiviertekonten' in mo.lower() for mo in filtered_memberOf) 
        is_skip_because_schueler_without_klasse =  any(mo and 'schueler-' in mo for mo in filtered_memberOf) and not any(mo and 'cn=klassen' in mo for mo in memberOf_raw)       
        is_skip = is_skip_because_fvmadmin or is_skip_because_deactive_and_not_lehrer_or_admin or is_skip_because_schueler_without_klasse
        
        #SKIP
        if is_skip == True:
            (number_of_total_skipped_api_calls, 
             number_of_fvmadmin_skipped_api_calls, 
             number_of_iqsh_skipped_api_calls, 
             number_of_deactive_skipped_api_calls,
             number_of_schueler_without_klassen_skipped_api_calls) = log_skip(skipped_persons=log_skipped_persons, 
                                                                              is_skip_because_fvmadmin=is_skip_because_fvmadmin, 
                                                                              is_skip_because_deactive_and_not_lehrer_or_admin=is_skip_because_deactive_and_not_lehrer_or_admin, 
                                                                              is_skip_because_schueler_without_klasse=is_skip_because_schueler_without_klasse, 
                                                                              number_of_total_skipped_api_calls=number_of_total_skipped_api_calls, 
                                                                              number_of_fvmadmin_skipped_api_calls=number_of_fvmadmin_skipped_api_calls,
                                                                              number_of_iqsh_skipped_api_calls=number_of_iqsh_skipped_api_calls, 
                                                                              number_of_deactive_skipped_api_calls=number_of_deactive_skipped_api_calls, 
                                                                              number_of_schueler_without_klassen_skipped_api_calls=number_of_schueler_without_klassen_skipped_api_calls,
                                                                              username=username, 
                                                                              email=email, 
                                                                              sn=sn, 
                                                                              given_name=given_name, 
                                                                              memberOf_raw=memberOf_raw)
            continue
        
        kopersnr_for_creation = None
        if(any(mo and 'lehrer-' in mo for mo in filtered_memberOf)):
            kopersnr_for_creation = kopersnr
  
        response_create_person = create_person_api_call(
            api_backend_personen=api_backend_personen, 
            headers=headers, 
            person_id=entry_uuid, 
            sn=sn, 
            given_name=given_name, 
            username=username, 
            hashed_password=hashed_password,
            kopersnr=kopersnr_for_creation
        )
        number_of_create_person_api_calls += 1
        if response_create_person.status_code == 401:
            log(f"Create-Person-Request - 401 Unauthorized error")
            sys.exit()
        elif response_create_person.status_code != 201:
            number_of_create_person_api_error_responses += 1
            log_failed_responses_create_person.append({
                'username': username,
                'email': email,
                'familienname': sn,
                'vorname': given_name,
                'hashedPassword': hashed_password,
                'error_response_body': response_create_person.json(),
                'status_code': response_create_person.status_code
            })
            log(f"Create Person API Error Response: {response_create_person.json()}")
            continue

        if any(mo and 'lehrer-deaktiviertekonten' in mo.lower() for mo in filtered_memberOf) is True: #Only for logging
            number_of_deactive_lehrer_api_calls += 1

        if any(mo and 'admins-deaktiviertekonten' in mo.lower() for mo in filtered_memberOf) is True: #No Kontexts For Deactive Admins
            number_of_deactive_admin_api_calls += 1
            continue
        
        befristung_valid_jsdate = None
        if befristung != None:
            number_of_migrated_persons_with_befristung_found += 1
            try:
                befristung_valid_jsdate = parse_and_convert_tojsdate(befristung)
            except ValueError as e:
                log_invalid_befristung.append({
                'username': username,
                'familienname': sn,
                'vorname': given_name,
                'description':'Befristung with invalid format found. All Kontexts for this person will be created without Befristung',
                'technical_error':e
                })
                befristung_valid_jsdate = None #Make Sure Kontexts are Created Anyway just without Befristung
        
        #CREATE KONTEXTS FOR PERSON
        created_person_id = response_create_person.json().get('person', {}).get('id')
        combined_schul_kontexts = get_combinded_school_kontexts_to_create_for_person(
            created_person_id=created_person_id,
            filtered_memberOf=filtered_memberOf,
            unfiltered_memberOf=memberOf_list,
            roleid_itslearning_sus=roleid_itslearning_sus, 
            roleid_lehrkraft=roleid_lehrkraft,
            roleid_lehrkraft_ersatz=roleid_lehrkraft_ersatz,
            roleid_schuladmin_oeffentlich=roleid_schuladmin_oeffentlich,
            roleid_schuladmin_ersatz=roleid_schuladmin_ersatz,
            roleid_schulbegleitung=roleid_schulbegleitung, 
            school_uuid_dnr_mapping=school_uuid_dnr_mapping
        )
        for schul_kontext in combined_schul_kontexts:
            email_for_creation = None
            if(schul_kontext['type'] == 'LEHRER'):
                email_for_creation = email
            response_create_kontext = create_kontext_api_call(
                migration_run_type='STANDARD',
                api_backend_dbiam_personenkontext=api_backend_dbiam_personenkontext, 
                headers=headers, 
                person_id=created_person_id, 
                username=None, 
                organisation_id=schul_kontext['orgaId'], 
                rolle_id=schul_kontext['roleId'], 
                email=email_for_creation,
                befristung_valid_jsdate=befristung_valid_jsdate
            )
            number_of_create_kontext_api_calls += 1
            number_of_create_school_kontext_api_calls += 1
            if response_create_kontext.status_code == 401:
                log(f"Create-Kontext-Request - 401 Unauthorized error")
                sys.exit()
            elif response_create_kontext.status_code != 201:
                number_of_create_kontext_api_error_responses += 1
                number_of_create_school_kontext_api_error_responses += 1
                log_failed_responses_create_kontext.append({
                    'username': username,
                    'person_id': created_person_id,
                    'kontext_orgaId':schul_kontext['orgaId'],
                    'kontext_roleId': schul_kontext['roleId'],
                    'error_response_body': response_create_kontext.json(),
                    'status_code': response_create_kontext.status_code,
                    'typ': 'SCHULE_API_ERROR'
                })
                log(f"Create Kontext API Error Response: {response_create_kontext.json()}")
                continue
            
            #KLASSEN FÜR JEDEN SCHULKONTEXT ANLEGEN
            if schul_kontext['roleId'] == roleid_itslearning_sus or schul_kontext['roleId'] == roleid_schulbegleitung:
                klassen_on_school = [mo.split('-', 1)[1].strip() for mo in memberOf_list if mo.startswith(schul_kontext['dnr'])]
                if(len(klassen_on_school) == 0):
                    log_schueler_on_school_without_klasse.append({
                            'username': username,
                            'person_id': created_person_id,
                            'school_dnr':schul_kontext['dnr'],
                            'school_id': schul_kontext['orgaId'],
                            'description':'The student is at this school, but has no classes here'
                        })
                for klasse in klassen_on_school:
                    orgaId = get_orgaid_by_className_and_administriertvon(
                        mapping_df=class_nameAndAdministriertvon_uuid_mapping, 
                        class_name=klasse, 
                        administriert_von=schul_kontext['orgaId'], 
                    ) #Klasse kann Zweifelfrei über Kombi aus Name + AdministriertVon Identifiziert werden
                    response_create_class_kontext = create_kontext_api_call(
                        migration_run_type='STANDARD',
                        api_backend_dbiam_personenkontext=api_backend_dbiam_personenkontext, 
                        headers=headers, 
                        person_id=created_person_id, 
                        username=None, 
                        organisation_id=orgaId, 
                        rolle_id=schul_kontext['roleId'], 
                        email=None,
                        befristung_valid_jsdate=befristung_valid_jsdate
                    )
                    number_of_create_kontext_api_calls += 1
                    number_of_create_class_kontext_api_calls += 1
                    if response_create_class_kontext.status_code == 401:
                        log(f"Create-Kontext-Request - 401 Unauthorized error")
                        sys.exit()
                    elif response_create_class_kontext.status_code != 201:
                        number_of_create_kontext_api_error_responses += 1
                        number_of_create_class_kontext_api_error_responses += 1
                        log_failed_responses_create_kontext.append({
                            'username': username,
                            'person_id': created_person_id,
                            'kontext_orgaId':orgaId,
                            'kontext_roleId': schul_kontext['roleId'],
                            'error_response_body': response_create_class_kontext.json(),
                            'status_code': response_create_class_kontext.status_code,
                            'typ': 'KLASSE_API_ERROR'
                        })
                        log(f"Create Kontext API Error Response: {response_create_class_kontext.json()}")
                        continue
    return {
        'log_skipped_persons': log_skipped_persons,
        'log_failed_responses_create_person': log_failed_responses_create_person,
        'log_failed_responses_create_kontext': log_failed_responses_create_kontext,
        'log_schueler_on_school_without_klasse': log_schueler_on_school_without_klasse,
        'log_invalid_befristung': log_invalid_befristung,
        'log_other': log_other,
        'number_of_total_skipped_api_calls': number_of_total_skipped_api_calls,
        'number_of_fvmadmin_skipped_api_calls': number_of_fvmadmin_skipped_api_calls,
        'number_of_iqsh_skipped_api_calls': number_of_iqsh_skipped_api_calls,
        'number_of_deactive_skipped_api_calls': number_of_deactive_skipped_api_calls,
        'number_of_schueler_without_klassen_skipped_api_calls': number_of_schueler_without_klassen_skipped_api_calls,
        'number_of_create_person_api_calls': number_of_create_person_api_calls,
        'number_of_deactive_lehrer_api_calls': number_of_deactive_lehrer_api_calls,
        'number_of_deactive_admin_api_calls':number_of_deactive_admin_api_calls,
        'number_of_create_person_api_error_responses': number_of_create_person_api_error_responses,
        'number_of_create_kontext_api_calls': number_of_create_kontext_api_calls,
        'number_of_create_kontext_api_error_responses': number_of_create_kontext_api_error_responses,
        'number_of_create_school_kontext_api_calls': number_of_create_school_kontext_api_calls,
        'number_of_create_school_kontext_api_error_responses': number_of_create_school_kontext_api_error_responses,
        'number_of_create_class_kontext_api_calls': number_of_create_class_kontext_api_calls,
        'number_of_create_class_kontext_api_error_responses': number_of_create_class_kontext_api_error_responses,
        'number_of_migrated_persons_with_befristung_found':number_of_migrated_persons_with_befristung_found
    }
