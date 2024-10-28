import numpy as np
import pandas as pd
from helper import get_class_name_and_administriertvon_uuid_mapping, get_rolle_id, get_school_dnr_uuid_mapping, log, save_to_excel
from migrate_persons.person_ldif_parser import BuildPersonDFLDIFParser
import concurrent.futures
from migrate_persons.process_df_part import process_df_part
                
def migrate_person_data(log_output_dir, api_backend_personen, api_backend_dbiam_personenkontext, api_backend_organisationen, api_backend_rolle, ldap_chunk):
    log(f"Start method migrate_person_data with Input {log_output_dir}, {api_backend_personen}, {api_backend_dbiam_personenkontext}, {api_backend_organisationen}, {api_backend_rolle}, {ldap_chunk}")
    
    log(f"Start BuildPersonDFLDIFParser")
    with open(ldap_chunk, 'rb') as input_file:
        parser = BuildPersonDFLDIFParser(input_file)
        parser.parse()
    df_ldap = pd.DataFrame(parser.entries_list)
    log(f"Finished BuildPersonDFLDIFParser")
    
    school_uuid_dnr_mapping = get_school_dnr_uuid_mapping(api_backend_organisationen)
    class_nameAndAdministriertvon_uuid_mapping = get_class_name_and_administriertvon_uuid_mapping(api_backend_organisationen)
    
    rolleid_sus = get_rolle_id(api_backend_rolle, 'SuS')
    rolleid_schuladmin = get_rolle_id(api_backend_rolle, 'Schuladmin')
    rolleid_lehrkraft = get_rolle_id(api_backend_rolle, 'Lehrkraft')
    rolleid_schulbegleitung = get_rolle_id(api_backend_rolle, 'Schulbegleitung')
    if not rolleid_sus or not rolleid_schuladmin or not rolleid_lehrkraft or not rolleid_schulbegleitung:
        raise ValueError("For at least one mandatory rolle () no rolle could be fechd from backend")
    log('Using The Following RolleIds:')
    log(f'SuS: {rolleid_sus}, Schuladmin: {rolleid_schuladmin}, Lehrkraft: {rolleid_lehrkraft}, Schulbegleitung: {rolleid_schulbegleitung}')
    
    log("")
    log(f"### STARTING 100 THREADS ###")
    log("")
    df_parts = np.array_split(df_ldap, 100)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_df_part, 
            index, 
            df_part, 
            school_uuid_dnr_mapping, 
            class_nameAndAdministriertvon_uuid_mapping, 
            rolleid_sus, 
            rolleid_schuladmin, 
            rolleid_lehrkraft, 
            rolleid_schulbegleitung, 
            api_backend_personen, 
            api_backend_dbiam_personenkontext
        ) for index, df_part in enumerate(df_parts)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
    combined_results = {
        'log_skipped_persons': [],
        'log_failed_responses_create_person': [],
        'log_failed_responses_create_kontext': [],
        'log_schueler_on_school_without_klasse': [],
        'log_invalid_befristung':[],
        'log_other': [],
        'number_of_total_skipped_api_calls': 0,
        'number_of_fvmadmin_skipped_api_calls': 0,
        'number_of_iqsh_skipped_api_calls': 0,
        'number_of_deactive_skipped_api_calls': 0,
        'number_of_schueler_without_klassen_skipped_api_calls': 0,
        'number_of_create_person_api_calls': 0,
        'number_of_deactive_lehrer_api_calls': 0,
        'number_of_create_person_api_error_responses': 0,
        'number_of_create_kontext_api_calls': 0,
        'number_of_create_kontext_api_error_responses': 0,
        'number_of_create_school_kontext_api_calls': 0,
        'number_of_create_school_kontext_api_error_responses': 0,
        'number_of_create_class_kontext_api_calls': 0,
        'number_of_create_class_kontext_api_error_responses': 0,
        'number_of_migrated_persons_with_befristung_found':0
    }

    for result in results:
        for key in combined_results.keys():
            if isinstance(combined_results[key], list):
                combined_results[key].extend(result[key])
            else:
                combined_results[key] += result[key]
    
    # RUN STATISTICS                                 
    log("")
    log("")
    log("### STANDARD RUN STATISTICS ###")
    log("")
    log(f"Number of found Persons: {parser.number_of_found_persons}")
    log(f"Number of Total Skipped Persons: {combined_results['number_of_total_skipped_api_calls']}")
    log(f"                Of That FVM-Admin Persons: {combined_results['number_of_fvmadmin_skipped_api_calls']}")
    log(f"                Of That IQSH Persons: {combined_results['number_of_iqsh_skipped_api_calls']}")
    log(f"                Of That Deactive Non LehrerPersons: {combined_results['number_of_deactive_skipped_api_calls']}")
    log(f"                Of That Schueler Without Any Klasse: {combined_results['number_of_schueler_without_klassen_skipped_api_calls']}")
    log(f"Number of Create-Person API Calls: {combined_results['number_of_create_person_api_calls']}")
    log(f"                Of That with Befristung: {combined_results['number_of_migrated_persons_with_befristung_found']}")
    log(f"                Of That Deactive Lehrer Persons: {combined_results['number_of_deactive_lehrer_api_calls']}")
    log(f"Number of Create-Person API Error Responses: {combined_results['number_of_create_person_api_error_responses']}")
    log(f"Number of Total Create-Kontext API Calls: {combined_results['number_of_create_kontext_api_calls']}")
    log(f"                Of That School Kontexts: {combined_results['number_of_create_school_kontext_api_calls']}")
    log(f"                Of That Class Kontexts: {combined_results['number_of_create_class_kontext_api_calls']}")
    log(f"Number of TotalCreate-Kontext API Error Responses: {combined_results['number_of_create_kontext_api_error_responses']}")
    log(f"                Of That School Kontexts: {combined_results['number_of_create_school_kontext_api_error_responses']}")
    log(f"                Of That Class Kontexts: {combined_results['number_of_create_class_kontext_api_error_responses']}")
    log("")
    
    
    # EXCEL LOGGING   
    log_data = {
    'Skipped_Persons': pd.DataFrame(combined_results['log_skipped_persons']),
    'Failed_Api_Create_Person': pd.DataFrame(combined_results['log_failed_responses_create_person']),
    'Failed_Api_Create_Kontext': pd.DataFrame(combined_results['log_failed_responses_create_kontext']),
    'Schueler_On_School_No_Class': pd.DataFrame(combined_results['log_schueler_on_school_without_klasse']),
    'Invalid_Befristung': pd.DataFrame(combined_results['log_invalid_befristung']),
    'Other': pd.DataFrame(combined_results['log_other'])
    }
    save_to_excel(log_data, log_output_dir, 'log_migrate_persons')
    log("")
    log("End method migrate_person_data")