import os
import numpy as np
import pandas as pd
from helper import get_class_nameAndAdministriertvon_uuid_mapping, get_rolle_id, get_school_dnr_uuid_mapping, log
from datetime import datetime
from migrate_persons.person_ldif_parser import BuildPersonDFLDIFParser
import concurrent.futures
from migrate_persons.process_df_part import process_df_part
                
def migrate_person_data(log_output_dir, create_person_post_endpoint, create_kontext_post_endpoint, orgas_get_endpoint, roles_get_endpoint, personenkontexte_for_person_get_endpoint, input_path_ldap):
    log(f"Start method migrate_person_data with Input {log_output_dir}, {create_person_post_endpoint}, {create_kontext_post_endpoint}, {orgas_get_endpoint}, {roles_get_endpoint}, {personenkontexte_for_person_get_endpoint}, {input_path_ldap}")
    
    log(f"Start BuildPersonDFLDIFParser")
    with open(input_path_ldap, 'rb') as input_file:
        parser = BuildPersonDFLDIFParser(input_file)
        parser.parse()
    df_ldap = pd.DataFrame(parser.entries_list)
    log(f"Finished BuildPersonDFLDIFParser")
    
    school_uuid_dnr_mapping = get_school_dnr_uuid_mapping(orgas_get_endpoint)
    class_nameAndAdministriertvon_uuid_mapping = get_class_nameAndAdministriertvon_uuid_mapping(orgas_get_endpoint)
    
    rolleid_sus = get_rolle_id(roles_get_endpoint, 'SuS')
    rolleid_schuladmin = get_rolle_id(roles_get_endpoint, 'Schuladmin')
    rolleid_lehrkraft = get_rolle_id(roles_get_endpoint, 'Lehrkraft')
    rolleid_schulbegleitung = get_rolle_id(roles_get_endpoint, 'Schulbegleitung')
    log('Using The Following RolleIds:')
    log(f'SuS: {rolleid_sus}, Schuladmin: {rolleid_schuladmin}, Lehrkraft: {rolleid_lehrkraft}, Schulbegleitung: {rolleid_schulbegleitung}')
    
    log("")
    log(f"### STARTING 100 THREADS ###")
    log("")
    df_parts = np.array_split(df_ldap, 100)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_df_part, index, df_part, school_uuid_dnr_mapping, class_nameAndAdministriertvon_uuid_mapping, rolleid_sus, rolleid_schuladmin, rolleid_lehrkraft, rolleid_schulbegleitung, create_person_post_endpoint, create_kontext_post_endpoint) for index, df_part in enumerate(df_parts)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
    combined_results = {
        'skipped_persons': [],
        'failed_responses_create_person': [],
        'failed_responses_create_kontext': [],
        'schueler_on_school_without_klasse': [],
        'other_log': [],
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
        'number_of_create_class_kontext_api_error_responses': 0
    }

    for result in results:
        for key in combined_results.keys():
            if isinstance(combined_results[key], list):
                combined_results[key].extend(result[key])
            else:
                combined_results[key] += result[key]
                                     
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
    log(f"                Of That Deactive Lehrer Persons: {combined_results['number_of_deactive_lehrer_api_calls']}")
    log(f"Number of Create-Person API Error Responses: {combined_results['number_of_create_person_api_error_responses']}")
    log(f"Number of Total Create-Kontext API Calls: {combined_results['number_of_create_kontext_api_calls']}")
    log(f"                Of That School Kontexts: {combined_results['number_of_create_school_kontext_api_calls']}")
    log(f"                Of That Class Kontexts: {combined_results['number_of_create_class_kontext_api_calls']}")
    log(f"Number of TotalCreate-Kontext API Error Responses: {combined_results['number_of_create_kontext_api_error_responses']}")
    log(f"                Of That School Kontexts: {combined_results['number_of_create_school_kontext_api_error_responses']}")
    log(f"                Of That Class Kontexts: {combined_results['number_of_create_class_kontext_api_error_responses']}")
    log("")
    
    
    skipped_persons_df = pd.DataFrame(combined_results['skipped_persons'])
    failed_responses_create_person_df = pd.DataFrame(combined_results['failed_responses_create_person'])
    failed_responses_create_kontext_df = pd.DataFrame(combined_results['failed_responses_create_kontext'])
    schueler_on_school_without_klasse_df = pd.DataFrame(combined_results['schueler_on_school_without_klasse'])
    other_log_df = pd.DataFrame(combined_results['other_log'])
    os.makedirs(log_output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    excel_path = os.path.join(log_output_dir, f'migrate_persons_log_{timestamp}.xlsx')
    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            skipped_persons_df.to_excel(writer, sheet_name='Skipped_Persons', index=False)
            failed_responses_create_person_df.to_excel(writer, sheet_name='Failed_Api_Create_Person', index=False)
            failed_responses_create_kontext_df.to_excel(writer, sheet_name='Failed_Api_Create_Kontext', index=False)
            schueler_on_school_without_klasse_df.to_excel(writer, sheet_name='Schueler_On_School_No_Class', index=False)
            other_log_df.to_excel(writer, sheet_name='Other', index=False)
        log(f"Failed API responses have been saved to '{excel_path}'.")
        log(f"Check the current working directory: {os.getcwd()}")
    except Exception as e:
        log(f"An error occurred while saving the Excel file: {e}")
    
    log("")
    log("End method migrate_person_data")