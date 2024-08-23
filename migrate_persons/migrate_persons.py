import numpy as np
import pandas as pd
from helper import get_class_nameAndAdministriertvon_uuid_mapping, get_rolle_id, get_school_dnr_uuid_mapping, log
from datetime import datetime
from migrate_persons.person_helper import create_and_save_log_excel, execute_merge, print_merge_run_statistics, print_standard_run_statistics
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
    
    roleid_sus = get_rolle_id(roles_get_endpoint, 'SuS')
    roleid_schuladmin = get_rolle_id(roles_get_endpoint, 'Schuladmin')
    roleid_lehrkraft = get_rolle_id(roles_get_endpoint, 'Lehrkraft')
    roleid_schulbegleitung = get_rolle_id(roles_get_endpoint, 'Schulbegleitung')
    log('Using The Following RoleIds:')
    log(f'SuS: {roleid_sus}, Schuladmin: {roleid_schuladmin}, Lehrkraft: {roleid_lehrkraft}, Schulbegleitung: {roleid_schulbegleitung}')
    
    log("")
    log(f"### STARTING 200 THREADS ###")
    log("")
    df_parts = np.array_split(df_ldap, 200)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_df_part, index, df_part, school_uuid_dnr_mapping, class_nameAndAdministriertvon_uuid_mapping, roleid_sus, roleid_schuladmin, roleid_lehrkraft, roleid_schulbegleitung, create_person_post_endpoint, create_kontext_post_endpoint) for index, df_part in enumerate(df_parts)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
    combined_results = {
        'skipped_persons': [],
        'failed_responses_create_person': [],
        'failed_responses_create_kontext': [],
        'schueler_on_school_without_klasse': [],
        'other_log': [],
        'potential_merge_admins': [],
        'potential_merge_into_lehrer': [],
        'number_of_total_skipped_api_calls': 0,
        'number_of_lehreradmin_skipped_api_calls': 0,
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
                                
                    
                    
    print_standard_run_statistics(parser.number_of_found_persons, combined_results['number_of_total_skipped_api_calls'], combined_results['number_of_lehreradmin_skipped_api_calls'],
    combined_results['number_of_fvmadmin_skipped_api_calls'], combined_results['number_of_iqsh_skipped_api_calls'], combined_results['number_of_deactive_skipped_api_calls'], combined_results['number_of_schueler_without_klassen_skipped_api_calls'],
    combined_results['number_of_deactive_lehrer_api_calls'], combined_results['number_of_create_person_api_calls'], combined_results['number_of_create_person_api_error_responses'], combined_results['number_of_create_kontext_api_calls'],
    combined_results['number_of_create_kontext_api_error_responses'], combined_results['number_of_create_school_kontext_api_calls'], combined_results['number_of_create_school_kontext_api_error_responses'],
    combined_results['number_of_create_class_kontext_api_calls'], combined_results['number_of_create_class_kontext_api_error_responses'])
    
    (migration_log,number_of_potential_merge_from_admins,number_of_successfully_merged_any_context_from_admin,
    number_of_no_corressponding_leher_found,number_of_corressponding_lehrer_found_but_missing_school_kontext,
    number_of_create_merge_kontext_api_calls,number_of_create_merge_kontext_api_error_response) = execute_merge(combined_results['potential_merge_admins'], combined_results['potential_merge_into_lehrer'], create_kontext_post_endpoint,
                                                                                                                personenkontexte_for_person_get_endpoint, school_uuid_dnr_mapping, roleid_lehrkraft,
                                                                                                                roleid_schuladmin)
     
    print_merge_run_statistics(number_of_potential_merge_from_admins,number_of_successfully_merged_any_context_from_admin, 
                               number_of_no_corressponding_leher_found,number_of_corressponding_lehrer_found_but_missing_school_kontext,
                               number_of_create_merge_kontext_api_calls,number_of_create_merge_kontext_api_error_response)
    
    create_and_save_log_excel(log_output_dir, combined_results['skipped_persons'], combined_results['failed_responses_create_person'], combined_results['failed_responses_create_kontext'], combined_results['schueler_on_school_without_klasse'], combined_results['other_log'], migration_log)
    
    log("")
    log("End method migrate_person_data")