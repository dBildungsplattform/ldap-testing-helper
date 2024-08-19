import itertools
import os
import tempfile

from helper import get_class_nameAndAdministriertvon_uuid_mapping, get_rolle_id, get_school_dnr_uuid_mapping
from migrate_classes.migrate_classes import migrate_class_data
from migrate_persons.migrate_persons import migrate_person_data
from migrate_persons.person_helper import execute_merge, print_standard_run_statistics, print_merge_run_statistics, \
    create_and_save_log_excel
from migrate_schools.migrate_schools import migrate_school_data


def main():
    print("Main execution started.")
    migrationType = os.environ.get('MIGRATION_TYPE')
    if not migrationType:
        raise ValueError("ENV: Migration Type path cannot be null or empty")
    log_output_dir = os.environ.get('LOG_OUTPUT_DIR')
    if not log_output_dir:
        raise ValueError("ENV: Log ouput path cannot be null or empty")

    if migrationType == 'SCHOOLS':
        createOrgaPostEndpoint = os.environ['MIGRATION_SCHOOLS_POST_ENDPOINT']
        getOeffAndErsatzUUIDEndpoint = os.environ['MIGRATION_SCHOOLS_UUID_ERSATZ_OEFFENTLICH_ENDPOINT']
        schoolDataInputExcel = os.environ['MIGRATION_SCHOOLS_INPUT_EXCEL_COMPLETE_PATH']
        schoolDataInputLDAP = os.environ['MIGRATION_SCHOOLS_INPUT_LDAP_COMPLETE_PATH']

        if not createOrgaPostEndpoint:
            raise ValueError("ENV: POST Endpoint For Create Organisation cannot be null or empty")
        if not getOeffAndErsatzUUIDEndpoint:
            raise ValueError("ENV: GET Endpoint For UUID (Oeffentlich vs Eratz) cannot be null or empty")
        if not schoolDataInputExcel:
            raise ValueError("ENV: Input path for Excel cannot be null or empty")
        if not schoolDataInputLDAP:
            raise ValueError("ENV: Input path for LDAP cannot be null or empty")

        migrate_school_data(log_output_dir, createOrgaPostEndpoint, getOeffAndErsatzUUIDEndpoint, schoolDataInputExcel,
                            schoolDataInputLDAP)
        
    if migrationType == 'CLASSES':
        createOrgaPostEndpoint = os.environ['MIGRATION_CLASSES_POST_ENDPOINT']
        schools_get_endpoint = os.environ['MIGRATION_CLASSES_GET_SCHOOLS_ENDPOINT']
        classDataInputLDAP = os.environ['MIGRATION_CLASSES_INPUT_LDAP_COMPLETE_PATH']

        if not createOrgaPostEndpoint:
            raise ValueError("ENV: POST Endpoint For Create Organisation cannot be null or empty")
        if not schools_get_endpoint:
            raise ValueError("ENV: Get Endpoint for Schools cannot be null or empty")
        if not classDataInputLDAP:
            raise ValueError("ENV: Input path for LDAP cannot be null or empty")

        migrate_class_data(log_output_dir, createOrgaPostEndpoint, schools_get_endpoint, classDataInputLDAP)

    if migrationType == 'PERSONS':
        create_person_post_endpoint = os.environ['MIGRATION_PERSONS_POST_ENDPOINT_CREATE_PERSON']
        create_kontext_post_endpoint = os.environ['MIGRATION_PERSONS_POST_ENDPOINT_CREATE_KONTEXT']
        persons_data_input_ldap = os.environ['MIGRATION_PERSONS_INPUT_LDAP_COMPLETE_PATH']
        orgas_get_endpoint = os.environ['MIGRATION_PERSONS_GET_SCHOOLS_ENDPOINT']
        roles_get_endpoint = os.environ['MIGRATION_PERSONS_GET_ROLES_ENDPOINT']
        personenkontexte_for_person_get_endpoint = os.environ[
            'MIGRATION_PERSONS_GET_PERSONENKONTEXTE_FOR_PERSON_ENDPOINT']

        if not create_person_post_endpoint:
            raise ValueError("ENV: POST Endpoint For Create Person cannot be null or empty")
        if not create_kontext_post_endpoint:
            raise ValueError("ENV: POST Endpoint For Create Kontext cannot be null or empty")
        if not persons_data_input_ldap:
            raise ValueError("ENV: Input path for LDAP cannot be null or empty")
        if not orgas_get_endpoint:
            raise ValueError("ENV: Get Endpoint for Schools/Orgas cannot be null or empty")
        if not roles_get_endpoint:
            raise ValueError("ENV: Get Endpoint for Roles cannot be null or empty")
        if not personenkontexte_for_person_get_endpoint:
            raise ValueError("ENV: Get Endpoint for Personenkontexte For Person cannot be null or empty")

        roleid_sus = get_rolle_id(roles_get_endpoint, 'SuS')
        roleid_schuladmin = get_rolle_id(roles_get_endpoint, 'Schuladmin')
        roleid_lehrkraft = get_rolle_id(roles_get_endpoint, 'Lehrkraft')
        roleid_schulbegleitung = get_rolle_id(roles_get_endpoint, 'Schulbegleitung')
        school_uuid_dnr_mapping = get_school_dnr_uuid_mapping(orgas_get_endpoint)
        class_nameAndAdministriertvon_uuid_mapping = get_class_nameAndAdministriertvon_uuid_mapping(orgas_get_endpoint)
        
        print(f'Using Roles --> SuS: {roleid_sus}, Schuladmin: {roleid_schuladmin}, Lehrkraft: {roleid_lehrkraft}, Schulbegleitung: {roleid_schulbegleitung}')
        print('Using School UUID - DNR Mapping:')
        print(school_uuid_dnr_mapping)
        print('Using Klass UUID - Name + AdministriertVon Mapping:')
        print(class_nameAndAdministriertvon_uuid_mapping)

        combined_results = {
            'number_of_found_persons': 0,
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

        tmpFiles = chunk_input_file(persons_data_input_ldap)
        
        for chunkFile in tmpFiles:
            migrate_person_data(combined_results, create_person_post_endpoint,
                                create_kontext_post_endpoint, chunkFile, roleid_sus, roleid_schuladmin,
                                roleid_lehrkraft, roleid_schulbegleitung, school_uuid_dnr_mapping, class_nameAndAdministriertvon_uuid_mapping)

        print_standard_run_statistics(combined_results['number_of_found_persons'],
                                      combined_results['number_of_total_skipped_api_calls'],
                                      combined_results['number_of_lehreradmin_skipped_api_calls'],
                                      combined_results['number_of_fvmadmin_skipped_api_calls'],
                                      combined_results['number_of_iqsh_skipped_api_calls'],
                                      combined_results['number_of_deactive_skipped_api_calls'],
                                      combined_results['number_of_schueler_without_klassen_skipped_api_calls'],
                                      combined_results['number_of_deactive_lehrer_api_calls'],
                                      combined_results['number_of_create_person_api_calls'],
                                      combined_results['number_of_create_person_api_error_responses'],
                                      combined_results['number_of_create_kontext_api_calls'],
                                      combined_results['number_of_create_kontext_api_error_responses'],
                                      combined_results['number_of_create_school_kontext_api_calls'],
                                      combined_results['number_of_create_school_kontext_api_error_responses'],
                                      combined_results['number_of_create_class_kontext_api_calls'],
                                      combined_results['number_of_create_class_kontext_api_error_responses'])

        (migration_log, number_of_potential_merge_from_admins, number_of_successfully_merged_any_context_from_admin,
         number_of_no_corressponding_leher_found, number_of_corressponding_lehrer_found_but_missing_school_kontext,
         number_of_create_merge_kontext_api_calls, number_of_create_merge_kontext_api_error_response) = execute_merge(
            combined_results['potential_merge_admins'], combined_results['potential_merge_into_lehrer'],
            create_kontext_post_endpoint,
            personenkontexte_for_person_get_endpoint, school_uuid_dnr_mapping, roleid_lehrkraft,
            roleid_schuladmin)

        print_merge_run_statistics(number_of_potential_merge_from_admins,
                                   number_of_successfully_merged_any_context_from_admin,
                                   number_of_no_corressponding_leher_found,
                                   number_of_corressponding_lehrer_found_but_missing_school_kontext,
                                   number_of_create_merge_kontext_api_calls,
                                   number_of_create_merge_kontext_api_error_response)

        create_and_save_log_excel(log_output_dir, combined_results['skipped_persons'],
                                  combined_results['failed_responses_create_person'],
                                  combined_results['failed_responses_create_kontext'],
                                  combined_results['schueler_on_school_without_klasse'], combined_results['other_log'],
                                  migration_log)

    print("Main execution finished.")


def chunk_input_file(personsDataInputLDAP):
    temp_files = (tempfile.NamedTemporaryFile(mode="w+t", prefix="personen_import") for _ in itertools.count())

    # Split the input LDIF to keep the memory footprint manageable
    # Luckily LDIF splits its records by simply putting an empty line in between them
    MAX_DATASETS_PER_FILE = 50000
    datasetCounter = 0
    tmpFiles = []
    current_tmp_file = next(temp_files)
    tmpFiles.append(current_tmp_file)
    with open(personsDataInputLDAP) as ldifFile:
        for line in ldifFile:
            if line == "\n":
                datasetCounter += 1
            if datasetCounter < MAX_DATASETS_PER_FILE:
                current_tmp_file.write(line)
            # New file and drop the separator line
            else:
                datasetCounter = 0
                current_tmp_file = next(temp_files)
                tmpFiles.append(current_tmp_file)
    return tmpFiles


if __name__ == "__main__":
    main()
