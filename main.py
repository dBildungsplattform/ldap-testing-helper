import os
from helper import log
from migrate_classes.migrate_classes import migrate_class_data
from migrate_persons.migrate_persons import migrate_person_data
from migrate_schools.migrate_schools import migrate_school_data


def main():
    
    log("")
    log("##########################")
    log("# Main execution started #")
    log("##########################")
    
    migration_type = os.environ.get('MIGRATION_TYPE')
    if not migration_type:
        raise ValueError("ENV: MIGRATION_TYPE cannot be null or empty")
    log_output_dir = os.environ.get('LOG_OUTPUT_DIR')
    if not log_output_dir:
        raise ValueError("ENV: LOG_OUTPUT_DIR cannot be null or empty")
    input_ldap = os.environ['INPUT_LDAP_COMPLETE_PATH']
    if not log_output_dir:
        raise ValueError("ENV: INPUT_LDAP_COMPLETE_PATH cannot be null or empty")

    if migration_type == 'SCHOOLS':
        log("")
        log("Selected Migration Type: SCHOOLS")
        schools_post_endpoint = os.environ['MIGRATION_SCHOOLS_POST_ENDPOINT']
        oeff_and_ersatz_uuid_get_endpoint = os.environ['MIGRATION_SCHOOLS_UUID_ERSATZ_OEFFENTLICH_ENDPOINT']
        input_excel_oeff_schools = os.environ['MIGRATE_SCHOOLS_INPUT_EXCEL_COMPLETE_PATH']

        if not schools_post_endpoint:
            raise ValueError("ENV: MIGRATION_SCHOOLS_POST_ENDPOINT cannot be null or empty")
        if not oeff_and_ersatz_uuid_get_endpoint:
            raise ValueError("ENV: MIGRATION_SCHOOLS_UUID_ERSATZ_OEFFENTLICH_ENDPOINT cannot be null or empty")
        if not input_excel_oeff_schools:
            raise ValueError("ENV: MIGRATE_SCHOOLS_INPUT_EXCEL_COMPLETE_PATH cannot be null or empty")

        migrate_school_data(log_output_dir, schools_post_endpoint, oeff_and_ersatz_uuid_get_endpoint, input_excel_oeff_schools,
                            input_ldap)
        
    if migration_type == 'CLASSES':
        log("")
        log("Selected Migration Type: CLASSES")
        classes_post_endpoint = os.environ['MIGRATION_CLASSES_POST_ENDPOINT']
        schools_get_endpoint = os.environ['MIGRATION_CLASSES_GET_SCHOOLS_ENDPOINT']

        if not classes_post_endpoint:
            raise ValueError("ENV: MIGRATION_CLASSES_POST_ENDPOINT cannot be null or empty")
        if not schools_get_endpoint:
            raise ValueError("ENV: MIGRATION_CLASSES_GET_SCHOOLS_ENDPOINT cannot be null or empty")

        migrate_class_data(log_output_dir, classes_post_endpoint, schools_get_endpoint, input_ldap)

    if migration_type == 'PERSONS':
        log("")
        log("Selected Migration Type: PERSONS")
        create_person_post_endpoint = os.environ['MIGRATION_PERSONS_POST_ENDPOINT_CREATE_PERSON']
        create_kontext_post_endpoint = os.environ['MIGRATION_PERSONS_POST_ENDPOINT_CREATE_KONTEXT']
        orgas_get_endpoint = os.environ['MIGRATION_PERSONS_GET_SCHOOLS_ENDPOINT']
        roles_get_endpoint = os.environ['MIGRATION_PERSONS_GET_ROLES_ENDPOINT']
        personenkontexte_for_person_get_endpoint = os.environ['MIGRATION_PERSONS_GET_PERSONENKONTEXTE_FOR_PERSON_ENDPOINT']

        if not create_person_post_endpoint:
            raise ValueError("ENV: MIGRATION_PERSONS_POST_ENDPOINT_CREATE_PERSON cannot be null or empty")
        if not create_kontext_post_endpoint:
            raise ValueError("ENV: MIGRATION_PERSONS_POST_ENDPOINT_CREATE_KONTEXT cannot be null or empty")
        if not orgas_get_endpoint:
            raise ValueError("ENV: MIGRATION_PERSONS_GET_SCHOOLS_ENDPOINT cannot be null or empty")
        if not roles_get_endpoint:
            raise ValueError("ENV: MIGRATION_PERSONS_GET_ROLES_ENDPOINT cannot be null or empty")
        if not personenkontexte_for_person_get_endpoint:
            raise ValueError("ENV: MIGRATION_PERSONS_GET_PERSONENKONTEXTE_FOR_PERSON_ENDPOINT cannot be null or empty")

        migrate_person_data(log_output_dir, create_person_post_endpoint, create_kontext_post_endpoint, orgas_get_endpoint, roles_get_endpoint, personenkontexte_for_person_get_endpoint, input_ldap)
        
    log("")
    log("###########################")
    log("# Main execution finished #")
    log("###########################")

if __name__ == "__main__":
    main()
