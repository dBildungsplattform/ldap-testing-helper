import os
from helper import get_hash_sha256_for_file, log
from migrate_classes.migrate_classes import migrate_class_data
from migrate_itslearning_affiliation.migrate_itslearning_affiliation import migrate_itslearning_affiliation_data
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
    input_ldap_sha256_hash = os.environ['INPUT_LDAP_SHA256HASH']
    if not input_ldap_sha256_hash:
        raise ValueError("ENV: INPUT_LDAP_SHA256HASH cannot be null or empty")
    
    if not get_hash_sha256_for_file(input_ldap) == input_ldap_sha256_hash:
        raise ValueError("ENV: INPUT_LDAP_SHA256HASH doesnt match the actual provided input_ldap files hash")
    else: 
        log('LDAP Hashes are matching')

    if migration_type == 'SCHOOLS':
        log("")
        log("Selected Migration Type: SCHOOLS")
        schools_post_endpoint = os.environ['MIGRATION_SCHOOLS_POST_ENDPOINT']
        oeff_and_ersatz_uuid_get_endpoint = os.environ['MIGRATION_SCHOOLS_UUID_ERSATZ_OEFFENTLICH_ENDPOINT']
        input_excel_schools = os.environ['MIGRATE_SCHOOLS_INPUT_EXCEL_COMPLETE_PATH']
        input_excel_sha256_hash = os.environ['INPUT_EXCEL_SHA256HASH']

        if not schools_post_endpoint:
            raise ValueError("ENV: MIGRATION_SCHOOLS_POST_ENDPOINT cannot be null or empty")
        if not oeff_and_ersatz_uuid_get_endpoint:
            raise ValueError("ENV: MIGRATION_SCHOOLS_UUID_ERSATZ_OEFFENTLICH_ENDPOINT cannot be null or empty")
        if not input_excel_schools:
            raise ValueError("ENV: MIGRATE_SCHOOLS_INPUT_EXCEL_COMPLETE_PATH cannot be null or empty")
        if not input_excel_sha256_hash:
            raise ValueError("ENV: INPUT_EXCEL_SHA256HASH cannot be null or empty")
        
        if not get_hash_sha256_for_file(input_excel_schools) == input_excel_sha256_hash:
            raise ValueError("ENV: INPUT_EXCEL_SHA256HASH doesnt match the actual provided excel files hash")
        else: 
            log('Excel Hashes are matching')

        migrate_school_data(log_output_dir, schools_post_endpoint, oeff_and_ersatz_uuid_get_endpoint, input_excel_schools,
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
        migrate_persons_create_person_post_endpoint = os.environ['MIGRATION_PERSONS_POST_ENDPOINT_CREATE_PERSON']
        migrate_persons_create_kontext_post_endpoint = os.environ['MIGRATION_PERSONS_POST_ENDPOINT_CREATE_KONTEXT']
        migrate_persons_orgas_get_endpoint = os.environ['MIGRATION_PERSONS_GET_SCHOOLS_ENDPOINT']
        migrate_persons_roles_get_endpoint = os.environ['MIGRATION_PERSONS_GET_ROLES_ENDPOINT']
        personenkontexte_for_person_get_endpoint = os.environ['MIGRATION_PERSONS_GET_PERSONENKONTEXTE_FOR_PERSON_ENDPOINT']

        if not migrate_persons_create_person_post_endpoint:
            raise ValueError("ENV: MIGRATION_PERSONS_POST_ENDPOINT_CREATE_PERSON cannot be null or empty")
        if not migrate_persons_create_kontext_post_endpoint:
            raise ValueError("ENV: MIGRATION_PERSONS_POST_ENDPOINT_CREATE_KONTEXT cannot be null or empty")
        if not migrate_persons_orgas_get_endpoint:
            raise ValueError("ENV: MIGRATION_PERSONS_GET_SCHOOLS_ENDPOINT cannot be null or empty")
        if not migrate_persons_roles_get_endpoint:
            raise ValueError("ENV: MIGRATION_PERSONS_GET_ROLES_ENDPOINT cannot be null or empty")
        if not personenkontexte_for_person_get_endpoint:
            raise ValueError("ENV: MIGRATION_PERSONS_GET_PERSONENKONTEXTE_FOR_PERSON_ENDPOINT cannot be null or empty")

        migrate_person_data(log_output_dir, migrate_persons_create_person_post_endpoint, migrate_persons_create_kontext_post_endpoint, migrate_persons_orgas_get_endpoint, migrate_persons_roles_get_endpoint, personenkontexte_for_person_get_endpoint, input_ldap)
        
    if migration_type == 'ITSLEARNING_AFFILIATION':
        log("")
        log("Selected Migration Type: ITSLEARNING_AFFILIATION")
        itslearning_affiliation_create_kontext_post_endpoint = os.environ['MIGRATE_ITSLEARNING_AFFILIATION_CREATE_KONTEXT_POST_ENDPOINT']
        itslearning_affiliation_orgas_get_endpoint = os.environ['MIGRATE_ITSLEARNING_AFFILIATION_ORGAS_GET_ENDPOINT']
        itslearning_affiliation_roles_get_endpoint = os.environ['MIGRATE_ITSLEARNING_AFFILIATION_ROLES_GET_ENDPOINT']

        if not itslearning_affiliation_create_kontext_post_endpoint:
            raise ValueError("ENV: ITSLEARNING_AFFILIATION_CREATE_KONTEXT_POST_ENDPOINT cannot be null or empty")
        if not itslearning_affiliation_orgas_get_endpoint:
            raise ValueError("ENV: ITSLEARNING_AFFILIATION_ORGAS_GET_ENDPOINT cannot be null or empty")
        if not itslearning_affiliation_roles_get_endpoint:
            raise ValueError("ENV: ITSLEARNING_AFFILIATION_ROLES_GET_ENDPOINT cannot be null or empty")


        migrate_itslearning_affiliation_data(log_output_dir, itslearning_affiliation_create_kontext_post_endpoint, itslearning_affiliation_orgas_get_endpoint, itslearning_affiliation_roles_get_endpoint, input_ldap)
        
    log("")
    log("###########################")
    log("# Main execution finished #")
    log("###########################")

if __name__ == "__main__":
    main()
