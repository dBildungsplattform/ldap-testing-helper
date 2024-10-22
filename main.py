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
    log_output_dir = os.environ.get('LOG_OUTPUT_DIR')
    input_ldap_complete_path = os.environ.get('INPUT_LDAP_COMPLETE_PATH')
    input_ldap_sha256_hash = os.environ.get('INPUT_LDAP_SHA256HASH')
    input_excel_schools_complete_path = os.environ.get('INPUT_EXCEL_SCHOOLS_COMPLETE_PATH')
    input_excel_schools_sha256_hash = os.environ.get('INPUT_EXCEL_SCHOOLS_SHA256HASH')
    api_backend_organisationen = os.environ.get('API_BACKEND_ORGANISATIONEN')
    api_backend_orga_root_children = os.environ.get('API_BACKEND_ORGAROOTCHILDREN')
    api_backend_personen = os.environ.get('API_BACKEND_PERSONEN')
    api_backend_dbiam_personenkontext = os.environ.get('API_BACKEND_DBIAMPERSONENKONTEXT')
    api_backend_rolle = os.environ.get('API_BACKEND_ROLLE')
    
    # Validate common environment variables
    migration_type = os.environ.get('MIGRATION_TYPE')
    if not migration_type:
        raise ValueError("ENV: MIGRATION_TYPE cannot be null or empty")
    log_output_dir = os.environ.get('LOG_OUTPUT_DIR')
    if not log_output_dir:
        raise ValueError("ENV: LOG_OUTPUT_DIR cannot be null or empty")
    input_ldap_complete_path = os.environ['INPUT_LDAP_COMPLETE_PATH']
    if not log_output_dir:
        raise ValueError("ENV: INPUT_LDAP_COMPLETE_PATH cannot be null or empty")
    input_ldap_sha256_hash = os.environ['INPUT_LDAP_SHA256HASH']
    if not input_ldap_sha256_hash:
        raise ValueError("ENV: INPUT_LDAP_SHA256HASH cannot be null or empty")
    
    if not get_hash_sha256_for_file(input_ldap_complete_path) == input_ldap_sha256_hash:
        raise ValueError("ENV: INPUT_LDAP_SHA256HASH doesnt match the actual provided input_ldap files hash")
    else: 
        log('LDAP Hashes are matching')

    if migration_type == 'SCHOOLS':
        log("")
        log("Selected Migration Type: SCHOOLS")

        if not api_backend_organisationen:
            raise ValueError("ENV: API_BACKEND_ORGANISATIONEN cannot be null or empty")
        if not api_backend_orga_root_children:
            raise ValueError("ENV: API_BACKEND_ORGAROOTCHILDREN cannot be null or empty")
        if not input_excel_schools_complete_path:
            raise ValueError("ENV: INPUT_EXCEL_SCHOOLS_COMPLETE_PATH cannot be null or empty")
        if not input_excel_schools_sha256_hash:
            raise ValueError("ENV: INPUT_EXCEL_SCHOOLS_SHA256HASH cannot be null or empty")
        
        if not get_hash_sha256_for_file(input_excel_schools_complete_path) == input_excel_schools_sha256_hash:
            raise ValueError("ENV: INPUT_EXCEL_SCHOOLS_COMPLETE_PATH Hash doesnt match the actual provided excel files hash")
        else: 
            log('Excel Hashes are matching')

        migrate_school_data(log_output_dir, api_backend_organisationen, api_backend_orga_root_children, input_excel_schools_complete_path,
                            input_ldap_complete_path)
        
    if migration_type == 'CLASSES':
        log("")
        log("Selected Migration Type: CLASSES")
        
        if not api_backend_organisationen:
            raise ValueError("ENV: API_BACKEND_ORGANISATIONEN cannot be null or empty")

        migrate_class_data(log_output_dir, api_backend_organisationen, input_ldap_complete_path)

    if migration_type == 'PERSONS':
        log("")
        log("Selected Migration Type: PERSONS")
        
        if not api_backend_personen:
            raise ValueError("ENV: API_BACKEND_PERSONEN cannot be null or empty")
        if not api_backend_dbiam_personenkontext:
            raise ValueError("ENV: API_BACKEND_DBIAMPERSONENKONTEXT cannot be null or empty")
        if not api_backend_organisationen:
            raise ValueError("ENV: API_BACKEND_ORGANISATIONEN cannot be null or empty")
        if not api_backend_rolle:
            raise ValueError("ENV: API_BACKEND_ROLLE cannot be null or empty")

        migrate_person_data(log_output_dir, api_backend_personen, api_backend_dbiam_personenkontext, api_backend_organisationen, api_backend_rolle, input_ldap_complete_path)
        
    if migration_type == 'ITSLEARNING_AFFILIATION':
        log("")
        log("Selected Migration Type: ITSLEARNING_AFFILIATION")
        
        if not api_backend_dbiam_personenkontext:
            raise ValueError("ENV: API_BACKEND_DBIAMPERSONENKONTEXT cannot be null or empty")
        if not api_backend_organisationen:
            raise ValueError("ENV: API_BACKEND_ORGANISATIONEN cannot be null or empty")
        if not api_backend_rolle:
            raise ValueError("ENV: API_BACKEND_ROLLE cannot be null or empty")

        migrate_itslearning_affiliation_data(log_output_dir, api_backend_dbiam_personenkontext, api_backend_organisationen, api_backend_rolle, input_ldap_complete_path)
        
    log("")
    log("###########################")
    log("# Main execution finished #")
    log("###########################")

if __name__ == "__main__":
    main()
