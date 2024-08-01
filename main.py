import itertools
import os
import tempfile

from migrate_classes.migrate_classes import migrate_class_data
from migrate_persons.migrate_persons import migrate_person_data
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

        migrate_school_data(log_output_dir, createOrgaPostEndpoint, getOeffAndErsatzUUIDEndpoint, schoolDataInputExcel, schoolDataInputLDAP)
        
    if migrationType == 'PERSONS':
        create_person_post_endpoint = os.environ['MIGRATION_PERSONS_POST_ENDPOINT_CREATE_PERSON']
        create_kontext_post_endpoint = os.environ['MIGRATION_PERSONS_POST_ENDPOINT_CREATE_KONTEXT']
        persons_data_input_ldap = os.environ['MIGRATION_PERSONS_INPUT_LDAP_COMPLETE_PATH']
        schools_get_endpoint = os.environ['MIGRATION_PERSONS_GET_SCHOOLS_ENDPOINT']
        roles_get_endpoint = os.environ['MIGRATION_PERSONS_GET_ROLES_ENDPOINT']
        personenkontexte_for_person_get_endpoint = os.environ['MIGRATION_PERSONS_GET_PERSONENKONTEXTE_FOR_PERSON_ENDPOINT']
        
        if not create_person_post_endpoint:
            raise ValueError("ENV: POST Endpoint For Create Person cannot be null or empty")
        if not create_kontext_post_endpoint:
            raise ValueError("ENV: POST Endpoint For Create Kontext cannot be null or empty")
        if not persons_data_input_ldap:
            raise ValueError("ENV: Input path for LDAP cannot be null or empty")
        if not schools_get_endpoint:
            raise ValueError("ENV: Get Endpoint for Schools cannot be null or empty")
        if not roles_get_endpoint:
            raise ValueError("ENV: Get Endpoint for Roles cannot be null or empty")
        if not personenkontexte_for_person_get_endpoint:
            raise ValueError("ENV: Get Endpoint for Personenkontexte For Person cannot be null or empty")

        tmpFiles = chunk_input_file(persons_data_input_ldap)

        for chunkFile in tmpFiles:
            migrate_person_data(log_output_dir, create_person_post_endpoint, create_kontext_post_endpoint, chunkFile, schools_get_endpoint, roles_get_endpoint, personenkontexte_for_person_get_endpoint)
        
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
        
        
    print("Main execution finished.")


def chunk_input_file(personsDataInputLDAP):

    temp_files = (tempfile.NamedTemporaryFile(mode="w+t", prefix="personen_import") for _ in itertools.count())

    # Split the input LDIF to keep the memory footprint manageable
    # Luckily LDIF splits its records by simply putting an empty line in between them
    MAX_DATASETS_PER_FILE = 10000
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