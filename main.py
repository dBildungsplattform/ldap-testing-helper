import os
from migrate_classes.migrate_classes import migrate_class_data
from migrate_persons.migrate_persons import migrate_person_data
from migrate_schools.migrate_schools import migrate_school_data


def main():
    print("Main execution started.")
    migrationType = os.environ.get('MIGRATION_TYPE')
    if migrationType == 'SCHOOLS':
        createOrgaPostEndpoint = os.environ['MIGRATION_SCHOOLS_POST_ENDPOINT']
        getOeffAndErsatzUUIDEndpoint = os.environ['MIGRATION_SCHOOLS_UUID_ERSATZ_OEFFENTLICH_ENDPOINT']
        schoolDataInputExcel = os.environ['MIGRATION_SCHOOLS_INPUT_EXCEL_COMPLETE_PATH']
        schoolDataInputLDAP = os.environ['MIGRATION_SCHOOLS_INPUT_LDAP_COMPLETE_PATH']
        
        if not createOrgaPostEndpoint:
            raise ValueError("POST Endpoint For Create Organisation cannot be null or empty")
        if not getOeffAndErsatzUUIDEndpoint:
            raise ValueError("GET Endpoint For UUID (Oeffentlich vs Eratz) cannot be null or empty")
        if not schoolDataInputExcel:
            raise ValueError("Input path for Excel cannot be null or empty")
        if not schoolDataInputLDAP:
            raise ValueError("Input path for LDAP cannot be null or empty")

        migrate_school_data(createOrgaPostEndpoint, getOeffAndErsatzUUIDEndpoint, schoolDataInputExcel, schoolDataInputLDAP)
        
    if migrationType == 'PERSONS':
        createPersonPostEndpoint = os.environ['MIGRATION_PERSONS_POST_ENDPOINT_CREATE_PERSON']
        createKontextPostEndpoint = os.environ['MIGRATION_PERSONS_POST_ENDPOINT_CREATE_KONTEXT']
        personsDataInputLDAP = os.environ['MIGRATION_PERSONS_INPUT_LDAP_COMPLETE_PATH']
        schoolsGetEndpoint = os.environ['MIGRATION_PERSONS_GET_SCHOOLS_ENDPOINT']
        rolesGetEndpoint = os.environ['MIGRATION_PERSONS_GET_ROLES_ENDPOINT']
        personenkontexteForPersonGetEndpoint = os.environ['MIGRATION_PERSONS_GET_PERSONENKONTEXTE_FOR_PERSON_ENDPOINT']
        
        if not createPersonPostEndpoint:
            raise ValueError("POST Endpoint For Create Person cannot be null or empty")
        if not createKontextPostEndpoint:
            raise ValueError("POST Endpoint For Create Kontext cannot be null or empty")
        if not personsDataInputLDAP:
            raise ValueError("Input path for LDAP cannot be null or empty")
        if not schoolsGetEndpoint:
            raise ValueError("Get Endpoint for Schools cannot be null or empty")
        if not rolesGetEndpoint:
            raise ValueError("Get Endpoint for Roles cannot be null or empty")
        if not personenkontexteForPersonGetEndpoint:
            raise ValueError("Get Endpoint for Personenkontexte For Person cannot be null or empty")

        migrate_person_data(createPersonPostEndpoint, createKontextPostEndpoint, personsDataInputLDAP, schoolsGetEndpoint, rolesGetEndpoint, personenkontexteForPersonGetEndpoint)
        
    if migrationType == 'CLASSES':
        createOrgaPostEndpoint = os.environ['MIGRATION_CLASSES_POST_ENDPOINT']
        schoolsGetEndpoint = os.environ['MIGRATION_CLASSES_GET_SCHOOLS_ENDPOINT']
        classDataInputLDAP = os.environ['MIGRATION_CLASSES_INPUT_LDAP_COMPLETE_PATH']
        
        if not createOrgaPostEndpoint:
            raise ValueError("POST Endpoint For Create Organisation cannot be null or empty")
        if not schoolsGetEndpoint:
            raise ValueError("Get Endpoint for Schools cannot be null or empty")
        if not classDataInputLDAP:
            raise ValueError("Input path for LDAP cannot be null or empty")
        
        migrate_class_data(createOrgaPostEndpoint, schoolsGetEndpoint, classDataInputLDAP)
        
        
    print("Main execution finished.")

if __name__ == "__main__":
    main()