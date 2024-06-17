import os
from migrate_persons.migrate_persons import migrate_person_data
from migrate_schools import migrate_school_data


def main():
    print("Main execution started.")
    migrationType = os.environ.get('MIGRATION_TYPE')
    if migrationType == 'SCHOOLS':
        postEndpoint = os.environ['MIGRATION_SCHOOLS_POST_ENDPOINT']
        getOeffAndErsatzUUIDEndpoint = os.environ['MIGRATION_SCHOOLS_UUID_ERSATZ_OEFFENTLICH_ENDPOINT']
        inputExcel = os.environ['MIGRATION_SCHOOLS_INPUT_EXCEL_COMPLETE_PATH']
        inputLDAP = os.environ['MIGRATION_SCHOOLS_INPUT_LDAP_COMPLETE_PATH']

        migrate_school_data(postEndpoint, getOeffAndErsatzUUIDEndpoint, inputExcel, inputLDAP)
        
    if migrationType == 'PERSONS':
        createPersonPostEndpoint = os.environ['MIGRATION_PERSONS_POST_ENDPOINT_CREATE_PERSON']
        createKontextPostEndpoint = os.environ['MIGRATION_PERSONS_POST_ENDPOINT_CREATE_KONTEXT']
        inputLDAP = os.environ['MIGRATION_PERSONS_INPUT_LDAP_COMPLETE_PATH']
        schoolsGetEndpoint = os.environ['MIGRATION_PERSONS_GET_SCHOOLS_ENDPOINT']
        rolesGetEndpoint = os.environ['MIGRATION_PERSONS_GET_ROLES_ENDPOINT']
        
        if not createPersonPostEndpoint:
            raise ValueError("POST Endpoint For Create Person cannot be null or empty")
        if not createKontextPostEndpoint:
            raise ValueError("POST Endpoint For Create Kontext cannot be null or empty")
        if not inputLDAP:
            raise ValueError("Input path for LDAP cannot be null or empty")
        if not schoolsGetEndpoint:
            raise ValueError("Get Endpoint for Schools cannot be null or empty")
        if not rolesGetEndpoint:
            raise ValueError("Get Endpoint for Roles cannot be null or empty")

        migrate_person_data(createPersonPostEndpoint, createKontextPostEndpoint, inputLDAP, schoolsGetEndpoint, rolesGetEndpoint)
        
    print("Main execution finished.")

if __name__ == "__main__":
    main()