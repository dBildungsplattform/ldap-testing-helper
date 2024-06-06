import os
from migrate_schools import migrate_school_data
from migrate_persons import migrate_person_data


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
        postEndpoint = os.environ['MIGRATION_PERSONS_POST_ENDPOINT']
        inputLDAP = os.environ['MIGRATION_PERSONS_INPUT_LDAP_COMPLETE_PATH']

        migrate_person_data(postEndpoint, inputLDAP)
        
    print("Main execution finished.")

if __name__ == "__main__":
    main()