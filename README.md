# Migration Tools

For all migrations the following env variables must be provided:

### Required Environment Variables

- **MIGRATION_TYPE**: `"SCHOOLS" || "CLASSES" || "PERSONS" || "ITSLEARNING_AFFILIATION"`
- **CLIENT_ID**: `"spsh"`
- **CLIENT_SECRET**: `<your_client_secret>`
- **USERNAME**: `<username_for_migration_user>`
- **PASSWORD**: `<password_for_migration_user>`
- **GRANT_TYPE**: `"password"`
- **TOKEN_URL**: `<Endpoint for Keycloak SPSH token>`
- **LOG_OUTPUT_DIR**: `<path_for_log_outputs>`
- **INPUT_LDAP**: `<path_to_input_ldap>`
- **INPUT_LDAP_SHA256HASH**: `<sha256_of_input_ldap>`
- **INPUT_EXCEL_SHA256HASH**: `<sha256_of_input_excel>`

### Migration Order

The migrations build on each other and must be executed in the following order:

1. **SCHOOLS**
   
   ***Migrates all schools from LDAP and enriches data using an Excel table.***

   Needed Extra Envs:
   - MIGRATE_SCHOOLS_INPUT_EXCEL
   - MIGRATION_SCHOOLS_POST_ENDPOINT
   - MIGRATION_SCHOOLS_UUID_ERSATZ_OEFFENTLICH_ENDPOINT
   
2. **CLASSES**
   
   ***Migrates all classes from LDAP***.

   Needed Extra Envs:
   - MIGRATION_CLASSES_POST_ENDPOINT
   - MIGRATION_CLASSES_GET_SCHOOLS_ENDPOINT
   
3. **PERSONS**
   
   ***Migrates persons along with their contexts from LDAP (excluding itslearning affiliations)***

   Needed Extra Envs:
   - MIGRATION_PERSONS_POST_ENDPOINT_CREATE_PERSON
   - MIGRATION_PERSONS_POST_ENDPOINT_CREATE_KONTEXT
   - MIGRATION_PERSONS_GET_SCHOOLS_ENDPOINT
   - MIGRATION_PERSONS_GET_ROLES_ENDPOINT
   - MIGRATION_PERSONS_GET_PERSONENKONTEXTE_FOR_PERSON_ENDPOINT
   
4. **ITSLEARNING_AFFILIATION**
   
   ***Migrates the itslearning affiliations for teachers and admins from LDAP***

   Needed Extra Envs:
   - MIGRATE_ITSLEARNING_AFFILIATION_CREATE_KONTEXT_POST_ENDPOINT
   - MIGRATE_ITSLEARNING_AFFILIATION_ORGAS_GET_ENDPOINT
   - MIGRATE_ITSLEARNING_AFFILIATION_ROLES_GET_ENDPOINT

### Additional Endpoints in Env

Depending on the chosen migration, you may need to set additional endpoints in the environment. Make sure to configure them as necessary for each specific migration.

---

