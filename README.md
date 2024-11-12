# Migration Tools

### Common Required Environment Variables

For all migrations the following env variables must be provided:

- **MIGRATION_TYPE**: `"SCHOOLS" || "CLASSES" || "PERSONS" || "ITSLEARNING_AFFILIATION"`
- **CLIENT_ID**: `"spsh"`
- **CLIENT_SECRET**: `<your_client_secret>`
- **USERNAME**: `<username_for_migration_user>`
- **PASSWORD**: `<password_for_migration_user>`
- **GRANT_TYPE**: `"password"`
- **TOKEN_URL**: `<Endpoint for Keycloak SPSH token>`
- **LOG_OUTPUT_DIR**: `<path_for_log_outputs>`
- **INPUT_LDAP_COMPLETE_PATH**: `<path_to_input_ldap>`
- **INPUT_LDAP_SHA256HASH**: `<sha256_of_input_ldap>`

### Code Structure

- Each Migration has its own module (migrate_schools / migrate_classes / migrate_persons / migrate_itslearning_affiliation)
- Each Migration uses its own LDIFParser to extract the for the migration needed information from the LDAP-File
- Each Migration uses only functions defined in its own module, third party libraries or explicit shared functions defined in helper.py. (No shared Function Usage across modules)


### Overview About The Individual Migrations

Technically the migrations run independantly from each other. Logically they build on each other and must therefor be executed in the following order.:

1. **SCHOOLS**
   
   ***Migrates all schools from LDAP and enriches data using an Excel table.***

   Needed Extra Envs:
   - INPUT_EXCEL_SCHOOLS_COMPLETE_PATH
   - INPUT_EXCEL_SCHOOLS_SHA256HASH
   - API_BACKEND_ORGANISATIONEN
   - API_BACKEND_ORGAROOTCHILDREN
   
2. **CLASSES**
   
   ***Migrates all classes from LDAP***.

   Needed Extra Envs:
   - API_BACKEND_ORGANISATIONEN
   
3. **PERSONS**
   
   ***Migrates persons along with their contexts from LDAP (excluding itslearning affiliations)***

   Needed Extra Envs:
   - API_BACKEND_PERSONEN
   - API_BACKEND_DBIAMPERSONENKONTEXT
   - API_BACKEND_ORGANISATIONEN
   - API_BACKEND_ROLLE
   
4. **ITSLEARNING_AFFILIATION**
   
   ***Migrates the itslearning affiliations for teachers and admins from LDAP***

   Needed Extra Envs:
   - API_BACKEND_DBIAMPERSONENKONTEXT
   - API_BACKEND_ORGANISATIONEN
   - API_BACKEND_ROLLE

### SQL Skript for Itslearning Befristung

There is the following SQL script which needs to be run after running the migrations 1-4 (Sets Befristung for Itslearning Kontexts where needed):

- VAR_ROLLE_ID_ITSLEARNING_ADMIN: Replace with actual rolle_id (UUID)
- VAR_ROLLE_ID_ITSLEARNING_LEHRER: Replace with actual rolle_id (UUID)

```sql
WITH selected_kontexts AS (
    SELECT *
    FROM public.personenkontext
    WHERE person_id IN (
        SELECT person_id
        FROM public.personenkontext
        WHERE befristung IS NOT NULL
    )
    AND (rolle_Id = 'VAR_ROLLE_ID_ITSLEARNING_ADMIN' OR rolle_id = 'VAR_ROLLE_ID_ITSLEARNING_LEHRER')
),
befristung_source AS (
    SELECT person_id, MAX(befristung) AS befristung
    FROM public.personenkontext
    WHERE befristung IS NOT NULL
    AND rolle_id NOT IN ('VAR_ROLLE_ID_ITSLEARNING_ADMIN', 'VAR_ROLLE_ID_ITSLEARNING_LEHRER')
    GROUP BY person_id
)
 
UPDATE public.personenkontext pk
SET befristung = bs.befristung
FROM selected_kontexts sk
JOIN befristung_source bs ON sk.person_id = bs.person_id
WHERE pk.person_id = sk.person_id
AND (pk.rolle_Id = sk.rolle_Id)
AND pk.befristung IS NULL;
