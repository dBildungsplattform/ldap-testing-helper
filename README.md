# migration-tools

For all tools set parameters in the env.list file as follows:
(Same as for Insomnia)

CLIENT_ID=spsh
CLIENT_SECRET=xxxx
USERNAME=xxxx
PASSWORD=xxxx
GRANT_TYPE=password
TOKEN_URL=http://localhost:8080/realms/SPSH/protocol/openid-connect/token

Currently there are 3 Different Migrations which can be set in the env.list (SCHOOLS & CLASSES & PERSONS).
The migrations build on each other an need to be run in Order 1. SCHOOLS, 2. CLASSES, 3.PERSONS
Depending on the chosen Migration Further Parameters need to be set

1. SCHHOOS
   - This Migration migrates all Schools in the LDAP and enriches the data for public schools using an Excel Table
2. CLASSES
   - This Migration migrates all Classes in the LDAP
3. PERSONS
   - This Migration migrates persones with their roles and merges schooladmins. 
