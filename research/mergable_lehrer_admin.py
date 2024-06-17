from base64 import b64decode
import os
import sys
import pandas as pd
from openpyxl import load_workbook
import ldap.dn
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from migrate_persons.person_helper import get_schools_dnr_for_create_admin_kontext
from migrate_persons.person_ldif_parser import BuildPersonDFLDIFParser


def check_mergable_lehrer_admins(input_path_ldap):
    print(f"Start Migration School Data")
    
    with open(input_path_ldap, 'rb') as input_file:
        parser = BuildPersonDFLDIFParser(input_file)
        parser.parse()
    df_ldap = pd.DataFrame(parser.entries_list)
    
    potantial_mergable_lehrer_admins = []
    all_lehrer = []
    
    total_persons = 0
    for index, row in df_ldap.iterrows():
        total_persons += 1
        memberOf = [singleMemberOf.decode('utf-8') if isinstance(singleMemberOf, bytes) else singleMemberOf for singleMemberOf in row['memberOf']]
        
        username = row['uid'].decode('utf-8') if isinstance(row['uid'], bytes) else row['uid']
        sn = row['sn'].decode('utf-8') if isinstance(row['sn'], bytes) else row['sn']
        given_name = row['givenName'].decode('utf-8') if isinstance(row['givenName'], bytes) else row['givenName']
        is_potential_lehrer_admin = ('#admin' in (sn or '').lower()) and ('sekadmin' not in (username or '').lower()) and ('extadmin' not in (username or '').lower())
        
        if memberOf is not None and len(memberOf) > 0:       
            memberOf_filtered = [mo for mo in memberOf if mo]
            is_admin_member_of = any(mo and 'admins-' in mo for mo in memberOf_filtered)
            is_lehrer_member_of = any(mo and 'lehrer-' in mo for mo in memberOf_filtered)

            if(is_potential_lehrer_admin and is_admin_member_of):
                schule = None
                for mo in memberOf_filtered:
                    if 'admins-' in mo:
                        schule = mo.split('-')[1].split(',')[0]
                        break
                if schule is not None:
                    potantial_mergable_lehrer_admins.append({
                        'given_name':given_name,
                        'schule':schule
                    })
            elif(is_lehrer_member_of):
                schule = None
                for mo in memberOf_filtered:
                    if 'lehrer-' in mo:
                        schule = mo.split('-')[1].split(',')[0]
                        break
                if schule is not None:
                    all_lehrer.append({
                        'username':username,
                        'schule':schule
                    })
                
    
    total_number_of_mergable = 0
    for lehrer_admin in potantial_mergable_lehrer_admins:
        for lehrer in all_lehrer:
            if lehrer_admin['given_name'] == lehrer['username'] and lehrer_admin['schule'] == lehrer['schule']:
                total_number_of_mergable += 1
                print(f"Found One Mergable: {lehrer_admin['given_name']} == {lehrer['username']} AND {lehrer_admin['schule']} == {lehrer['schule']}")
                break
            
    print(f"Number of Total Persons: {total_persons}")
    print(f"Number of Potential Mergable Admin Persons {len(potantial_mergable_lehrer_admins)}")
    print(f"Number of Lehrer Persons {len(all_lehrer)}")
    print(f"Total Number of Mergable: {total_number_of_mergable}")
        
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python script.py <path_to_ldap_file>")
        sys.exit(1)
    
    input_path_ldap = sys.argv[1]
    check_mergable_lehrer_admins(input_path_ldap)