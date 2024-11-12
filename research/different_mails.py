from base64 import b64decode
import os
import sys
import pandas as pd
import ldap.dn
from datetime import datetime, timedelta
from ldif import LDIFParser

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from helper import get_is_uid_object, log, parse_dn

class BuildPersonDFLDIFParser(LDIFParser):
    def __init__(self, input_file):
        super().__init__(input_file)
        self.number_of_found_persons = 0
        self.entries_list = []

    def handle(self, dn, entry):
        parsed_dn = parse_dn(dn)
        is_person = get_is_uid_object(parsed_dn)
        if is_person:
            self.number_of_found_persons += 1
            log(f"Identified Person Nr: {self.number_of_found_persons}")
            log(f"{parsed_dn}")
            log(f"")
            uid = parsed_dn['uid'][0]
            krb5PrincipalName = entry.get('krb5PrincipalName', [None])[0]
            oxDisplayName = entry.get('oxDisplayName', [None])[0]
            mail = entry.get('mail', [None])[0]
            mailPrimaryAddress = entry.get('mailPrimaryAddress', [None])[0]
            
            new_entry = {
                'uid': uid,
                'krb5PrincipalName': krb5PrincipalName,
                'oxDisplayName': oxDisplayName,
                'mail': mail,
                'mailPrimaryAddress': mailPrimaryAddress,
            }
            self.entries_list.append(new_entry)


def check_different_mails(input_path_ldap):
    log(f"Start Migration School Data")
    
    with open(input_path_ldap, 'rb') as input_file:
        parser = BuildPersonDFLDIFParser(input_file)
        parser.parse()
    df_ldap = pd.DataFrame(parser.entries_list)
    
    decoded_entries = []
    for index, row in df_ldap.iterrows():
        decoded_entry = {
            'uid': row['uid'].decode('utf-8') if isinstance(row['uid'], bytes) else row['uid'],
            'krb5PrincipalName': row['krb5PrincipalName'].decode('utf-8') if isinstance(row['krb5PrincipalName'], bytes) else row['krb5PrincipalName'],
            'oxDisplayName': row['oxDisplayName'].decode('utf-8') if isinstance(row['oxDisplayName'], bytes) else row['oxDisplayName'],
            'mail': row['mail'].decode('utf-8') if isinstance(row['mail'], bytes) else row['mail'],
            'mailPrimaryAddress': row['mailPrimaryAddress'].decode('utf-8') if isinstance(row['mailPrimaryAddress'], bytes) else row['mailPrimaryAddress'],
        }
        decoded_entries.append(decoded_entry)
    df_decoded = pd.DataFrame(decoded_entries)
    output_path = 'research_email_data.xlsx'
    df_decoded.to_excel(output_path, index=False)
    log(f"Decoded data saved to {output_path}")
                
        
if __name__ == '__main__':
    if len(sys.argv) != 2:
        log("Usage: python script.py <path_to_ldap_file>")
        sys.exit(1)
    
    input_path_ldap = sys.argv[1]
    check_different_mails(input_path_ldap)