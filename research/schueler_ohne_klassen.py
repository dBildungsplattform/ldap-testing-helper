from base64 import b64decode
import os
import sys
import pandas as pd
from openpyxl import load_workbook
import ldap.dn
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from migrate_persons.person_ldif_parser import BuildPersonDFLDIFParser


def check_schueler_ohne_klassen(input_path_ldap):
    print(f"Start Migration School Data")
    
    with open(input_path_ldap, 'rb') as input_file:
        parser = BuildPersonDFLDIFParser(input_file)
        parser.parse()
    df_ldap = pd.DataFrame(parser.entries_list)
    
    total_persons = 0
    total_schueler = 0
    schuelerOhneKlasse = 0
    aktiveSchuelerOhneKlasse = 0
    for index, row in df_ldap.iterrows():
        total_persons += 1
        memberOf = [singleMemberOf.decode('utf-8') if isinstance(singleMemberOf, bytes) else singleMemberOf for singleMemberOf in row['memberOf']]

        if memberOf is not None and len(memberOf) > 0:       
            memberOf_filtered = [mo for mo in memberOf if mo]
            is_deactive = any(mo and 'DeaktivierteKonten' in mo for mo in memberOf_filtered)
            is_schueler = any(mo and 'schueler-' in mo for mo in memberOf_filtered)
            has_klassen = any(mo and 'klassen' in mo for mo in memberOf_filtered)
        
            if is_schueler:
                total_schueler+=1
            
            if is_schueler and not has_klassen:
                schuelerOhneKlasse += 1
                if(not is_deactive):
                    aktiveSchuelerOhneKlasse += 1
            if is_schueler and has_klassen and not is_deactive:
                print(memberOf)
        
    print(f"Number of Total Persons: {total_persons}")
    print(f"Number Of Total Schueler: {total_schueler}")
    print(f"Number of All persons where 'memberOf' starts with 'schueler-' and 'klassen' is missing: {schuelerOhneKlasse}")
    print(f"Number of Active persons where 'memberOf' starts with 'schueler-' and 'klassen' is missing: {aktiveSchuelerOhneKlasse}")
        
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python script.py <path_to_ldap_file>")
        sys.exit(1)
    
    input_path_ldap = sys.argv[1]
    check_schueler_ohne_klassen(input_path_ldap)