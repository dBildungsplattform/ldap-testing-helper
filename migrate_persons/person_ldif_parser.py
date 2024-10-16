from ldif import LDIFParser
from helper import log, parse_dn, get_is_uid_object

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
            entryUUID = entry.get('entryUUID', [None])[0]
            givenName = entry.get('givenName', [None])[0]
            sn = entry.get('sn', [None])[0]
            ucsschoolRecordUID = entry.get('ucsschoolRecordUID', [None])[0]
            krb5PrincipalName = entry.get('krb5PrincipalName', [None])[0]
            krb5ValidEnd = entry.get('krb5ValidEnd', [None])[0]
            userPassword = entry.get('userPassword', [None])[0]
            memberOf = entry.get('memberOf', [None])
            
            new_entry = {
                'uid': uid,
                'entryUUID':entryUUID,
                'givenName': givenName,
                'sn': sn,
                'ucsschoolRecordUID':ucsschoolRecordUID,
                'krb5PrincipalName': krb5PrincipalName,
                'krb5ValidEnd':krb5ValidEnd,
                'userPassword': userPassword,
                'memberOf': memberOf
            }
            self.entries_list.append(new_entry)