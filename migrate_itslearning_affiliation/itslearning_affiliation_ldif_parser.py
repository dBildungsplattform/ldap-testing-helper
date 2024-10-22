from ldif import LDIFParser
from helper import get_is_itslearning_lehrer_or_admin_group_object, log, parse_dn

class BuildItslearningGroupsDFLDIFParser(LDIFParser):
    def __init__(self, input_file):
        super().__init__(input_file)
        self.number_of_found_itslearning_admin_or_lehrer_groups = 0
        self.islearning_groups = []

    def handle(self, dn, entry):
        parsed_dn = parse_dn(dn)
        is_itslearning_group_object = get_is_itslearning_lehrer_or_admin_group_object(parsed_dn, entry)
        if is_itslearning_group_object == True:
            self.number_of_found_itslearning_admin_or_lehrer_groups += 1
            log(f"Identified Itslearning Group Nr: {self.number_of_found_itslearning_admin_or_lehrer_groups}")
            log(f"{parsed_dn}")
            log(f"")
            self.islearning_groups.append(entry)