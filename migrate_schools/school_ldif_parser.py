from ldif import LDIFParser
from helper import get_is_school_object, parse_dn

class BuildSchoolDFLDIFParser(LDIFParser):
    def __init__(self, input_file):
        super().__init__(input_file)
        self.number_of_found_schools = 0
        self.entries_list = []
        self.schools = []

    def handle(self, dn, entry):
        parsed_dn = parse_dn(dn)
        is_school_object = get_is_school_object(parsed_dn)
        if is_school_object:
            self.number_of_found_schools += 1
            self.schools.append(parsed_dn['ou'][0])