from ldif import LDIFParser
from helper import get_is_class_object, parse_dn

class BuildClassesDFLDIFParser(LDIFParser):
    def __init__(self, input_file):
        super().__init__(input_file)
        self.number_of_found_classes = 0
        self.classes = []

    def handle(self, dn, entry):
        parsed_dn = parse_dn(dn)
        is_class_object = get_is_class_object(parsed_dn)
        if is_class_object:
            self.number_of_found_classes += 1
            self.classes.append(parsed_dn['cn'][0])