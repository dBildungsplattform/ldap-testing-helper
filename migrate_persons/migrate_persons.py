import concurrent.futures
import concurrent.futures
from datetime import datetime

import numpy as np
import pandas as pd

from migrate_persons.person_ldif_parser import BuildPersonDFLDIFParser
from migrate_persons.process_df_part import process_df_part


def migrate_person_data(combined_results, create_person_post_endpoint, create_kontext_post_endpoint,
                        input_file, roleid_sus, roleid_schuladmin, roleid_lehrkraft, roleid_schulbegleitung,
                        school_uuid_dnr_mapping, class_nameAndAdministriertvon_uuid_mapping):
    print(f"{datetime.now()} :Start Migrating Chunk")
    with input_file:
        input_file.seek(0)
        parser = BuildPersonDFLDIFParser(input_file)
        parser.parse()
    df_ldap = pd.DataFrame(parser.entries_list)

    df_parts = np.array_split(df_ldap, 100)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_df_part, index, df_part, school_uuid_dnr_mapping,
                                   class_nameAndAdministriertvon_uuid_mapping, roleid_sus, roleid_schuladmin,
                                   roleid_lehrkraft, roleid_schulbegleitung, create_person_post_endpoint,
                                   create_kontext_post_endpoint) for index, df_part in enumerate(df_parts)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]

    combined_results['number_of_found_persons'] += parser.number_of_found_persons
    for result in results:
        for key in combined_results.keys():
            if isinstance(combined_results[key], list):
                combined_results[key].extend(result[key])
            else:
                if key != 'number_of_found_persons': # This is handled separately
                    combined_results[key] += result[key]
