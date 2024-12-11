"""Script to download the list of BDC studies from Gen3

(using it as a source of truth).

USAGE:
  python bin/get_bdc_studies_from_gen3.py output.csv
The BDC Gen3 instance is hosted at https://gen3.biodatacatalyst.nhlbi.nih.gov/
"""

import csv
import json
import logging
import re
import urllib.parse
from collections import Counter
from datetime import datetime

import click
import requests

# Configuration
# The number of items to download at a single go. This is usually capped by the
# Gen3 instance, so you need to make sure that this limit is lower than theirs!
GEN3_DOWNLOAD_LIMIT = 50

# Turn on logging
logging.basicConfig(level=logging.INFO)


def download_gen3_list(input_url, download_limit=GEN3_DOWNLOAD_LIMIT):
    """Download list of studies from gen3

    This function helps download a list of items from Gen3 by downloading
    the list and -- as long as there are as many items as the download_limit --
    by using `offset` to get the next set of results.

    :param input_url: The URL to download. This function will concatenate
    `&limit=...&offset=...` to it, so it should end with arguments or at least a
    question mark.

    :param download_limit: The maximum number of items to download (as set by
    `limit=...`). Note that Gen3 has an internal limit, so you should make sure
    your limit is smaller than that -- otherwise, you will request e.g. 3000
    entries but retrieve the Gen3 limit (say, 2000), which this function will
    interpret to mean that all entries have been downloaded.

    :return: A list of retrieved strings. (This function only works when the
    result is a simple JSON list of strings.)

    """
    complete_list = []
    offset = 0
    while True:
        url = input_url + f"&limit={download_limit}&offset={offset}"
        logging.debug(f"Requesting GET {url} from Gen3")
        partial_list_response = requests.get(url, timeout=60)
        if not partial_list_response.ok:
            raise RuntimeError(
                f"Could not download discovery_metadata from BDC Gen3 {url}: " +
                f"{partial_list_response.status_code} "
                f"{partial_list_response.text}")

        partial_list = partial_list_response.json()
        complete_list.extend(partial_list)
        if len(partial_list) < GEN3_DOWNLOAD_LIMIT:
            # No more entries to download!
            break

        # Otherwise, increment offset by DOWNLOAD_SIZE
        offset += download_limit

    # Make sure we don't have duplicates -- this is more likely to be an error
    # in the offset algorithm than an actual
    # error.
    if len(complete_list) != len(set(complete_list)):
        duplicate_ids = sorted([ident for ident, count in
                                Counter(complete_list).items() if count > 1])
        logging.warning(f"Found duplicate discovery_metadata: {duplicate_ids}")

    return complete_list

def retrieve_bdc_study_info(bdc_gen3_base_url, study_id):
    """Fetch metadata for study_id from endpoint following bdc_gen3_base_url

    For a given study_id, fetch the metadata for the study from the BDC Gen3
    endpoint, and populate a dict with the critical information.
    """
    # Download study information.
    url = urllib.parse.urljoin(bdc_gen3_base_url, f'/mds/metadata/{study_id}')
    study_info_response = requests.get(url, timeout=60)
    if not study_info_response.ok:
        raise RuntimeError(f"Could not download study information "
                           f"about study {study_id} at URL {url}.")

    return study_info_response.json()

def retrieve_dbgap_info(fhir_id):
    """Fetch dbgap study info from API
    """
    dbgap_url = (f'https://dbgap-api.ncbi.nlm.nih.gov/'
                 f'fhir/x1/ResearchStudy?_id={fhir_id}')
    dbgap_response = requests.get(dbgap_url, timeout=60)
    if not dbgap_response.ok:
        raise RuntimeError(f"Could not download dbgap information "
                           f"about fhir id {fhir_id} at URL {dbgap_url}")
    return dbgap_response.json()

def retrieve_study_info_list(bdc_gen3_base_url):
    """Download study_info for all studies in BDC Gen3"""
    # Step 1. Download all the discovery_metadata from the BDC Gen3 Metadata
    # Service (MDS).
    mds_discovery_metadata_url = urllib.parse.urljoin(
        bdc_gen3_base_url,
        f'/mds/metadata?_guid_type=discovery_metadata'
    )

    logging.debug(f"Downloading study identifiers from MDS discovery "
                  f"metadata URL: {mds_discovery_metadata_url}.")
    discovery_list = download_gen3_list(mds_discovery_metadata_url,
                                        download_limit=GEN3_DOWNLOAD_LIMIT)
    logging.info(f"Downloaded {len(discovery_list)} discovery_metadata from "
                 f"BDC Gen3 with a limit of {GEN3_DOWNLOAD_LIMIT}.")
    sorted_study_ids = sorted(discovery_list)

    outlist = []
    for study_id in sorted_study_ids:
        study_info = retrieve_bdc_study_info(bdc_gen3_base_url, study_id)
        if study_info:
            outlist.append(study_info)
    return outlist

def format_name_notes(name='', short_name=''):
    """Format a notes string for the study name"""
    if name:
        return f"Name: {name}, short name: {short_name}.\n"
    elif short_name:
        return f"Short name: {short_name}.\n"
    return ''

def get_study_name(gen3_discovery):
    """Extract the study name and notes field from the gen3 metadata"""
    # We prefer full_name to name, which is often identical to the short name.
    notes = ''
    study_name = ''
    name = ''
    short_name = ''
    if 'full_name' in gen3_discovery:
        study_name = gen3_discovery['full_name']
        name = gen3_discovery.get('name', '')
        short_name = gen3_discovery.get('short_name', '')
    elif 'name' in gen3_discovery:
        study_name = gen3_discovery['name']
        short_name = gen3_discovery.get('short_name', '')
    elif 'short_name' in gen3_discovery:
        study_name = gen3_discovery['short_name']
    else:
        study_name = '(no name)'
    return (study_name, name, short_name,)

def get_study_design(dbgap_info):
    """Extract the study design from a nest of variables with care"""
    try:
        return dbgap_info['entry'][0]['resource']['category'][0]['text']
    except (KeyError, IndexError):
        return ""

def get_program(gen3_discovery, default=""):
    """Extract the program string from the tags, if present."""
    tags = gen3_discovery.get('tags', None)
    if tags:
        return tags[0].get('name')
    else:
        return default

def make_csv_dict_from_study_info(study_info):
    "Take the response from Gen3 and build the necessary CSV dict"

    # Reset the variables we need.
    program_names = []
    description = ''

    # Gen3 doesn't have a last-modified date. We could eventually try to
    # download that directly from dbGaP (but why?), but it's easier to use the
    # current date.
    last_modified = str(datetime.now().date())

    if not 'gen3_discovery' in study_info:
        return {}
    gen3_discovery = study_info['gen3_discovery']
    study_id = gen3_discovery['study_id']

    (study_name, name, short_name) = get_study_name(gen3_discovery)
    notes = format_name_notes(name, short_name)

    # Program name.
    if 'authz' in gen3_discovery:
        # authz is in the format
        # /programs/topmed/projects/ECLIPSE_DS-COPD-MDS-RD
        match = re.fullmatch(r'^/programs/(.*)/projects/(.*)$',
                             gen3_discovery['authz'])
        if match:
            program_names.append(match.group(1))
            # study_short_name = match.group(2)

    # Description.
    description = gen3_discovery.get('study_description', '')

    # Extract accession and consent.
    m = re.match(r'^(phs.*?)(?:\.(c\d+))?$', study_id)
    if not m:
        logging.warning(f"Skipping study_id '{study_id}' as non-dbGaP "
                        f"identifiers are not currently supported by "
                        f"Dug.")
        return None

    if m.group(2):
        accession = m.group(1)
        consent = m.group(2)
    else:
        accession = study_id
        consent = ''

    # Remove any blank program names.
    program_names = filter(lambda n: n != '', program_names)

    return {
        'Accession': accession,
        'Consent': consent,
        'Study Name': study_name,
        'Description': description,
        'Program': '|'.join(sorted(set(program_names))),
        'Last modified': last_modified,
        'Notes': notes.strip()
    }


def write_list_to_csv_file(study_info_list, output):
    """Take a list of objs from gen3 API, write a CSV summary file to output
    """

    # Step 2. For every study ID, write out an entry into the CSV output file.
    csv_writer = csv.DictWriter(output, fieldnames=['Accession', 'Consent',
                                                    'Study Name', 'Program',
                                                    'Last modified', 'Notes',
                                                    'Description'])

    csv_writer.writeheader()
    for study_info in study_info_list:
        study_info_dict = make_csv_dict_from_study_info(study_info)
        if study_info_dict:
            csv_writer.writerow(study_info_dict)

def make_kgx(nodes, edges):
    """Very basic kgx format assembler"""
    kgx = {
        'nodes': nodes,
        'edges': edges,
    }

    return kgx

def make_study_kgx_node(gen3_discovery, study_id):
    """Generate a kgx-style node dict from the Gen3 study info JSON"""
    # dbgap_info = retrieve_dbgap_info(study_info['DBGAP_FHIR_Id'])['entry'][0]
    (study_name, name, short_name) = get_study_name(gen3_discovery)
    node = {
        "id": study_id,
        "name": name or study_name,
        "full_name": study_name,
        "short_study_name": short_name,
        "categories": [
            "biolink:Study"
        ],
        "description" : gen3_discovery.get("study_description", ""),
        "iri": gen3_discovery.get("dbgap_url", ""),
        "abstract": gen3_discovery.get("doi_descriptions", ""),
        "program": get_program(gen3_discovery),
        "study_design": gen3_discovery.get("DBGAP_FHIR_Category", ""),
        # "publications": gen3_discovery.get("DBGAP_FHIR_Citers", {}),
        "release_date": gen3_discovery.get("DBGAP_FHIR_ReleaseDate", ""),
    }
    return node

def get_id_and_consent(study_id):
    """Use a regex to find the participant set ID and consent from the study ID

    IF it doesn't match the standard dbgap-style ID, return an empty string for
    consent.
    """
    m = re.search(r'^(?P<base_id>phs\d+\.v\d+\.p\d+)\.(?P<consent_id>c\d+)$',
                  study_id)
    if m:
        return m.groups()
    else:
        return (study_id, '')

def make_consent_info_dict(gen3_discovery):
    """Build a structure for the relevant consent and return it as a dict.
    """
    consent_info = {
        "id": gen3_discovery["study_id"],
        "iri": gen3_discovery.get("doi_url", ""),
        "name": gen3_discovery.get("project_id", ""),
        "categories": [
            "biolink:StudyPopulation",
        ],
        "authz": gen3_discovery.get("authz", ""),
        "num_subjects": gen3_discovery.get("_subjects_count", ""),
        "study_citation": gen3_discovery.get("doi_citation", ""),
        "consent_text": gen3_discovery.get("dbgap_consent_text", ""),
    }
    return consent_info

def make_edge_link(study_id, consent_id):
    """Build a structure for the edge between study and consent
    """
    edge_info = {
        "subject": study_id,
        "predicate": "biolink:related_to",
        "object": consent_id,
    }
    return edge_info

def make_kgx_lists(study_info_list):
    """Take studies from gen3 and build a list of nodes

    This will consolidate studies which only differ in consent IDs into a single
    node and will populate the 'consents' field with their details.
    """
    study_dict = {}
    consent_list = []
    edge_list = []

    for study_info in study_info_list:
        if not 'gen3_discovery' in study_info:
            continue
        gen3_discovery = study_info['gen3_discovery']
        (study_id, consent) = get_id_and_consent(gen3_discovery['study_id'])
        if not consent:
            # Non-dbgap IDs not supported by Dug
            continue
        if not study_id in study_dict:
            study_dict[study_id] = make_study_kgx_node(
                gen3_discovery, study_id)
        consent_list.append(make_consent_info_dict(gen3_discovery))
        edge_list.append(make_edge_link(study_id, gen3_discovery['study_id']))
    return (list(study_dict.values()) + consent_list, edge_list)

# Set up command line arguments.
@click.command()
@click.argument('output', type=click.File('w'), required=True)
@click.option('--bdc-gen3-base-url',
              help='The base URL of the BDC Gen3 instance (before `/mds/...`)',
              type=str,
              metavar='URL',
              default='https://gen3.biodatacatalyst.nhlbi.nih.gov/')
@click.option('--kgx-file', type=click.File('w'), default=None,
              required=False, help="Optional KGX output file")
def get_bdc_studies_from_gen3(output, bdc_gen3_base_url, kgx_file):
    """
    Retrieve BDC studies from the BDC Gen3 Metadata Service (MDS) instance and
    write them out as a CSV file to OUTPUT
    for get_dbgap_data_dicts.py to use.
    \f
    # \f truncates the help text as per
    https://click.palletsprojects.com/en/8.1.x/documentation/#truncating-help-texts

    :param output: The CSV file to be generated.
    :param bdc_gen3_base_url: The BDC Gen3 base URL (i.e. everything before the
    `/mds/...`). Defaults to https://gen3.biodatacatalyst.nhlbi.nih.gov/.
    """
    study_info_list = retrieve_study_info_list(bdc_gen3_base_url)

    write_list_to_csv_file(study_info_list, output)


    if kgx_file:
        (nodes, edges) = make_kgx_lists(study_info_list)
        logging.info("Writing out %d kgx nodes to file %s", len(nodes),
                      kgx_file)
        json.dump(make_kgx(nodes, edges), kgx_file, indent=2)

# def _depricated_code():
#     """This code previously came after the csv_writer.writerow column but
#     also came after an exit(0) statement that made it unreachable. It has been
#     wrapped in a function definition to help code organization.

#     Possibly consider just deleting this whole block?
#     """
#     assert (file_format == 'CSV',
#             'HEAL VLMD CSV is the only currently supported input format.')

#     with open(click.format_filename(input_file), 'r') as input:
#         # dbGaP files are represented in XML as `data_table`s. We start by
#         # creating one.
#         data_table = ETree.Element('data_table')

#         # Write out the study_id.
#         if not study_id:
#             # If no study ID is provided, use the input filename.
#             # TODO: once we support JSON, we can use either root['title'] or
#             # root['description'] here.
#             study_id = os.path.basename(input_file)
#         else:
#             # Assume it is an HDP identifier, so add the HDP_ID_PREFIX.
#             study_id = HDP_ID_PREFIX + study_id
#         data_table.set('study_id', study_id)

#         # Add the APPL ID.
#         if appl_id:
#             data_table.set('appl_id', appl_id)

#         # Add the study title.
#         # Note: not a dbGaP XML field! We make this up for communication.
#         if study_name:
#             data_table.set('study_name', study_name)

#         # Record the creation date as this moment.
#         data_table.set('date_created', datetime.now().isoformat())

#         # Read input file and convert variables into
#         if file_format == 'CSV':
#             reader = csv.DictReader(input)

#             # Some counts that are currently useful.
#             counters = defaultdict(int)

#             unique_variable_ids = set()
#             for index, row in enumerate(reader):
#                 counters['row'] += 1
#                 row_index = index + 1  # Convert from zero-based index to one-based index.

#                 variable = ETree.SubElement(data_table, 'variable')

#                 # Variable name
#                 var_name = row.get('name')
#                 if not var_name:
#                     logging.error(f"No variable name found in row on line {index + 1}, skipping.")
#                     counters['no_varname'] += 1
#                     continue
#                 counters['has_varname'] += 1
#                 # Make sure the variable ID is unique (by adding `_1`, `_2`, ... to the end of it).
#                 variable_index = 0
#                 while var_name in unique_variable_ids:
#                     variable_index += 1
#                     var_name = row['name'] + '_' + variable_index
#                 variable.set('id', var_name)
#                 if var_name != row['name']:
#                     logging.warning(f"Duplicate variable ID detected for {row['name']}, so replaced it with "
#                                     f"{var_name} -- note that the name element is unchanged.")
#                 name = ETree.SubElement(variable, 'name')
#                 name.text = var_name

#                 # Variable title
#                 # NOTE: this is not yet supported by Dug!
#                 if row.get('title'):
#                     title = ETree.SubElement(variable, 'title')
#                     title.text = row['title']
#                     counters['has_title'] += 1
#                 else:
#                     counters['no_title'] += 1

#                 # Variable description
#                 if row.get('description'):
#                     desc = ETree.SubElement(variable, 'description')
#                     desc.text = row['description']
#                     counters['has_description'] += 1
#                 else:
#                     counters['no_description'] += 1

#                 # Module (questionnaire/subsection name) Export the `module` field so that we can look for
#                 # instruments.
#                 #
#                 # TODO: this is a custom field. Instead of this, we could export each data dictionary as a separate
#                 # dbGaP file. Need to check to see what works better for Dug ingest.
#                 if row.get('module'):
#                     variable.set('module', row['module'])
#                     if 'module_counts' not in counters:
#                         counters['module_counts'] = defaultdict(int)
#                     counters['module_counts'][row['module']] += 1
#                 else:
#                     counters['no_module'] += 1

#                 # Constraints

#                 # Minium and maximum values
#                 if row.get('constraints.maximum'):
#                     logical_max = ETree.SubElement(variable, 'logical_max')
#                     logical_max.text = str(row['constraints.maximum'])
#                 if row.get('constraints.minimum'):
#                     logical_min = ETree.SubElement(variable, 'logical_min')
#                     logical_min.text = str(row['constraints.minimum'])

#                 # Maximum length ('constraints.maxLength') is not supported in dbGaP XML, so we ignore it.

#                 # We ignore 'constraints.pattern' and 'format' for now, but we can try to include them in the
#                 # description later if that is useful.
#                 if row.get('constraints.pattern'):
#                     counters['constraints.pattern'] += 1
#                     logging.warning(f"`constraints.pattern` of {row['constraints.pattern']} found in row {row_index}, skipped.")
#                 if row.get('format'):
#                     counters['format'] += 1
#                     logging.warning(f"Found `format` of {row['format']} found in row {row_index}, skipped.")

#                 # Process enumerated and encoded values.
#                 encs = {}
#                 if row.get('encodings'):
#                     counters['encodings'] += 1

#                     for encoding in re.split("\\s*\\|\\s*", row['encodings']):
#                         m = re.fullmatch("^\\s*(.*?)\\s*=\\s*(.*)\\s*$", encoding)
#                         if not m:
#                             raise RuntimeError(
#                                 f"Could not parse encodings {row['encodings']} on row {row_index}")
#                         key = m.group(1)
#                         value = m.group(2)

#                         if key in encs:
#                             raise RuntimeError(
#                                 f"Duplicate key detected in encodings {row['encodings']} on row {row_index}")
#                         encs[key] = value

#                 for key, value in encs.items():
#                     value_element = ETree.SubElement(variable, 'value')
#                     value_element.set('code', key)
#                     value_element.text = value

#                 # Double-check encodings with constraints.enum
#                 if row.get('constraints.enum'):
#                     enums = re.split("\\s*\\|\\s*", row['constraints.enum'])
#                     if set(enums) != set(encs.keys()):
#                         logging.error(f"`constraints.enum` ({row['constraints.enum']}) and `encodings` ({row['encodings']}) do not match.")
#                         counters['enum_encoding_mismatch'] += 1

#                 # Variable type.
#                 typ = row.get('type')
#                 if encs:
#                     typ = 'encoded value'
#                 if typ:
#                     type_element = ETree.SubElement(variable, 'type')
#                     type_element.text = typ

#                 # We currently ignore metadata fields not usually filled in for input VLMD files:
#                 # ordered, missingValues, trueValues, falseValues, repo_link

#                 # We currently ignore all standardMappings: standardsMappings.type, standardsMappings.label,
#                 # standardsMappings.url, standardsMappings.source, standardsMappings.id
#                 # We currently ignore all relatedConcepts: relatedConcepts.type, relatedConcepts.label,
#                 # relatedConcepts.url, relatedConcepts.source, relatedConcepts.id

#                 # We currently ignore all univarStats vars: univarStats.median, univarStats.mean, univarStats.std,
#                 # univarStats.min, univarStats.max, univarStats.mode, univarStats.count,
#                 # univarStats.twentyFifthPercentile, univarStats.seventyFifthPercentile,
#                 # univarStats.categoricalMarginals.name, univarStats.categoricalMarginals.count
#         else:
#             # This shouldn't be needed, since Click should catch any file format not in the accepted list.
#             raise RuntimeError(f"Unsupported file format {file_format}")

#         # Write out dbGaP XML.
#         xml_str = ETree.tostring(data_table, encoding='unicode')
#         pretty_xml_str = minidom.parseString(xml_str).toprettyxml()
#         print(pretty_xml_str, file=output)

#         # Display counters.
#         logging.info(f"Counters: {json.dumps(counters, sort_keys=True, indent=2)}")


# Run get_bdc_studies_from_gen3() if not used as a library.
if __name__ == "__main__":
    get_bdc_studies_from_gen3()
