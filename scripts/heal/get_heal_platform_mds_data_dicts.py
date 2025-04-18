#
# Script to download all data dictionaries from the HEAL Platform Metadata Service (MDS).
#
# USAGE:
#   python bin/get_heal_platform_mds_data_dicts.py
#
# If no MDS endpoint is specified, we default to the production endpoint at https://healdata.org/mds/metadata
#
import csv
import json
import os
import re
import glob
import click
import logging
import requests
from collections import defaultdict
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom

# Some defaults.
DEFAULT_MDS_ENDPOINT = 'https://healdata.org/mds/metadata'
PUBLIC_MDS_ENDPOINT = 'https://healdata.org/portal/discovery'
MDS_DEFAULT_LIMIT = 10000
DATA_DICT_GUID_TYPE = 'data_dictionary'
HEAL_STUDY_GUID_TYPES = [
    'discovery_metadata',                   # Fully registered studies.
    'unregistered_discovery_metadata'       # Studies added to the Platform MDS but without the investigator registering the study.
]
HDP_ID_PREFIX = 'HEALDATAPLATFORM:'

# Turn on logging
logging.basicConfig(level=logging.INFO)


def translate_data_dictionary_field(field):
    """
    Translate a data dictionary field into the internal format needed by generate_dbgap_files().

    :param field: A dictionary representing a single field from the Platform MDS.
    :return: A dictionary representing a single field from the Platform MDS in a standard format.
    :raise ValueError: if we can't figure out the information in the input field.
    """

    result = field.copy()

    # The variable name could be called 'name' or 'property' (for older data dictionaries).
    if 'name' in field:
        result['name'] = field['name']
    elif 'property' in field:
        result['name'] = field['property']
    else:
        raise ValueError(f"Unable to translate field {field}: missing name or property")

    # The section name could be called 'section', 'module' or 'node'.
    if 'section' in field:
        result['section'] = field['section']
    elif 'module' in field:
        result['section'] = field['module']
    elif 'node' in field:
        result['section'] = field['node']

    return result


def download_from_mds(studies_dir, data_dicts_dir, studies_with_data_dicts_dir, mds_metadata_endpoint, mds_limit):
    """
    Download all the studies and data dictionaries from the Platform MDS.
    (At the moment, we assume everything that isn't a data dictionary is a
    study).

    :param studies_dir: The directory into which to write the studies.
    :param data_dicts_dir: The directory into which to write the data dictionaries.
    :param studies_with_data_dicts_dir: The directory into which to write the studies with data dictionaries.
    :param mds_metadata_endpoint: The Platform MDS endpoint to use.
    :param mds_limit: The maximum number of studies to download from the MDS. TODO: add support for queries beyond the limit.
    :return: A dictionary of all the studies, with the study ID as keys.
    """

    # Download data dictionary identifiers. We filter using the DATA_DICT_GUID_TYPE provided earlier.
    # This allows us to download (and complain about) the data dictionaries that are not part of studies.
    #
    # TODO: extend this so it can function even if there are more than mds_limit data dictionaries.
    result = requests.get(mds_metadata_endpoint, params={
        '_guid_type': DATA_DICT_GUID_TYPE,
        'limit': mds_limit,
    })
    if not result.ok:
        raise RuntimeError(f'Could not retrieve data dictionary list: {result}')
    datadict_ids = result.json()
    logging.info(f"Downloaded {len(datadict_ids)} data dictionaries.")

    # Download "studies" (everything that isn't a data dictionary). To do this, we download every metadata ID
    # (which we store in metadata_ids) and filter out the data dictionary identifiers we've seen before.
    #
    # TODO: extend this so it can function even if there are more than mds_limit data dictionaries.
    metadata_ids = []
    for heal_study_guid_type in HEAL_STUDY_GUID_TYPES:
        result = requests.get(mds_metadata_endpoint, params={
            '_guid_type': heal_study_guid_type,
            'limit': mds_limit,
        })
        if not result.ok:
            raise RuntimeError(f'Could not retrieve metadata list for guid_type {heal_study_guid_type}: {result}')
        metadata_ids.extend(result.json())
    study_ids = list(set(metadata_ids) - set(datadict_ids))

    # Download all the studies. This allows us to identify which study each data dictionary is connected to, and
    # allows us to complain about stray data dictionaries that are not connected to any study.
    #
    studies = {}
    studies_to_dds = defaultdict(list)
    for count, study_id in enumerate(study_ids):
        logging.debug(f"Downloading study {study_id} ({count + 1}/{len(study_ids)})")

        result = requests.get(mds_metadata_endpoint + '/' + study_id)
        if not result.ok:
            raise RuntimeError(f'Could not retrieve study ID {study_id}: {result}')

        result_json = result.json()

        # Record all the studies in case we need to look them up later.
        if study_id in studies:
            raise RuntimeError(f'Duplicate study ID: {study_id}')
        studies[study_id] = result_json

        # Record studies that have data dictionaries.
        if "variable_level_metadata" in result_json and "data_dictionaries" in result_json["variable_level_metadata"]:
            dicts = result_json['variable_level_metadata']['data_dictionaries'].items()
            for (key, dd_id) in dicts:
                logging.info(f"Found data dictionary {key} in study {study_id}: {dd_id}")
                studies_to_dds[study_id].append({
                    'id': dd_id,
                    'label': key
                })

        # For debugging (and later Dug ingest), write the study-level metadata into the studies directory.
        with open(os.path.join(studies_dir, study_id + '.json'), 'w') as f:
            json.dump(result_json, f)

    logging.info(f"Downloaded {len(studies)} studies, of which {len(studies_to_dds)} studies have data dictionaries.")

    # For studies containing data dictionaries, write them into data_dicts_dir, but after adding a
    # `data_dictionaries` key that has a list of the data dictionaries associated with it, which we
    # download separately from the MDS.
    data_dict_ids_within_studies = set()
    for count, study_id in enumerate(studies_to_dds.keys()):
        study_json = studies[study_id]
        study_json['data_dictionaries'] = []

        for dd in studies_to_dds[study_id]:
            dd_id = dd['id']
            dd_label = dd['label']

            logging.info(f"Adding data dictionary to study {study_id} ({count + 1}/{len(studies_to_dds)}): {dd_id} ({dd_label})")

            result = requests.get(mds_metadata_endpoint + '/' + dd_id)
            if result.status_code == 404:
                logging.warning(
                    f"Study {study_id} refers to data dictionary {dd_id}, but no such data dictionary was found in "
                    f"the MDS.")
                result_json = {
                    '@id': dd_id,
                    'error': result.json(),
                    'fields': [],
                }
            elif not result.ok:
                raise RuntimeError(f'Could not retrieve data dictionary {dd_id}: {result}')
            else:
                data_dict_ids_within_studies.add(dd_id)
                result_json = result.json()

                result_json['@id'] = dd_id
                result_json['label'] = dd_label

                # Sometimes 'data_dictionary' is a list of fields, and sometimes it is a dictionary with a 'fields' field.
                # We standardize so that the top-level 'fields' field is always a list of fields.
                try:
                    if "data_dictionary" in result_json and isinstance(result_json["data_dictionary"], list):
                        result_json["fields"] = result_json["data_dictionary"]
                    elif (
                        "data_dictionary" in result_json
                        and isinstance(result_json["data_dictionary"], dict)
                        and "fields" in result_json["data_dictionary"]
                    ):
                        result_json["fields"] = list(
                            map(
                                translate_data_dictionary_field,
                                result_json["data_dictionary"]["fields"],
                            )
                        )
                    elif (
                        "data_dictionary" in result_json
                        and isinstance(result_json["data_dictionary"], dict)
                        and "data_dictionary" in result_json["data_dictionary"]
                    ):
                        result_json["fields"] = list(
                            map(
                                translate_data_dictionary_field,
                                result_json["data_dictionary"]["data_dictionary"],
                            )
                        )
                        if (not dd_label or dd_label == "NA") and "title" in result_json[
                            "data_dictionary"
                        ]:
                            result_json["label"] = result_json["data_dictionary"]["title"]
                    else:
                        logging.error(
                            f"Could not determine fields for data dictionary {dd_id}, skipping: {result_json}"
                        )
                        result_json["fields"] = []
                except ValueError as ve:
                    logging.error(
                        f"Could not determine fields for data dictionary {dd_id}, skipping: {ve}"
                    )
                    result_json["fields"] = []

            study_json['data_dictionaries'].append(result_json)

        # Write out the data dictionaries.
        with open(os.path.join(studies_with_data_dicts_dir, study_id + '.json'), 'w') as f:
            json.dump(study_json, f)

        logging.debug(
            f"Wrote {len(study_json['data_dictionaries'])} dictionaries to {studies_with_data_dicts_dir}/{study_id}.json")

    # We shouldn't need to do this, but at the moment we have multiple data dictionaries (in pre-prod, not prod) that aren't linked to from
    # within studies. So let's download them separately!
    data_dict_ids_not_within_studies = list(set(datadict_ids) - data_dict_ids_within_studies)
    for count, dd_id in enumerate(data_dict_ids_not_within_studies):
        dd_id_json_path = os.path.join(data_dicts_dir, dd_id.replace('/', '_') + '.json')

        logging.debug(
            f"Downloading data dictionary not linked to a study {dd_id} ({count + 1}/{len(data_dict_ids_not_within_studies)})")

        result = requests.get(mds_metadata_endpoint + '/' + dd_id)
        if not result.ok:
            raise RuntimeError(f'Could not retrieve data dictionary {dd_id}: {result}')

        data_dict_json = result.json()
        data_dict_json['@id'] = dd_id
        with open(dd_id_json_path, 'w') as f:
            json.dump(data_dict_json, f)

        logging.debug(f"Wrote data dictionary to {dd_id_json_path}.json")

    if len(data_dict_ids_not_within_studies) > 0:
        logging.warning(f"Some data dictionaries ({len(data_dict_ids_not_within_studies)} are present in the Platform "
                        f"MDS, but aren't associated with studies: {data_dict_ids_not_within_studies}")

    # Return the list of studies and the studies with data dictionaries.
    return study_ids, data_dict_ids_within_studies


def generate_dbgap_files(dbgap_dir, studies_with_data_dicts_dir, subdirectory_for_hdpid = None, research_network_name_for_hdpid = None):
    """
    Generate dbGaP files from data dictionaries containing

    :param dbgap_dir: The dbGaP directory into which we write the dbGaP files.
    :param studies_with_data_dicts_dir: The directory that contains studies containing data dictionaries.
        (This should work for the data_dicts directory too, but then we have no way of linking them to studies.)
    :param subdirectory_for_hdpid: A function that -- given an HDP ID -- returns the subdirectory name to put it into.
    :param research_network_name_for_hdpid: A function that -- given an HDP ID -- returns the research network name for that HDPID.
    :return: The list of dbGaP files generated.
    """

    dbgap_files_generated = set()

    # Create a complete variable index for every variable we find.
    all_variable_index = []

    data_dict_files = os.listdir(studies_with_data_dicts_dir)
    for data_dict_file in data_dict_files:
        file_path = os.path.join(studies_with_data_dicts_dir, data_dict_file)

        # We're only interested in files.
        if not os.path.isfile(file_path):
            continue

        # We're only interested in JSON files.
        if not file_path.lower().endswith('.json'):
            continue

        # Read the JSON file.
        logging.info(f"Loading study containing data dictionaries: {file_path}")
        with open(file_path, 'r') as f:
            json_data = json.load(f)

        # Check if this contains data dictionaries or if it _is_ a data dictionary.
        # (This is not currently used, but the idea is that you could call this function on
        # the data_dict directory instead of the studies_with_data_dicts directory and generate
        # dbGaP XML files for all of them instead.
        if 'data_dictionaries' in json_data:
            data_dicts = json_data['data_dictionaries']
            study = json_data
        elif 'data_dictionary' in json_data:
            data_dicts = [json_data['data_dictionary']]
            study = {}
        else:
            raise RuntimeError(f"Could not read {file_path}: unknown format.")

        # Prepare data table to write out.
        data_table = ET.Element('data_table')

        if 'gen3_discovery' not in study:
            logging.error(f"No gen3_discovery field found in data dictionary file {file_path}, skipping.")
            continue

        # Every data dictionary from the HEAL Data Platform should have an ID, and the previous code should have
        # stored it in the `@id` field in the data dictionary JSON file.
        #
        # There may also be a `label`, which is the key of the data dictionary in the study.
        if '@id' in study['gen3_discovery']:
            data_table.set('id', study['gen3_discovery']['@id'])
            study_id = study['gen3_discovery']['@id']
        else:
            logging.warning(f"No identifier found in data dictionary file {file_path}")
        study_name =  study.get('gen3_discovery', {}).get('label') or study.get('gen3_discovery', {}).get('study_metadata',{}).get('minimal_info',{}).get('study_name')
        if study_name:
            data_table.set('study_name', study_name)
        study_description = study.get('gen3_discovery', {}).get('study_metadata',{}).get('minimal_info',{}).get('study_description')
        if study_description:
            data_table.set('study_description', study_description)

        # Determine the data_table study_id from the internal HEAL Data Platform (HDP) identifier.
        study_id = None
        if '_hdp_uid' in study['gen3_discovery']:
            study_id = HDP_ID_PREFIX + study['gen3_discovery']['_hdp_uid']
            data_table.set('study_id', study_id)
        else:
            logging.warning(f"No HDP ID found in data dictionary file {file_path}")

        # If there is a research program in gen3_discovery, include it here.
        if 'research_program' in study['gen3_discovery'] and study['gen3_discovery']['research_program'] != "":
            research_program = study['gen3_discovery']['research_program']
            data_table.set('research_program', research_program)

        # Look up and store the research_network_name if we have one.
        if research_network_name_for_hdpid:
            research_network_name = research_network_name_for_hdpid(study_id)
            if research_network_name:
                data_table.set('research_network_name', research_network_name)

        # Create a non-standard appl_id field just in case we need it later.
        # This should be fine for now, but there is also a `comments` element that we can
        # store information like this in if we need to.
        if 'appl_id' in study['gen3_discovery']:
            data_table.set('appl_id', study['gen3_discovery']['appl_id'])
        else:
            logging.warning(f"No APPL ID found in data dictionary file {file_path}")

        # Determine the data_table date_created
        if 'date_added' in study['gen3_discovery']:
            data_table.set('date_created', study['gen3_discovery']['date_added'])
        else:
            logging.warning(f"No date_added found in data dictionary file {file_path}")

        # A list of unique variable identifiers in this data dictionary file.
        # If you need to make sure every variable from MDS is uniquely identified, you can move this set to the
        # top-level of this file.
        unique_variable_ids = set()

        total_variable_count = 0
        count_data_dictionaries = 0
        for data_dict in data_dicts:
            count_data_dictionaries += 1
            variable_count = 0

            # Check for data_dict['error']
            if 'error' in data_dict:
                logging.warning(f"Could not retrieve data dictionary {data_dict['@id']} from MDS: {data_dict['error']['detail']}")
                continue

            for var_dict in data_dict.get('fields', []):
                total_variable_count += 1
                variable_count += 1

                variable_entry = {}

                logging.debug(f"Generating dbGaP for variable {var_dict} in {file_path}")

                # Retrieve the variable name.
                variable = ET.SubElement(data_table, 'variable')

                # Let's create a dd_id field for each variable, even if nobody supports it yet.
                variable.set('dd_id', data_dict['@id'])
                variable_entry['study_id'] = data_table.get('study_id')
                variable_entry['dd_id'] = data_dict['@id']

                # Make sure the variable ID is unique (by adding `_1`, `_2`, ... to the end of it).
                name_or_node = var_dict.get('name', var_dict.get('property', ''))
                var_name = name_or_node
                variable_index = 0
                while var_name in unique_variable_ids:
                    variable_index += 1
                    var_name = name_or_node + '_' + variable_index
                variable.set('id', var_name)
                if var_name != name_or_node:
                    logging.warning(f"Duplicate variable ID detected for {name_or_node}, so replaced it with "
                                    f"{var_name} -- note that the name element is unchanged.")

                # Create a name element for the variable. We don't uniquify this field.
                name = ET.SubElement(variable, 'name')
                name.text = name_or_node

                variable_entry['name'] = name_or_node

                # Create a title element for the variable.
                if 'title' in var_dict:
                    title = ET.SubElement(variable, 'title')
                    title.text = var_dict['title']
                    variable_entry['title'] = var_dict['title']

                if 'description' in var_dict:
                    desc = ET.SubElement(variable, 'description')
                    desc.text = var_dict['description']
                    variable_entry['description'] = var_dict['description']

                # Export the `module` field so that we can look for instruments.
                # TODO: this is a custom field. Instead of this, we could export each data dictionary as a separate dbGaP
                # file. Need to check to see what works better for Dug ingest.
                if 'section' in var_dict:
                    variable.set('section', var_dict['section'])
                    variable_entry['section'] = var_dict['section']

                # Add constraints.
                logging.debug(f"Looking for constraints in {data_dict['@id']} for {data_table.get('study_id')}: {json.dumps(var_dict, indent=2, sort_keys=True)}")
                if 'constraints' in var_dict:
                    var_dict_constraints = var_dict['constraints']

                    # Check for minimum and maximum constraints.
                    if 'minimum' in var_dict_constraints:
                        logical_min = ET.SubElement(variable, 'logical_min')
                        logical_min.text = str(var_dict_constraints['minimum'])
                        variable_entry['logical_min'] = str(var_dict_constraints['minimum'])
                    if 'maximum' in var_dict_constraints:
                        logical_max = ET.SubElement(variable, 'logical_max')
                        logical_max.text = str(var_dict_constraints['maximum'])
                        variable_entry['logical_max'] = str(var_dict_constraints['maximum'])

                    # Determine a type for this variable.
                    typ = var_dict.get('type')
                    if 'enum' in var_dict_constraints and len(var_dict_constraints['enum']) > 0:
                        typ = 'encoded value'
                        enum_values = var_dict_constraints['enum']
                        enum_labels = var_dict.get('enumLabels', {})
                        encodings = []

                        # In some older data dictionaries, the enumLabels are stored in the `encodings` string.
                        if 'encodings' in var_dict_constraints and len(enum_labels) == 0:
                            for pair in var_dict_constraints['encodings'].split('|'):
                                key, value = pair.split('=')
                                enum_labels[key.strip()] = value.strip()

                        for key in enum_values:
                            value_element = ET.SubElement(variable, 'value')
                            value_element.set('code', key)
                            try:
                                value = enum_labels[key]
                            except KeyError:
                                logging.warning(f"No enumLabel found for code '{key}' in enumLabels {enum_labels}, using '{key}' as value.")
                                value = key

                            value_element.text = value
                            encodings.append(f"{key}={value}")

                        variable_entry['encodings'] = "|".join(encodings)

                    if typ:
                        type_element = ET.SubElement(variable, 'type')
                        type_element.text = typ
                        variable_entry['type'] = typ

                all_variable_index.append(variable_entry)

            logging.info(f"Added {variable_count} variables in data dictionary {data_dict['@id']} in {file_path} for study {study_name}.")

        # Write out XML.
        xml_str = ET.tostring(data_table, encoding='unicode')
        pretty_xml_str = minidom.parseString(xml_str).toprettyxml()

        # Calculate the subdirectory for this study.
        subdir = None
        if subdirectory_for_hdpid:
            subdir = subdirectory_for_hdpid(study_id)

        if subdir:
            os.makedirs(os.path.join(dbgap_dir, subdir), exist_ok=True)
            output_xml_filename = os.path.join(dbgap_dir, subdir, data_dict_file.replace('.json', '.xml'))
        else:
            output_xml_filename = os.path.join(dbgap_dir, data_dict_file.replace('.json', '.xml'))

        # Produce the XML file by changing the .json to .xml.
        with open(output_xml_filename, 'w') as f:
            f.write(pretty_xml_str)
        logging.info(f"Wrote {data_table} (containing {total_variable_count} variables from {count_data_dictionaries} data dictionaries) to {output_xml_filename}")

        # Make a list of dbGaP files to report to the main program.
        dbgap_files_generated.add(output_xml_filename)

    # Write a full variable index to the output XML filename directory.
    variable_index_filename = os.path.join(dbgap_dir, 'variable_index.csv')
    with open(variable_index_filename, 'w') as f:
        header = ['study_id', 'dd_id', 'name', 'section', 'title', 'description', 'type', 'encodings', 'logical_min', 'logical_max']

        csv_writer = csv.DictWriter(f, fieldnames=header)
        csv_writer.writeheader()
        for row in all_variable_index:
            csv_writer.writerow(row)

    logging.info(f"Wrote variable index of {len(all_variable_index)} variables to {variable_index_filename}.")

    return dbgap_files_generated

def make_study_kgx_node(gen3_discovery, nih_reporter):
    """Generate a kgx-style node dict from the Gen3 study info JSON"""
    # Pull these two sub-dicts out for easier reference
    study_metadata = gen3_discovery.get('study_metadata', {})
    minimal_info = gen3_discovery.get('minimal_info', {})
    if not minimal_info:
        # sometimes this shows up in different places
        minimal_info = study_metadata.get('minimal_info', {})
    metadata_location = gen3_discovery.get('metadata_location', {})
    study_id = gen3_discovery['_hdp_uid']
    node = {
        "id": HDP_ID_PREFIX + study_id,
        "name": gen3_discovery.get('project_title', ""),
        "categories": [
            "biolink:Study"
        ],
        "description" : minimal_info.get('study_description', ""),
        "iri": PUBLIC_MDS_ENDPOINT + "/" + study_id,
        "abstract": gen3_discovery.get("study_description_summary", ""),
        "archive_date": gen3_discovery.get("archive_date", ""),
        "project_start_date": nih_reporter.get('project_start_date', ""),
        "project_end_date": nih_reporter.get('project_end_date', ""),
    }
    return node

def make_kgx(nodes, edges):
    """Very basic kgx format assembler"""
    kgx = {
        'nodes': nodes,
        'edges': edges,
    }

    return kgx

def generate_kgx_from_studies_files(studies_dir, kgx_file):
    """Read SLMD files from the studies_dir, write out kgx_file

    Study level metadata kgx file generation uses the cached files in the
    studies_dir from the previous step in order to generate the kgx output
    file.
    """
    nodes = []
    edges = []
    study_files = glob.glob(os.path.join(studies_dir, '*.json'))
    logging.info("Found %d study files in %s", len(study_files), studies_dir)
    for study_file in glob.glob(f'{studies_dir}/*.json'):
        logging.info("Creating kgx node from study file %s", study_file)
        with open(study_file, 'rt') as sf:
            study = json.load(sf)
        gen3_discovery = study.get('gen3_discovery', None)
        nih_reporter = study.get('nih_reporter', None)

        # We can't really produce study data without gen3_discovery.
        if not gen3_discovery:
            logging.warning(f"No gen3_discovery found in study file {study_file}, skipping.")
            continue

        # We can produce study data without nih_reporter -- we'll only miss the study start/end dates -- so continue.
        if not nih_reporter:
            logging.warning(f"No nih_reporter found in study file {study_file}, continuing.")
            nih_reporter = {}

        nodes.append(make_study_kgx_node(gen3_discovery, nih_reporter))

    logging.info("Writing out %d kgx nodes", len(nodes))
    json.dump(make_kgx(nodes, edges), kgx_file, indent=2)


# Set up command line arguments.
@click.command()
@click.argument('output', type=click.Path(exists=False), required=True)
@click.option(
    '--mds-metadata-endpoint', '--mds', default=DEFAULT_MDS_ENDPOINT,
    help='The MDS metadata endpoint to use, e.g. https://healdata.org/mds/metadata')
@click.option(
    '--hdp-to-study-type-mappings-csv',
    default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         'data/ResearchNetworksMappedToHDPID_Feb2025.csv'),
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    help='The CSV file that maps HDP study IDs to HEAL study types.')
@click.option(
    '--limit', default=MDS_DEFAULT_LIMIT,
    help='The maximum number of entries to retrieve from the Platform '
    'MDS. Note that some MDS instances have their own built-in '
    'limit; if you hit that limit, you will need to update the '
    'code to support offsets.')
@click.option(
    '--use-cached/--no-use-cached', default=False,
    help='Just use files already on disk, do not download any new'
    'data from platform. Used for testing.')
def get_heal_platform_mds_data_dicts(output, mds_metadata_endpoint,
                                     hdp_to_study_type_mappings_csv, limit,
                                     use_cached):
    """
    Retrieves files from the HEAL Platform Metadata Service (MDS) in a format that Dug can index,
    which at the moment is the dbGaP XML format (as described in https://ftp.ncbi.nlm.nih.gov/dbgap/dtd/).

    Creates the output directory, and then creates three directories in this directory:

      - studies/[study ID (appl)].json: All the studies in the HEAL Platform MDS.

      - datadicts/[data dictionary ID].json: All the data dictionaries in the HEAL Platform MDS.

      - dbGaPs/[data dictionary ID].xml: All the data dictionaries in the HEAL Platform MDS, converted into dbGaP XML format.

    Since other projects also use the Gen3 Metadata Service (MDS), one of our lesser goals here is to
    build code that could be quickly rewritten for other MDS schemas.

    :param output: The output directory, which should not exist when the script is run.
    :param mds_metadata_endpoint: The MDS metadata endpoint to use, e.g. https://healdata.org/mds/metadata
    :param limit: The maximum number of entries to retrieve from the Platform MDS. Note that some MDS instances have their own built-in limit; if you hit that limit, you will need to update the code to support offsets.
    """

    # Don't allow the program to run if the output directory already exists.
    if os.path.exists(output) and not use_cached:
        logging.error(
            f"To ensure that existing data is not partially overwritten, "
            f"the specified output directory ({output}) must not exist.")
        exit(1)

    # Load the HDP to HEAL Study Type CSV file.
    hdp_to_study_type_mappings_csv_filename = click.format_filename(hdp_to_study_type_mappings_csv)
    hdp_to_study_type_mappings = {}
    with open(hdp_to_study_type_mappings_csv_filename, 'r') as mappingsf:
        mappings_reader = csv.DictReader(mappingsf)
        for mapping in mappings_reader:
            hdp_to_study_type_mappings[HDP_ID_PREFIX + mapping['HDPID']] = {
                'research_network': mapping['Research Network Name'],
                'study_type': mapping['HEAL Study Type'],
            }

    # Create the output directory.
    os.makedirs(output, exist_ok=True)

    # Download studies and data dictionaries from the MDS endpoint. We create a lot of directories and temp files to
    # help with debugging -- we can simplify this later on if needed.
    studies_dir = os.path.join(output, 'studies')
    os.makedirs(studies_dir, exist_ok=True)
    data_dicts_dir = os.path.join(output, 'data_dicts')
    os.makedirs(data_dicts_dir, exist_ok=True)
    studies_with_data_dicts_dir = os.path.join(output, 'studies_with_data_dicts')
    os.makedirs(studies_with_data_dicts_dir, exist_ok=True)
    kgx_dir = os.path.join(output, 'studies_kgx')
    os.makedirs(kgx_dir, exist_ok=True)

    if use_cached:
        logging.warning("New data dictionaries will NOT be downloaded, "
                        "as we are running in `--use-cached` mode.")
    else:
        download_from_mds(studies_dir, data_dicts_dir,
                          studies_with_data_dicts_dir, mds_metadata_endpoint,
                          limit)

    # Generate dbGaP entries from the studies and the data dictionaries.
    dbgap_dir = os.path.join(output, 'dbGaPs')
    os.makedirs(dbgap_dir, exist_ok=True)

    dbgap_filenames = generate_dbgap_files(
        dbgap_dir, studies_with_data_dicts_dir,
        lambda hdp_id: hdp_to_study_type_mappings[hdp_id]['study_type'],
        lambda hdp_id: hdp_to_study_type_mappings[hdp_id]['research_network'])
    logging.info(f"Generated {len(dbgap_filenames)} dbGaP files for ingest in {dbgap_dir}.")

    with open(os.path.join(kgx_dir, "heal_studies_kgx.json"), 'w') as kgx_file:
        generate_kgx_from_studies_files(studies_dir, kgx_file)

# Run get_heal_platform_mds_data_dicts() if not used as a library.
if __name__ == "__main__":
    get_heal_platform_mds_data_dicts()
