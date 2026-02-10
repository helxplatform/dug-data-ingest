#
# Script to download all HEAL CDEs from the HEAL Platform Metadata Service (MDS).
#
# USAGE:
#   python bin/get_heal_platform_cdes.py [output directory]
#

import json
import os
import re
from pathlib import PurePosixPath
from urllib.parse import urlparse

import click
import logging
import requests

# Some defaults.
DEFAULT_MDS_ENDPOINT = 'https://healdata.org/mds/metadata'
MDS_DEFAULT_LIMIT = 10000

# Turn on logging
logging.basicConfig(level=logging.INFO)

def map_mds_type_to_dug(data_type: str) -> str:
    """
    Map a Platform MDS data type to a Dug Data Model v2 data type.

    :param data_type: A Platform MDS data type (e.g. "integer").
    :return: The corresponding Dug Data Model v2 data type (e.g. "integer").
    :raises ValueError: If the data type is not recognised.
    """

    # Is it None? If so, warn the user.
    if data_type is None:
        logging.warning(f"map_mds_type_to_dug called with None as a data type. Mapping to 'string'.")
        return "string"

    accepted_types = {
        'string',
        'integer',
        'number',
        'date',
        'time',
        'datetime',
    }
    if data_type not in accepted_types:
        raise ValueError(f"Data type '{data_type}' not recognised. Using one of the following instead: {accepted_types}.")

    return data_type


def download_from_mds(output_dir, mds_metadata_endpoint = DEFAULT_MDS_ENDPOINT, mds_limit = MDS_DEFAULT_LIMIT):
    """
    Download all the studies and data dictionaries from the Platform MDS.

    :param output_dir: The directory into which to write the studies.
    :param mds_metadata_endpoint: The Platform MDS endpoint to use.
    :param mds_limit: The maximum number of studies to download from the MDS. TODO: add support for queries beyond the limit.
    :return: A dictionary of all the studies, with the CDE ID as keys.
    """

    response = requests.get(mds_metadata_endpoint, params={
        'data': 'true',                 # Include the full JSON object in the response.
        '_guid_type': 'cde_metadata',   # Download only CDEs.
        'limit': mds_limit,             # Use the download limit.
    })
    response.raise_for_status()

    heal_platform_cdes = response.json()
    if len(heal_platform_cdes) == mds_limit:
        raise RuntimeError(f"Download limit of {mds_limit} hit. Please increase the limit.")
    elif len(heal_platform_cdes) > (mds_limit * 0.90):
        logging.warning(f"Downloaded {len(heal_platform_cdes)} CDEs from the Platform MDS, which is close to the limit. "
                        "Please update the script so that it is not limited by the limit.")

    dug_crfs = {}
    for crf_id, crf_metadata in heal_platform_cdes.items():
        cde_metadata = crf_metadata['cde_metadata']
        standardsMappings = cde_metadata['standardsMappings']

        dug_variables = []
        for field in cde_metadata['fields']:
            # Should correspond to a DugVariable
            dug_variable = {
                'type': 'variable',
                'is_cde': True,
                'id': field['name'],
                'name': field.get('title', None),        # TODO: need to get Platform to fix this
                'description': field['description'],
                'data_type': map_mds_type_to_dug(field.get('type', None)),
                'parents': [
                    crf_id,
                ],
                'metadata': {
                    # TODO: add standards mappings once I figure out how to read it.
                }
            }

            # Check for optional variables.
            if 'drupal_id' in crf_metadata:
                dug_variable['metadata']['drupal_id'] = crf_metadata['drupal_id']

            # Handle enumValues and enumLabels.
            enumValues = field.get('constraints', {'enum': []}).get('enum', [])
            enumLabels = field.get('enumLabels', {})
            if not isinstance(enumLabels, dict):
                logging.error(f"Field {field['name']} in CDE {crf_id} has a string for enumLabels \"{enumLabels})\", skipping enumLabels.")
                enumLabels = {}

            if not enumLabels and not enumValues:
                # No need to do anything.
                pass
            elif enumLabels and not enumValues:
                logging.error(f"Field {field['name']} in CDE {crf_id} has inconsistent enumLabels ({enumLabels}) and enumValues ({enumValues}).")
            else:
                if enumLabels and (len(enumLabels.keys()) != len(enumValues)):
                    logging.info(f"Field {field['name']} in CDE {crf_id} has inconsistent numbers of "
                                    f"enumValues ({len(enumValues)}: {enumValues}) and "
                                    f"enumLabels ({len(enumLabels)}: {enumLabels}).")
                else:
                    dug_variable['metadata']['enum'] = [value.strip() for value in enumValues]
                    dug_variable['metadata']['permissible_values'] = {key.strip(): value.strip() for key, value in enumLabels.items()}

            dug_variables.append(dug_variable)

        # Figure out all the URLs. There is (usually?) only one.
        crf_mappings = cde_metadata['standardsMappings']
        urls = []
        for crf_mapping in crf_mappings:
            if 'instrument' in crf_mapping and 'url' in crf_mapping['instrument']:
                url = crf_mapping['instrument']['url']

                if re.match(r'^https://heal.nih.gov/files/CDEs/20\d{2}-\d{2}/', url):
                    url = 'https://www.nih.gov/sites/default/files/CDEs/2026-01/' + url[40:]

                urls.append(url)

        if len(urls) > 1:
            logging.warning(f"CDE {crf_id} has multiple URLs: {urls}")

        # Set up the Dug CRF.
        if crf_id in dug_crfs:
            raise RuntimeError(f"Duplicate CDE ID: {crf_id} (previous: {dug_crfs[crf_id]}, new: {cde_metadata})")

        dug_crf = {
            'type': 'section',
            'is_crf': True,
            'id': crf_id,
            'parents': [], # TODO: add HDP IDs here.
            'crf_id': crf_id,
            'name': crf_metadata['file_name'],
            'description': '', # TODO: can we get this back somehow.
            'variable_list': [var['id'] for var in dug_variables],
        }
        if len(urls) > 0:
            dug_crf['action'] = urls[0]
            dug_crf['urls'] = []
            for url in urls:
                urlpath = PurePosixPath(urlparse(url).path)
                filename = urlpath.name
                if filename == '':
                    filename = url

                mime_type = 'application/octet-stream'
                description = ''
                drupal_id = ''
                lang = 'en'
                lang_full = 'English'
                match urlpath.suffix.lower():
                    case '.xlsx':
                        mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                        description = f"This is the Excel file that describes the {len(dug_variables)} CDEs present in this CRF."
                        drupal_id = crf_metadata.get('drupal_id', None)
                    case '.docx':
                        mime_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                        description = f"This is the {lang_full} download of the Microsoft Word document containing the questionnaire."
                    case '.pdf':
                        mime_type = 'application/pdf'
                        description = f"This is the {lang_full} download of the PDF document containing the questionnaire."

                dug_crf['urls'].append({
                    'url': url,
                    'filename': filename,
                    'lang': lang,
                    'mime-type': mime_type,
                    'description': description or None,
                    'drupal_id': drupal_id or None,
                })

        # Each CRF entry should be the variables followed by the CRF.
        # The other way around makes more sense, but this will make comparisons easier.
        dug_entries = dug_variables + [dug_crf]
        dug_crfs[crf_id] = dug_entries

        # Write the JSON file.
        crf_id_as_filename = re.sub(r'\W', '_', crf_id)
        output_path = os.path.join(output_dir, f"{crf_id_as_filename}.json")
        with open(output_path, 'w') as f:
            json.dump(dug_entries, f, indent=2)

    return dug_crfs


# Set up command line arguments.
@click.command()
@click.argument('output', type=click.Path(exists=False), required=True)
@click.option(
    '--mds-metadata-endpoint', '--mds', default=DEFAULT_MDS_ENDPOINT,
    help='The MDS metadata endpoint to use, e.g. https://healdata.org/mds/metadata')
@click.option(
    '--limit', default=MDS_DEFAULT_LIMIT,
    help='The maximum number of entries to retrieve from the Platform '
    'MDS. Note that some MDS instances have their own built-in '
    'limit; if you hit that limit, you will need to update the '
    'code to support offsets.')
def get_heal_platform_cdes(output, mds_metadata_endpoint, limit):
    """
    Retrieves files from the HEAL Platform Metadata Service (MDS) in the Dug Data Model v2 JSON format.

    \f

    The current iteration of this method is the simplest: it simply downloads the CDEs from the Platform MDS and writes
    them in the Dug Data Model v2 JSON format along with any study/CRF mappings included in it.
    In the (near) future, I will need to add support for the two other types of mappings we support:
    1. Study/CRF mappings from the HEAL CDE team questionnaire.
       - We will probably want to pull this from LakeFS, and then I can set up another job to update that regularly,
         or just keep updating them manually for now.
       - This will hopefully go away once all those entries are in the Platform.
    2. Variable/CDE mappings from Liezl's script.
       - We can probably use a GitHub PAT to clone this repo and extract the mappings from there.
       - Study/HDP mappings and CDE/HDPCDE mappings will need to move into that repository as well.

    :param output: The output directory, which should not exist when the script is run.
    :param mds_metadata_endpoint: The MDS metadata endpoint to use, e.g. https://healdata.org/mds/metadata
    :param limit: The maximum number of entries to retrieve from the Platform MDS. Note that some MDS instances have their own built-in limit; if you hit that limit, you will need to update the code to support offsets.
    """

    # Don't allow the program to run if the output directory already exists.
    if os.path.exists(output):
        logging.error(
            f"To ensure that existing data is not partially overwritten, "
            f"the specified output directory ({output}) must not exist.")
        exit(1)

    # TODO: load additional mappings.

    # Create the output directory.
    os.makedirs(output, exist_ok=True)
    cdes = download_from_mds(output, mds_metadata_endpoint, limit)

    logging.info(f"Generated {len(cdes)} CDEs.")


# Run get_heal_platform_mds_data_dicts() if not used as a library.
if __name__ == "__main__":
    get_heal_platform_cdes()
