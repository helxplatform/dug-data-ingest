import logging
import os
import requests
import click
from typing import List
from pathlib import Path
from xml.etree import ElementTree as ET
import csv
import json
from dug import utils as utils
from _base import DugStudy, DugVariable, DugElementParsedList, SECTION_TYPE, VARIABLE_TYPE

logger = logging.getLogger('dug')

DEFAULT_MDS_ENDPOINT = 'https://healdata.org/mds/metadata'
PUBLIC_MDS_ENDPOINT = 'https://healdata.org/portal/discovery'
MDS_DEFAULT_LIMIT = 10000
DATA_DICT_GUID_TYPE = 'data_dictionary'
HEAL_STUDY_GUID_TYPES = [
    'discovery_metadata',                   # Fully registered studies.
    'unregistered_discovery_metadata'       # Studies added to the Platform MDS but without the investigator registering the study.
]

def get_cde_mappings(cde_dir:Path):
    
    all_cde_files = list(cde_dir.glob("*.dug.json"))
    study_cde_mappings = dict()
    variable_cde_mappings = dict()
    for cde_file in all_cde_files:
        with open(cde_file, "r") as f:
            json_obj = json.load(f)
            elements = DugElementParsedList.validate_python(json_obj)
            section_obj = [e for e in elements if e.type==SECTION_TYPE]
            if len(section_obj) !=1:
                print(f"Something wrong with CDE: {cde_file}")
            section = section_obj[0] 
            section_id_splits = section.id.split(":")
            section_id = section_id_splits[0] if len(section_id_splits) == 1 else section.id.split(":")[1]
            study_mappings = section.metadata['study_mappings']
            if len(study_mappings) > 0:
                studies = list(study_mappings.keys())  
                for study in studies:
                    if study in study_cde_mappings:
                        study_cde_mappings[study].append(section_id)
                    else:
                        study_cde_mappings[study] = [section_id]
            cdes = [e for e in elements if e.type==VARIABLE_TYPE]
            for cde in cdes:
                # Find if there are mappings
                # variable_cde_mappings will be a dict of dicts, with HDPID = dict of variable -> cde mapping
                # variables are mapped to a single CDE.
                mappings = cde.metadata['study_variable_mappings'] if 'study_variable_mappings' in cde.metadata else {}
                for hdpid in mappings:
                    if hdpid not in variable_cde_mappings:
                        variable_cde_mappings[hdpid] = dict()
                    variable_mappings = {v:{"measure":cde.id, "cde":section_id}  for v in mappings[hdpid]}
                    variable_cde_mappings[hdpid] = {**variable_cde_mappings[hdpid], **variable_mappings}
    print(variable_cde_mappings)
    return study_cde_mappings, variable_cde_mappings

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

def get_dd_info_from_mds(dd_id:str, dd_label:str,  mds_url:str=None):
    
    result = requests.get(mds_url + '/' + dd_id)
    if result.status_code == 404:
        logging.warning(
            f"Referred data dictionary {dd_id}, but no such data dictionary was found in "
            f"the MDS.")
        result_json = {
            '@id': dd_id,
            'error': result.json(),
            'fields': [],
        }
    elif not result.ok:
        raise RuntimeError(f'Could not retrieve data dictionary {dd_id}: {result}')
    else:
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
        return result_json

def get_study_info_from_mds(study_id:str, mds_url:str=None):
        if not mds_url:
            mds_url = DEFAULT_MDS_ENDPOINT

        result = requests.get(mds_url + '/' + study_id)
        if not result.ok:
            logger.error(f'Could not retrieve study ID {study_id}: {result}')
            return None

        study_json = result.json()

        ## Get study information from whatever sources and create a DugStudy element
        gen3_discovery = study_json.get('gen3_discovery', None)
        nih_reporter = study_json.get('nih_reporter', None)
        vlmd_data = study_json.get('variable_level_metadata', None)

        if gen3_discovery is None and nih_reporter is None:
            return None

        study_metadata = gen3_discovery.get('study_metadata', {})
        minimal_info = gen3_discovery.get('minimal_info', {})
        if not minimal_info:
                # sometimes this shows up in different places
            minimal_info = study_metadata.get('minimal_info', {})
        
        if not nih_reporter:
            print(f"No nih_reporter found in study file {study_id}, continuing.")
            nih_reporter = {}
        
        abstract = minimal_info.get('study_description', "")
        description = minimal_info.get('study_description', "") #gen3_discovery.get("study_description_summary", "")
        abstract = "No Summary Found" if ( abstract is None or (abstract is not None and len(abstract) == 0)) else abstract
        description = "No Summary Found" if (description is None or (description is not None and len(description) == 0)) else description

        pi_list = []
        if gen3_discovery is not None and 'investigators_name' in gen3_discovery and len(gen3_discovery['investigators_name']) > 0:
                pi_list = gen3_discovery['investigators_name']
        elif study_metadata is not None and 'citation' in study_metadata and 'investigators' in study_metadata['citation']:
            pi_list = [ " ".join([k['investigator_first_name'], k["investigator_middle_initial"], k["investigator_last_name"]]) for k in study_metadata['citation']['investigators']]

        publication_list = []
        if study_metadata is not None and ('findings' in study_metadata and 'primary_publications' in study_metadata['findings']):
            publication_list = study_metadata['findings']['primary_publications']
        
        repositories = []
        if study_metadata is not None and ('metadata_location' in study_metadata and 'data_repositories' in study_metadata['metadata_location']):
            repositories = [k['repository_study_link'] for k in study_metadata['metadata_location']['data_repositories'] if 'repository_study_link' in k and len(k['repository_study_link']) > 0]

        """
        gen3_discovery.__manifest field exists AND is not empty
        OR
        gen3_discovery.study_metadata.metadata_location.data_repositories.repository_study_link  exists AND is not empty ]
        AND
        _guid_type is set to discovery_metadata OR unregistered_discovery_metadata
        """
        data_availability = ''
        if ("__manifest" in gen3_discovery and len(gen3_discovery["__manifest"]) > 0) or len(repositories) > 0:
            data_availability = "available"

        vlmd_dds = []
        if vlmd_data is not None and "data_dictionaries" in vlmd_data:
            dicts = vlmd_data['data_dictionaries'].items()
            for (key, dd_id) in dicts:
                logging.info(f"Found data dictionary {key} in study {study_id}: {dd_id}")
                ## Grab vlmd for this dd_id
                try:
                    vlmd_dd = get_dd_info_from_mds(dd_id, key, mds_url)
                except Exception as e:
                    logging.info(f"Trouble getting {dd_id}\nERROR: {e}")
                else:
                    vlmd_dds.append(vlmd_dd)
                    with open(f"/tmp/vlmd_{dd_id}.json", 'w') as f:
                        json.dump(vlmd_dd, f, indent=2)

        study_details = {
            "id": study_id,
            "study_name" : minimal_info.get('study_name', ""),
            "description" : description,
            "action" : gen3_discovery['doi_url'] if (gen3_discovery is not None and "doi_url" in gen3_discovery and len(gen3_discovery['doi_url']) >0) else (PUBLIC_MDS_ENDPOINT + "/" + study_id), ## TODO: There's a DOI link on MDS as well. Use that when available.
            "abstract" : abstract,
            "project_start_date" : nih_reporter.get('project_start_date', ""),
            "project_end_date" : nih_reporter.get('project_end_date', ""),
            "publication_list": publication_list,
            "pi_list": pi_list,
            'institution': gen3_discovery['institutions'] if gen3_discovery is not None and 'institutions' in gen3_discovery else '',
            'data_availability': data_availability,
            'repositories': repositories,
            'vlmd_dds': vlmd_dds,
        }
        return study_details

def guess_data_type(values):
    """
    Guess the data type of a list of values (usually strings from JSON).
    Returns: "integer", "number", "boolean", or "string"
    """
    if not values:
        return "string"
    is_int = True
    is_float = True
    is_bool = True
    for v in values:
        v_str = str(v).strip().lower()
        # Check for boolean
        if v_str not in {"true", "false", "0", "1"}:
            is_bool = False
        # Check for integer
        try:
            int(v_str)
        except ValueError:
            is_int = False
        # Check for float
        try:
            float(v_str)
        except ValueError:
            is_float = False
    if is_bool:
        return "boolean"
    if is_int:
        return "integer"
    if is_float:
        return "number"
    return "string"

def transform_dds_to_dug(vlmd_dds, study_id, research_program=None, research_network=None,vlmd_cde_mappings = {}):
    dug_variables = []
    for vlmd_dd in vlmd_dds:
        for variable in vlmd_dd.get('fields', []):
            # print(variable)
            data_type = variable["type"] if "type" in variable else "string"
            metadata = {}
            if "constraints" in variable:
                ## Assuming that variable['constraints'] is a dict, copy over to DugElement's metadata field.
                ## This should copy fields like `minimum`, `maximum`, `required`, `enum`, `pattern`, `maxLenth`
                metadata = variable["constraints"] ## This is assuming that variable['constraints'] is a dict
                if "enumLabels" in variable or "encodings" in variable:
                    metadata["permissible_values"] = variable["enumLabels"] if "enumLabels" in variable else variable["encodings"]
                    # This probably needs to be refined as the permissible values can also encode a boolean, or integer, etc., but let's think about that later.
                    if "type" not in variable or len(variable["type"]) == 0:
                        data_type = "enum" 
                elif "maximum" in metadata and "minimum" in metadata:
                    data_type = "number"
                elif "enum" in metadata and ("type" not in variable or len(variable["type"]) == 0):
                    data_type = guess_data_type(metadata["enum"])

            elem = DugVariable(id=study_id+':'+variable['name'],
                              name=variable['name'],
                              description=variable['description'],
                              programs=[research_program] if research_program else [],
                              parents=[k for k in (study_id, variable.get('section', '')) if len(k) > 0],
                              data_type=data_type,
                              is_cde=False
                              ) ## This would be changed to study id
            if research_network:
                elem.add_tag("Research Network", research_network)
            if research_program:
                elem.add_tag("Research Program", research_program)
            if study_id in vlmd_cde_mappings and variable['name'] in vlmd_cde_mappings[study_id]:
                metadata["cde_mapping"] = vlmd_cde_mappings[study_id][variable['name']]
            elem.metadata = metadata
            dug_variables.append(elem)
    return dug_variables

# Set up command line arguments.
@click.command()
@click.argument('output', type=click.Path(exists=False), required=True)
@click.option(
    '--mds-metadata-endpoint', '--mds', default=DEFAULT_MDS_ENDPOINT,
    help='The MDS metadata endpoint to use, e.g. https://healdata.org/mds/metadata')
@click.option(
    '--hdp-to-study-type-mappings-csv',
    default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         'data/ResearchNetworksResearchProgramsMappedToHDPID_Feb2026.csv'),
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    help='The CSV file that maps HDP study IDs to HEAL study types.')
@click.option(
    '--cde-location',
    default=None,
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    help='Location to a directory with DUG JSON files of CDEs to get CDE->Study mapping. '
)
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
@click.option(
     '--debug', default=False,
     help='Run in debug mode.'
)
def get_heal_studies(output, mds_metadata_endpoint,
                                     hdp_to_study_type_mappings_csv, 
                                     limit,
                                     cde_location,
                                     use_cached,
                                     debug):
    logging.basicConfig(filename= Path(output)/"log.tx", level = logging.DEBUG if debug else logging.INFO)
    
    if not mds_metadata_endpoint:
        mds_metadata_endpoint = DEFAULT_MDS_ENDPOINT

     # Load the HDP to HEAL Study Type CSV file.
    hdp_to_study_type_mappings_csv_filename = click.format_filename(hdp_to_study_type_mappings_csv)
    hdp_to_study_type_mappings = {}
    with open(hdp_to_study_type_mappings_csv_filename, 'r') as mappingsf:
        mappings_reader = csv.DictReader(mappingsf)
        for mapping in mappings_reader:
            if mapping['study_type'] in ['HDP', 'CTN']:
                hdp_to_study_type_mappings[mapping['key']] = {
                    'research_program': 'Clinical Trials Network' if mapping['study_type'] == 'CTN' else  (None if mapping["Research Program"] == '-' else mapping["Research Program"]),
                    'research_network': None if mapping["Research Network"] == '-' else ('Clinical Trials Network' if mapping["Research Network"] == 'CTN' else mapping["Research Network"]),
                }
    study_cde_mappings, variable_cde_mappings = get_cde_mappings(Path(cde_location))
    metadata_ids = []
    for heal_study_guid_type in HEAL_STUDY_GUID_TYPES:
        result = requests.get(mds_metadata_endpoint, params={
            '_guid_type': heal_study_guid_type,
            'limit': MDS_DEFAULT_LIMIT,
        })
        if not result.ok:
            logger.error(f'Could not retrieve metadata list for guid_type {heal_study_guid_type}: {result}')
            return None
        metadata_ids.extend(result.json())
    study_ids = list(metadata_ids)
    logger.info(f"Getting information for {len(study_ids)} studies from HEAL MDS")
    
    # studies = []
    for count, sid in enumerate(study_ids):
            # if count==20:
            #     break
            study_details = get_study_info_from_mds(study_id = sid, mds_url = mds_metadata_endpoint)
            if study_details is None:
                logger.debug(f"Metadata for Study {sid} is not available, Skipping!")
                continue
            research_program = hdp_to_study_type_mappings[study_details['id']]['research_program'] if study_details['id'] in hdp_to_study_type_mappings else None
            research_network = hdp_to_study_type_mappings[study_details['id']]['research_network'] if study_details['id'] in hdp_to_study_type_mappings else None
            dug_variables = transform_dds_to_dug(study_details['vlmd_dds'], 
                                                 study_details['id'], 
                                                 research_program = research_program, 
                                                 research_network=research_network, 
                                                 vlmd_cde_mappings = variable_cde_mappings)
            metadata = {}
            if study_details['project_start_date'] is not None and len(study_details['project_start_date']) > 0:
                metadata['Project Start Date'] = study_details['project_start_date']
            if study_details['project_end_date'] is not None and len(study_details['project_end_date']) > 0:
                metadata['Project End Date'] = study_details['project_end_date']
            if study_details['institution'] is not None and len(study_details['institution']) > 0:
                metadata['Institution'] = study_details['institution']
            if study_details['pi_list'] is not None and len(study_details['pi_list']) > 0:
                metadata['Investigator/s'] = study_details['pi_list']
            if len(study_details['data_availability']) > 0:
                metadata['Data Availability'] = study_details['data_availability']
            if len(study_details['repositories']) > 0:
                metadata['Data Package Links'] = study_details['repositories']
            print(study_details['id'])
            study = DugStudy(
                        id=study_details['id'],
                        name=study_details['study_name'],
                        description=study_details['description'],
                        programs=[research_program] if research_program else [],
                        parents=[],
                        action = study_details['action'],
                        abstract=study_details['abstract'],
                        publications = study_details['publication_list'],
                        variable_list = [k.id for k in dug_variables] if dug_variables is not None else [],
                        section_list = study_cde_mappings[study_details['id']] if study_details['id'] in study_cde_mappings else [],
                        metadata = metadata
                        )
            if research_program:
                study.add_tag("Research Program", research_program)
            if research_network:
                study.add_tag("Research Network", research_network)

            logger.debug(study)
            if dug_variables is None:
                elements = [study]
            else:
                elements = dug_variables
                elements.append(study)
                
            study_json = [k.model_dump() for k in elements]
            file_path = Path(output)/f"{study_details['id']}.dug.json"
            print(file_path)
            with open(file_path, "w") as f:
                json.dump(study_json, f, indent=4)

if __name__ == "__main__":
    get_heal_studies()