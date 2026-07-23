import logging
import os
import shutil
import tempfile
import requests
import click
from pathlib import Path
import csv
import json
from dug_data_model.v2 import DugStudy, DugVariable, DugElementParsedList, SECTION_TYPE, VARIABLE_TYPE
from lakefs_spec import LakeFSFileSystem

logger = logging.getLogger('dug')
logger.setLevel(logging.INFO)

DEFAULT_MDS_ENDPOINT = 'https://healdata.org/mds/metadata'
PUBLIC_MDS_ENDPOINT = 'https://healdata.org/portal/discovery'
MDS_DEFAULT_LIMIT = 10000
DATA_DICT_GUID_TYPE = 'data_dictionary'
DEFAULT_RESEARCH_PROGRAM_NETWORK_ENDPOINT = (
    'https://opzv7se6o6fpwzfpgt4uqne6rm0fnqtu.lambda-url.us-east-1.on.aws/query?name=get_resnet_resprog'
)
DEFAULT_RESEARCH_NORMALIZATION_CSV = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'data/research_program_network_normalization.csv')

HEAL_STUDY_GUID_TYPES = [
    'discovery_metadata',                   # Fully registered studies.
    'unregistered_discovery_metadata'       # Studies added to the Platform MDS but without the investigator registering the study.
]

def _process_cde_json(json_obj, study_cde_mappings, variable_cde_candidates, source_name=''):
    elements = DugElementParsedList.validate_python(json_obj)
    section_obj = [e for e in elements if e.type == SECTION_TYPE]
    if len(section_obj) != 1:
        logger.warning(f"Something wrong with CDE: {source_name}")
        return
    section = section_obj[0]
    section_id_splits = section.id.split(":")
    section_id = section_id_splits[0] if len(section_id_splits) == 1 else section.id.split(":")[1]
    study_mappings = section.metadata['study_mappings']
    if len(study_mappings) > 0:
        for study in list(study_mappings.keys()):
            if study in study_cde_mappings:
                study_cde_mappings[study].append(section_id)
            else:
                study_cde_mappings[study] = [section_id]
    cdes = [e for e in elements if e.type == VARIABLE_TYPE]
    for cde in cdes:
        # variable_cde_candidates: HDPID -> {variable_name -> [{measure, cde}, ...]}
        # A measure can be reused across more than one CDE/section (i.e. across more than
        # one .dug.json file), so we can't pick the "right" one until every file has been
        # read and study_cde_mappings (which CDEs a study actually uses) is complete. See
        # _resolve_variable_cde_mappings for the second pass that does the picking.
        mappings = cde.metadata.get('study_variable_mappings', {})
        for hdpid in mappings:
            variable_cde_candidates.setdefault(hdpid, {})
            for v in mappings[hdpid]:
                variable_cde_candidates[hdpid].setdefault(v, []).append({"measure": cde.id, "cde": section_id})


def _resolve_variable_cde_mappings(variable_cde_candidates, study_cde_mappings):
    """Pick, for each study/variable, the candidate CDE that the study is actually mapped to.

    When a variable's measure is a candidate for more than one CDE, prefer whichever
    candidate CDE(s) the study is known to use (per study_cde_mappings). If more than one
    still matches, or none do, default to the alphabetically first candidate and log it.
    """
    variable_cde_mappings = {}
    for hdpid, variables in variable_cde_candidates.items():
        study_cdes = set(study_cde_mappings.get(hdpid, []))
        variable_cde_mappings[hdpid] = {}
        for variable_name, candidates in variables.items():
            matches = sorted((c for c in candidates if c["cde"] in study_cdes), key=lambda c: c["cde"])
            if not matches:
                matches = sorted(candidates, key=lambda c: c["cde"])
                if len(candidates) > 1:
                    logger.warning(
                        f"No candidate CDE for {hdpid}:{variable_name} matches the study's "
                        f"known CDEs {sorted(study_cdes)}; defaulting to {matches[0]['cde']}"
                    )
            elif len(matches) > 1:
                logger.warning(
                    f"{hdpid}:{variable_name} matches multiple CDEs the study is mapped to "
                    f"({[m['cde'] for m in matches]}); defaulting to {matches[0]['cde']}"
                )
            variable_cde_mappings[hdpid][variable_name] = matches[0]
    total_variable_mappings = sum(len(v) for v in variable_cde_mappings.values())
    logger.info(
        f"CDE mappings: {len(study_cde_mappings)} studies mapped to a CDE section, "
        f"{len(variable_cde_mappings)} studies have {total_variable_mappings} variable-level CDE mappings"
    )
    return variable_cde_mappings


def get_cde_mappings(cde_dir: Path):
    study_cde_mappings = dict()
    variable_cde_candidates = dict()
    cde_files = list(cde_dir.glob("*.dug.json"))
    for cde_file in cde_files:
        with open(cde_file, "r") as f:
            json_obj = json.load(f)
        _process_cde_json(json_obj, study_cde_mappings, variable_cde_candidates, cde_file)
    logger.info(f"Read {len(cde_files)} CDE files from {cde_dir}")
    variable_cde_mappings = _resolve_variable_cde_mappings(variable_cde_candidates, study_cde_mappings)
    return study_cde_mappings, variable_cde_mappings, len(cde_files)


def _get_lakefs_filesystem():
    """Create a LakeFSFileSystem instance with credentials from environment variables."""
    host = os.environ.get('LAKEFS_SERVER_ENDPOINT_URL')
    username = os.environ.get('LAKEFS_ACCESS_KEY_ID')
    password = os.environ.get('LAKEFS_SECRET_ACCESS_KEY')
    
    if not all([host, username, password]):
        raise ValueError(
            f"Missing LakeFS credentials. Required environment variables: "
            f"LAKEFS_SERVER_ENDPOINT_URL={host}, "
            f"LAKEFS_ACCESS_KEY_ID={'***' if username else 'NOT SET'}, "
            f"LAKEFS_SECRET_ACCESS_KEY={'***' if password else 'NOT SET'}"
        )

    if not host.startswith(('http://', 'https://')):
        host = 'https://' + host
        logger.warning(f"LAKEFS_SERVER_ENDPOINT_URL had no scheme; defaulting to https://: {host}")

    logger.info(f"Initializing LakeFSFileSystem with host={host}, username={username}")
    return LakeFSFileSystem(host=host, username=username, password=password)


def load_research_normalization(csv_path: Path) -> dict:
    """Load the raw_value -> {canonical_name, code} table used to normalize research
    program/network values before they're used in `programs`/tags.

    A single shared table is used for both research programs and research networks --
    some entities (e.g. JCOIN) are referred to as both, and their raw_value strings don't
    collide across the two, so one lookup keyed on raw_value covers both fields. Lookups
    are case-insensitive (keyed on the lowercased raw_value) since the source API is
    inconsistent about casing for otherwise-identical values (e.g. "PAIN ERN" vs "Pain
    ERN"). If the same raw_value appears more than once with conflicting canonical
    name/code, the first row wins and the rest are logged as conflicts.
    """
    table = {}
    with open(csv_path, 'r', newline='') as f:
        for row in csv.DictReader(f):
            raw_value = row['raw_value'].strip()
            if not raw_value:
                continue
            key = raw_value.lower()
            entry = {
                'canonical_name': row['canonical_name'].strip(),
                'code': row['code'].strip() or None,
            }
            if key in table:
                if table[key] != entry:
                    logger.warning(
                        f"{csv_path} has conflicting rows for raw_value {raw_value!r} "
                        f"(case-insensitive): {table[key]} vs {entry}; keeping the first row"
                    )
                continue
            table[key] = entry
    return table


def _normalize_research_value(raw_value: str, normalization_table: dict, unmapped: set = None) -> tuple:
    """Return (canonical_name, code) for a raw research program/network value.

    Falls back to the raw value unchanged (with no code) if it isn't in the table yet. If
    `unmapped` is given, misses are added to it so the caller can warn about them once,
    instead of once per occurrence (the same raw value can recur across many studies).
    """
    entry = normalization_table.get(raw_value.lower())
    if entry:
        return entry['canonical_name'], entry['code']
    if unmapped is not None:
        unmapped.add(raw_value)
    return raw_value, None


def _research_tag_value(canonical_name: str, code: str) -> str:
    # Code-first so tags sort consistently by short form. The acronym and full name are
    # still both present as separate tokens in the tag's analyzed search field either way.
    return f"({code}): {canonical_name}" if code else canonical_name


def get_research_program_network_mappings(endpoint_url: str, normalization_table: dict) -> dict:
    """Fetch HDPID -> normalized research program/network info from the Research Program/Network API.

    Each entry has 'research_program'/'research_network' (canonical names, used for the
    `programs` list) and 'research_program_tag'/'research_network_tag' (canonical name +
    code, used for tags -- see _research_tag_value).

    Rows without a study_hdp_id can't be used for this per-study lookup and are dropped. A
    handful of study_hdp_ids appear more than once in the source data with the same program
    but a null network on one row and a real value on another; when that happens we keep
    whichever value is non-null rather than picking a row arbitrarily.
    """
    result = requests.get(endpoint_url)
    if not result.ok:
        raise RuntimeError(f'Could not retrieve research program/network mappings from {endpoint_url}: {result}')
    raw_mappings = {}
    for row in result.json().get('results', []):
        hdpid = row.get('study_hdp_id')
        if not hdpid:
            continue
        existing = raw_mappings.get(hdpid, {})
        raw_mappings[hdpid] = {
            'research_program': row.get('research_program') or existing.get('research_program'),
            'research_network': row.get('research_network') or existing.get('research_network'),
        }
    mappings = {}
    unmapped = set()
    for hdpid, raw in raw_mappings.items():
        program_name = program_code = network_name = network_code = None
        if raw['research_program']:
            program_name, program_code = _normalize_research_value(raw['research_program'], normalization_table, unmapped)
        if raw['research_network']:
            network_name, network_code = _normalize_research_value(raw['research_network'], normalization_table, unmapped)
        mappings[hdpid] = {
            'research_program': program_name,
            'research_program_tag': _research_tag_value(program_name, program_code) if program_name else None,
            'research_network': network_name,
            'research_network_tag': _research_tag_value(network_name, network_code) if network_name else None,
        }
    logger.info(f"Loaded research program/network mappings for {len(mappings)} studies from {endpoint_url}")
    if unmapped:
        logger.warning(
            f"{len(unmapped)} distinct research program/network value(s) from {endpoint_url} have no "
            f"entry in the normalization table and were passed through unchanged: {sorted(unmapped)}"
        )
    return mappings


def get_cde_mappings_from_lakefs(lakefs_uri: str):
    """Read all .dug.json CDE files from a lakeFS URI and return CDE mappings."""
    # Debug: Print environment variables
    logger.info(f"LAKEFS_SERVER_ENDPOINT_URL: {os.environ.get('LAKEFS_SERVER_ENDPOINT_URL', 'NOT SET')}")
    logger.info(f"LAKEFS_ACCESS_KEY_ID: {os.environ.get('LAKEFS_ACCESS_KEY_ID', 'NOT SET')}")
    logger.info(f"LAKEFS_SECRET_ACCESS_KEY: {'***' if os.environ.get('LAKEFS_SECRET_ACCESS_KEY') else 'NOT SET'}")
    
    lakefs = _get_lakefs_filesystem()
    study_cde_mappings = dict()
    variable_cde_candidates = dict()
    cde_count = 0
    for obj in lakefs.ls(lakefs_uri, detail=True, recursive=True):
        if obj['type'] != 'file':
            continue
        obj_name = obj['name']
        if not obj_name.endswith('.dug.json'):
            continue
        with lakefs.open(obj_name, 'rt') as f:
            json_obj = json.load(f)
        _process_cde_json(json_obj, study_cde_mappings, variable_cde_candidates, obj_name)
        cde_count += 1
    logger.info(f"Read {cde_count} CDE files from {lakefs_uri}")
    variable_cde_mappings = _resolve_variable_cde_mappings(variable_cde_candidates, study_cde_mappings)
    return study_cde_mappings, variable_cde_mappings, cde_count


def upload_file_to_lakefs(local_path: Path, lakefs_base_uri: str) -> None:
    """Upload a local file to a lakeFS URI, appending the filename to the base URI."""
    lakefs = _get_lakefs_filesystem()
    base = lakefs_base_uri.rstrip('/')
    dest_uri = f"{base}/{local_path.name}"
    lakefs.put(str(local_path), dest_uri)
    logger.info(f"Uploaded {local_path} -> {dest_uri}")


def send_slack_notification(webhook_url: str, text: str) -> None:
    """Post a message to a Slack incoming webhook. No-ops if webhook_url is falsy, and
    never raises -- a failed notification shouldn't fail the ingest run itself."""
    if not webhook_url:
        logger.debug("SLACK_WEBHOOK_URL not set; skipping Slack notification.")
        return
    try:
        result = requests.post(webhook_url, json={"text": text})
        if not result.ok:
            logger.error(f"Failed to send Slack notification: {result.status_code} {result.text}")
    except requests.RequestException as e:
        logger.error(f"Failed to send Slack notification: {e}")


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
            logger.error(f'Could not retrieve gen3 details and NIH reporter for study ID {study_id}')
            return None

        study_metadata = gen3_discovery.get('study_metadata', {})
        minimal_info = gen3_discovery.get('minimal_info', {})
        if not minimal_info:
                # sometimes this shows up in different places
            minimal_info = study_metadata.get('minimal_info', {})
        
        if not nih_reporter:
            logger.warning(f"No nih_reporter found in study file {study_id}, continuing.")
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

        nih_reporter_link = None
        if study_metadata is not None and ('metadata_location' in study_metadata and 'nih_reporter_link' in study_metadata['metadata_location']):
            nih_reporter_link = study_metadata['metadata_location']['nih_reporter_link']

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
            "nih_reporter_link": nih_reporter_link if nih_reporter_link is not None else "",
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

def transform_dds_to_dug(vlmd_dds, study_id, research_program=None,
                          research_program_tag=None, research_network_tag=None, vlmd_cde_mappings = {}):
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
            if research_network_tag:
                elem.add_tag("Research Network", research_network_tag)
            if research_program_tag:
                elem.add_tag("Research Program", research_program_tag)
            if study_id in vlmd_cde_mappings and variable['name'] in vlmd_cde_mappings[study_id]:
                metadata["cde_mapping"] = vlmd_cde_mappings[study_id][variable['name']]
            elem.metadata = metadata
            dug_variables.append(elem)
    return dug_variables

# Set up command line arguments.
@click.command()
@click.argument('output', type=click.Path(), required=False, default=None)
@click.option(
    '--mds-metadata-endpoint', '--mds', default=DEFAULT_MDS_ENDPOINT,
    help='The MDS metadata endpoint to use, e.g. https://healdata.org/mds/metadata')
@click.option(
    '--research-program-network-endpoint',
    default=DEFAULT_RESEARCH_PROGRAM_NETWORK_ENDPOINT,
    help='The API endpoint that maps HDP study IDs to HEAL research programs/networks.')
@click.option(
    '--research-normalization-csv',
    default=DEFAULT_RESEARCH_NORMALIZATION_CSV,
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    help='CSV (code,raw_value,canonical_name) used to normalize research program/network '
         'values into canonical names and tags.')
@click.option(
    '--cde-location',
    default=None,
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    help='Location to a local directory with DUG JSON files of CDEs to get CDE->Study mapping.'
)
@click.option(
    '--cde-lakefs-location',
    default=None,
    help='lakeFS URI (e.g. lakefs://repo/branch/path/) to read .dug.json CDE files from. '
         'Takes precedence over --cde-location when both are provided.'
)
@click.option(
    '--lakefs-output',
    default=None,
    help='lakeFS URI (e.g. lakefs://repo/branch/path/) to upload output .dug.json files to. '
         'Local file writing still runs regardless of this option.'
)
@click.option(
     '--debug', default=False,
     help='Run in debug mode.'
)
def get_heal_studies(output, mds_metadata_endpoint,
                                     research_program_network_endpoint,
                                     research_normalization_csv,
                                     cde_location,
                                     cde_lakefs_location,
                                     lakefs_output,
                                     debug):
    if not output and not lakefs_output:
        raise click.UsageError("At least one of OUTPUT (argument) or --lakefs-output must be provided.")
    if not cde_location and not cde_lakefs_location:
        raise click.UsageError("At least one of --cde-location or --cde-lakefs-location must be provided.")

    _tmpdir = None
    if not output:
        _tmpdir = tempfile.mkdtemp()
        output = _tmpdir
    Path(output).mkdir(parents=True, exist_ok=True)
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        handlers=[
            logging.FileHandler(Path(output) / "log.txt"),
            logging.StreamHandler(),
        ],
        force=True,
    )

    if not mds_metadata_endpoint:
        mds_metadata_endpoint = DEFAULT_MDS_ENDPOINT

    # Load the normalization sheet.
    research_normalization_table = load_research_normalization(Path(research_normalization_csv))
    # Query the MySQL database to get the Research Network and Research Program mappings for all HDPIDs.
    hdp_program_network_mappings = get_research_program_network_mappings(
        research_program_network_endpoint, research_normalization_table)
    if cde_lakefs_location:
        study_cde_mappings, variable_cde_mappings, cde_file_count = get_cde_mappings_from_lakefs(cde_lakefs_location)
    else:
        study_cde_mappings, variable_cde_mappings, cde_file_count = get_cde_mappings(Path(cde_location))
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

    existing_remote_files: set[str] = set()
    if lakefs_output:
        try:
            pre_check_objects = _get_lakefs_filesystem().ls(lakefs_output, detail=True)
        except FileNotFoundError:
            pre_check_objects = []
        for obj in pre_check_objects:
            filename = obj['name'].rstrip('/').split('/')[-1]
            if filename.endswith('.dug.json'):
                existing_remote_files.add(filename)
        logger.info(f"Found {len(existing_remote_files)} existing study files in lakeFS: {lakefs_output}")

    written_files: set[str] = set()
    # studies = []
    for _ , sid in enumerate(study_ids):
            # if count==20:
            #     break
            study_details = get_study_info_from_mds(study_id = sid, mds_url = mds_metadata_endpoint)
            if study_details is None:
                logger.debug(f"Metadata for Study {sid} is not available, Skipping!")
                continue
            program_network = hdp_program_network_mappings.get(study_details['id'], {})
            research_program = program_network.get('research_program')
            research_program_tag = program_network.get('research_program_tag')
            research_network_tag = program_network.get('research_network_tag')
            dug_variables = transform_dds_to_dug(study_details['vlmd_dds'],
                                                 study_details['id'],
                                                 research_program = research_program,
                                                 research_program_tag = research_program_tag,
                                                 research_network_tag = research_network_tag,
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
            if len(study_details['nih_reporter_link']) > 0:
                metadata['NIH Reporter Link'] = study_details['nih_reporter_link']
            
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
            if research_program_tag:
                study.add_tag("Research Program", research_program_tag)
            if research_network_tag:
                study.add_tag("Research Network", research_network_tag)

            logger.debug(study)
            if dug_variables is None:
                elements = [study]
            else:
                elements = dug_variables
                elements.append(study)
                
            study_json = [k.model_dump() for k in elements]
            file_path = Path(output)/f"{study_details['id']}.dug.json"
            logger.info(file_path)
            with open(file_path, "w") as f:
                json.dump(study_json, f, indent=4)
            written_files.add(file_path.name)
            if lakefs_output:
                upload_file_to_lakefs(file_path, lakefs_output)

    logger.info(f"Wrote {len(written_files)} study files")

    deleted_files = []
    if lakefs_output:
        lakefs = _get_lakefs_filesystem()
        base = lakefs_output.rstrip('/')
        for filename in existing_remote_files:
            if filename not in written_files:
                lakefs.rm(f"{base}/{filename}")
                deleted_files.append(filename)
                logger.info(f"Deleted stale file from lakeFS: {base}/{filename}")
        logger.info(f"Deleted {len(deleted_files)} stale files from lakeFS" +
                    (f": {deleted_files}" if deleted_files else ""))

    total_variable_mappings = sum(len(v) for v in variable_cde_mappings.values())
    summary_lines = [
        "*HEAL ingest completed successfully*",
        f"lakeFS updated: {'yes' if lakefs_output else 'no (local output only)'}",
        f"CDE files read: {cde_file_count}",
        f"Studies mapped to a CDE section: {len(study_cde_mappings)}",
        f"Studies with variable-level CDE mappings: {len(variable_cde_mappings)} ({total_variable_mappings} mappings)",
        f"Study files written: {len(written_files)}",
    ]
    if lakefs_output:
        summary_lines.append(f"Files uploaded to lakeFS: {len(written_files)}")
        summary_lines.append(
            f"Files deleted from lakeFS: {len(deleted_files)}"
            + (f" ({', '.join(sorted(deleted_files))})" if deleted_files else "")
        )
    send_slack_notification(os.environ.get('SLACK_WEBHOOK_URL'), "\n".join(summary_lines))

    if _tmpdir:
        shutil.rmtree(_tmpdir, ignore_errors=True)

if __name__ == "__main__":
    try:
        get_heal_studies()
    except SystemExit as e:
        # Click converts both successful completion and internal errors (bad args,
        # uncaught exceptions) into SystemExit; only notify on a genuine failure.
        if e.code not in (None, 0):
            send_slack_notification(
                os.environ.get('SLACK_WEBHOOK_URL'),
                f"*HEAL ingest FAILED* (exit code {e.code}) -- check the ingest logs for details.",
            )
        raise