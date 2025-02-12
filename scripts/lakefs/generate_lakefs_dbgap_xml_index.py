#!/usr/bin/env python3
#
# generate_lakefs_dbgap_xml_index.py - Generate an index of all the dbGaP XML files in a set of LakeFS repositories.
#
# For some applications (at the moment, HEAL), we have data dictionaries flowing into LakeFS from multiple
# sources -- one repository from the Platform MDS, one repository from the GitHub repository, and so on.
# At the moment, Roger doesn't have any built-in support for checking for duplicates.
#
# This script is intended to be provided with a list of LakeFS repositories (either on the command line or in a
# newline-delimited text file). We also need LakeFS authentication information, which can be provided in one of the
# [two ways supposed by the LakeFS library and client](https://lakefs.io/blog/improved-python-experience/#configuring-a-lakefs-client):
#   1. By creating an ~/.lakectl.yaml file with the authentication information.
#   2. By setting the LAKECTL_SERVER_ENDPOINT_URL, LAKECTL_CREDENTIALS_ACCESS_KEY_ID and
#      LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY environment variables.
#
# It then recursively searches through each repository for dbGaP-formatted XML files and reads them, creating an
# index of all the studies, variables and forms. While it does this, it will also look out for duplicate studies,
# forms and variables.
#
# At the moment, this is planned to be an independent script, but in the future it might be useful to integrate
# them into ingest scripts to generate before-and-after reports or something.

import json
import sys
import xml.etree.ElementTree
from collections import defaultdict
import logging

import click
from lakefs_spec import LakeFSFileSystem
from dataclasses import dataclass

# Set up logging.
logging.basicConfig(level=logging.INFO)

# Configuration options.
# When specifying a repository, you can include a branch or tag with a colon, e.g. `heal-mds-import:v3`.
# If no branch is specified, DEFAULT_LAKEFS_BRANCH is used instead.
DEFAULT_LAKEFS_BRANCH = 'main'

# Indexes and dataclasses to store the loaded indexes.
@dataclass(frozen=True)
class Value:
    code: str
    label: str

@dataclass(frozen=True)
class Variable:
    dd_id: str
    id: str
    name: str
    title: str
    description: str
    values: list[Value]
    typ: str

@dataclass(frozen=True)
class Module:
    module: str
    variables: list[Variable]

@dataclass(frozen=True)
class Study:
    repository: str
    filepath: str
    study_id: str
    study_name: str
    study_description: str
    appl_id: str
    study_version: str
    modules: list[Module]

# Index variables.
variables = []
studies = []
studies_by_study_id = defaultdict(list)

def get_child_as_text(node, child):
    children = node.findall(child)
    if len(children) == 0:
        return ""
    elif len(children) == 1:
        return children[0].text
    else:
        raise ValueError(f"Found multiple {child} children in {node}.")


def load_dbgap_xml_file(lakefs, filepath):
    """
    Load a dbGaP XML file.

    This function reads an XML file opened using LakeFS, parses its structure to retrieve
    the study ID, and checks whether that study ID is already present in a tracking dictionary.
    If the study ID exists, the number of occurrences of the file in the dictionary is updated;
    otherwise, the study ID is added to the dictionary with the respective file path and count.

    :param lakefs: Object used to open the XML file from a LakeFS repository.
    :type lakefs: Any
    :param study_id_dict: A dictionary tracking processed study IDs and their associated file paths.
    :type study_id_dict: dict
    :param filepath: Path to the XML file being checked for duplicate study IDs.
    :type filepath: str
    :return: None
    """
    logging.info(f"Loading dbGaP XML file {filepath}.")

    # Use the LakeFS library to open the file path.
    with lakefs.open(filepath, "rt") as f:
        doc = xml.etree.ElementTree.parse(f)
        data_table = doc.getroot()

        modules = defaultdict(list)

        for child in data_table:
            if child.tag == "variable":
                values = []
                value_tags = child.findall('value')
                for value in value_tags:
                    values.append(Value(code=value.attrib['code'], label=value.text))

                variable = Variable(
                    dd_id=child.attrib['dd_id'],
                    id=child.attrib['id'],
                    name=get_child_as_text(child, 'name'),
                    title=get_child_as_text(child, 'title'),
                    description=get_child_as_text(child, 'description'),
                    typ=get_child_as_text(child, 'type'),
                    values=values,
                )
                variables.append(variable)
                modules[child.attrib['dd_id']].append(variable)
            else:
                raise ValueError(f"Found unknown tag {child} in {filepath}.")

        study = Study(
            repository=filepath.split("/")[2],
            filepath=filepath,
            study_id=data_table.attrib['study_id'],
            study_name=data_table.attrib['study_name'],
            study_description=data_table.attrib['study_description'],
            appl_id=data_table.attrib.get('appl_id', ''),
            study_version="",
            modules=modules,
        )

        logging.info(f"Found study: {study}")


def load_lakefs_object(lakefs, obj):
    """
    Check an object for duplicates based on its type.

    This function processes objects which may be files or directories. If the object is a file and ends
    with `.xml`, it calls check_dbgap_xml_file_for_duplicates() to check it. If the object is a directory,
    it recursively explores its contents.

    Unknown types result in a runtime error. Logging is used to provide debug information about skipped
    files.

    :param lakefs: The lakeFS client instance used for file operations.
    :param obj: A dictionary representing the object metadata, including type and name.
    :return: None
    """
    match obj['type']:
        case 'object':
            # An object is a file. But is it a dbGaP XML file?
            obj_name = obj['name']
            if not obj_name.lower().endswith(".xml"):
                # Doesn't look like a dbGaP XML file.
                logging.debug(f"Skipping file {obj_name} as it doesn't end with `.xml`.")
            else:
                # It looks like a dbGaP XML file: check it for duplicates.
                load_dbgap_xml_file(lakefs, obj_name)
        case 'directory':
            # Recurse into this directory.
            for inner_obj in lakefs.ls(obj['name'], detail=True):
                load_lakefs_object(lakefs, inner_obj)
        case _:
            raise RuntimeError(f"Unknown type {obj['type']} in object {json.dumps(obj)}")


@click.command()
@click.option(
    "--repository",
    "-r",
    "repositories",
    multiple=True,
    metavar="REPO_NAME",
    required=True,
    help="One or more LakeFS repositories to check for duplicates (use `repo/branch_name` to specify a branch name).",
)
def check_duplicates_in_lakefs_repos(repositories):
    """
    Detects duplicate study IDs in specified LakeFS repositories and generates a report
    identifying the duplicates. The function connects to the LakeFS server, iterates
    through the objects in each repository, and checks for duplicate study IDs based
    on filepaths.

    :param repositories: One or more LakeFS repositories to be checked for duplicates. Each
        repository can optionally specify a branch name using the `repo:branch_name`
        format. If no branch is specified, the default branch (`main`) will be used.
    :type repositories: tuple[str]
    :return: None
    :raises SystemExit: Exits with code 0 if no duplicates are found, otherwise exits with
        the number of duplicate study IDs found.
    """

    # Log into LakeFS server.
    lakefs = LakeFSFileSystem()

    # Check each repository to be checked.
    study_id_dict = dict()
    for repository_reference in repositories:
        # Handle repositories with branch names (generally represented as e.g. `heal-studies:v2.0`).
        if ':' in repository_reference:
            repository, branch_name = repository_reference.split(':', 2)
        else:
            repository = repository_reference
            branch_name = DEFAULT_LAKEFS_BRANCH

        # Check repository for duplicates.
        logging.info(f"Checking repository {repository} at branch {branch_name} for duplicates.")
        for obj in lakefs.ls(f"lakefs://{repository}/{branch_name}/", detail=True):
            load_lakefs_object(lakefs, obj)

    # Generate an overall report in JSON.
    duplicates = defaultdict(list)
    count_duplicate_study_ids = 0
    for study_id in sorted(study_id_dict.keys()):
        if len(study_id_dict[study_id]['filepaths']) > 1:
            # Duplicate filepaths!
            count_duplicate_study_ids += 1
            duplicates[study_id] = sorted(study_id_dict[study_id]['filepaths'].keys())
    json.dump(duplicates, sys.stdout, indent=2, sort_keys=True)

    # Provide a final duplicate count.
    logging.info(f"Found {count_duplicate_study_ids} duplicate study IDs.")

    # Exit with an exit code, which will be zero if there are no duplicates, and the number of duplicates if there are some.
    sys.exit(count_duplicate_study_ids)


if __name__ == "__main__":
    check_duplicates_in_lakefs_repos()

