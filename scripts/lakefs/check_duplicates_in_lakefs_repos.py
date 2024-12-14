#!/usr/bin/python
#
# check_duplicates_in_lakefs_repos.py - Report on duplicates among a set of LakeFS repositories.
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
# It then recursively searches through each repository for dbGaP-formatted XML files and reads the study_id, which
# is used by Roger and Dug to assign the data dictionary to a study. (It will also look for other IDs, such as
# APPL IDs). It will then produce a report about duplicate study IDs (and, optionally, just all the available study
# IDs).
#
# At the moment, this is planned to be an independent script, but in the future it might be useful to integrate
# them into ingest scripts to generate before-and-after reports or something.
#
# See ticket at https://renci.atlassian.net/browse/DUG-374
import json
import os
import sys
import xml.etree.ElementTree
import logging

from pydantic.utils import defaultdict

logging.basicConfig(level=logging.INFO)

import click
from lakefs_spec import LakeFSFileSystem

# Configuration options
DEFAULT_LAKEFS_BRANCH = 'main'


def check_dbgap_xml_file_for_duplicates(lakefs, study_id_dict, filepath):
    logging.debug(f"Checking file {filepath} for duplicate IDs.")
    with lakefs.open(filepath, "rt") as f:
        doc = xml.etree.ElementTree.parse(f)
        data_table = doc.getroot()
        study_id = data_table.attrib['study_id']

        if study_id in study_id_dict:
            logging.error(f"Duplicate study ID {study_id} found in {filepath} (previously found in {sorted(study_id_dict[study_id]['filepaths'].keys())}.")
            if filepath not in study_id_dict[study_id]['filepaths']:
                study_id_dict[study_id]['filepaths'][filepath] = 0
            study_id_dict[study_id]['filepaths'][filepath] += 1
        else:
            logging.info(f"Found study ID {study_id} in {filepath}.")
            study_id_dict[study_id] = {
                'filepaths': {filepath: 1},
            }


def check_object_for_duplicates(lakefs, study_id_dict, obj):
    match obj['type']:
        case 'object':
            # An object is a file. But is it a dbGaP XML file?
            obj_name = obj['name']
            if not obj_name.lower().endswith(".xml"):
                logging.debug(f"Skipping file {obj_name} as it doesn't end with `.xml`.")
            else:
                # It looks like an XML file: check it for duplicates.
                check_dbgap_xml_file_for_duplicates(lakefs, study_id_dict, obj_name)
        case 'directory':
            # Recurse into this directory.
            for inner_obj in lakefs.ls(obj['name'], detail=True):
                check_object_for_duplicates(lakefs, study_id_dict, inner_obj)
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
    Report on duplicates among a set of LakeFS repositories.

    :param repositories: One or more LakeFS repositories to check for duplicates.
    """

    # Log into LakeFS server.
    lakefs = LakeFSFileSystem()

    # Check each repository to be checked.
    study_id_dict = dict()
    for repository in repositories:
        logging.info(f"Checking repository {repository} for duplicates.")
        if "/" not in repository:
            repo_and_branch_name = f"{repository}/{DEFAULT_LAKEFS_BRANCH}/"
        else:
            repo_and_branch_name = f"{repository}/"
        for obj in lakefs.ls(repo_and_branch_name, detail=True):
            check_object_for_duplicates(lakefs, study_id_dict, obj)

    # Generate an overall report.
    duplicates = defaultdict(list)
    count_duplicate_study_ids = 0
    for study_id in sorted(study_id_dict.keys()):
        if len(study_id_dict[study_id]['filepaths']) > 1:
            # Duplicate filepaths!
            count_duplicate_study_ids += 1
            duplicates[study_id] = sorted(study_id_dict[study_id]['filepaths'].keys())
    json.dump(duplicates, sys.stdout, indent=2, sort_keys=True)

    logging.info(f"Found {count_duplicate_study_ids} duplicate study IDs.")

    # Will be zero if there are no duplicates, and non-zero if there are duplicates.
    sys.exit(count_duplicate_study_ids)

if __name__ == "__main__":
    check_duplicates_in_lakefs_repos()