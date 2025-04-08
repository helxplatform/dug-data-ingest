#!/usr/bin/env python3
#
# generate_lakefs_dbgap_xml_index.py - Generate an index of all the dbGaP XML files in a set of LakeFS repositories.
#
# SYNOPSIS
#   python scripts/lakefs/generate_lakefs_dbgap_xml_index.py -r heal-mds-import -r heal-studies:v2.0 \
#       -r heal-research-programs:v1.0 -r sparc:v1.0 -r nida:v1.0 -r bacpac:v1.0 -r ctn:v1.0 > ~/Downloads/hdp-index.csv
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
import csv
import json
import logging
import xml.etree.ElementTree
from collections import defaultdict
from dataclasses import dataclass

import click
from lakefs_spec import LakeFSFileSystem

# Set up logging.
logging.basicConfig(level=logging.INFO)

# Configuration options.
# When specifying a repository, you can include a branch or tag with a colon, e.g. `heal-mds-import:v3`.
# If no branch is specified, DEFAULT_LAKEFS_BRANCH is used instead.
DEFAULT_LAKEFS_BRANCH = "main"


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
class Section:
    section: str
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
    sections: list[Section]


# Index variables.
variables = []
studies = []
studies_by_study_id = defaultdict(list)


def get_child_as_text(node, child):
    """
    Retrieves the text content of a specified child element within an XML node.

    The function searches for all child elements matching the `child` tag within
    the given `node`. As long as there is exactly one matching child element, it
    returns the text content of that element. If there are more than one matching
    elements, it raises a `ValueError`.

    :param node: The parent XML node from which to search for the child
        elements.
    :type node: xml.etree.ElementTree.Element
    :param child: The tag name of the child element to retrieve the text from.
    :type child: str
    :return: The text content of the child element if exactly one child
        is found, or an empty string if no matching child elements exist.
    :rtype: str
    :raises ValueError: If multiple child elements matching the `child` tag
        are found within the node.
    """
    children = node.findall(child)
    if len(children) == 0:
        return ""
    elif len(children) == 1:
        return children[0].text
    else:
        raise ValueError(f"Found multiple {child} children in {node}.")


def load_dbgap_xml_file(lakefs, repository, filepath):
    """
    Load a dbGaP XML file.

    This function reads an XML file opened using LakeFS, parses its structure to retrieve
    the study ID, and checks whether that study ID is already present in a tracking dictionary.
    If the study ID exists, the number of occurrences of the file in the dictionary is updated;
    otherwise, the study ID is added to the dictionary with the respective file path and count.

    We use the global index variables (variables, studies, studies_by_study_id) to store the loaded data.

    :param lakefs: Object used to open the XML file from a LakeFS repository.
    :type lakefs: Any
    :param repository: The name of the LakeFS repository containing the file, including the tag or branch name.
    :type repository: str
    :param filepath: Path to the XML file being checked for duplicate study IDs.
    :type filepath: str
    :return: None
    """
    logging.info(f"Loading dbGaP XML file {filepath}.")

    # Use the LakeFS library to open the file path.
    with lakefs.open(filepath, "rt") as f:
        doc = xml.etree.ElementTree.parse(f)
        data_table = doc.getroot()

        # Group the variables into sections.
        sections = defaultdict(list)

        for child in data_table:
            if child.tag == "variable":
                values = []
                value_tags = child.findall("value")
                for value in value_tags:
                    values.append(Value(code=value.attrib["code"], label=value.text))

                variable = Variable(
                    dd_id=child.attrib.get("dd_id", ""),
                    id=child.attrib.get("id", ""),
                    name=get_child_as_text(child, "name"),
                    title=get_child_as_text(child, "title"),
                    description=get_child_as_text(child, "description"),
                    typ=get_child_as_text(child, "type"),
                    values=values,
                )
                variables.append(variable)

                section = child.attrib.get(
                    "section", child.attrib.get("module", child.attrib.get("dd_id", ""))
                )
                if not section:
                    logging.warning(
                        f"Found variable {child} with no section or module in {filepath}, using 'none'."
                    )
                    section = "none"
                sections[section].append(variable)
            else:
                raise ValueError(f"Found unknown tag {child} in {filepath}.")

        study_id = data_table.attrib["study_id"]
        logging.info(
            f"Repository {repository} with filepath {filepath} contains {len(sections)} sections and {len(variables)} variables for study ID {study_id}."
        )

        sections_as_list = list(
            map(lambda s: Section(section=s, variables=sections[s]), sections.keys())
        )
        study = Study(
            repository=repository,
            filepath=filepath,
            study_id=study_id,
            study_name=data_table.attrib.get("study_name", ""),
            study_description=data_table.attrib.get("study_description", ""),
            appl_id=data_table.attrib.get("appl_id", ""),
            study_version="",
            sections=sections_as_list,
        )
        studies.append(study)
        studies_by_study_id[study_id].append(study)


def load_lakefs_object(lakefs, repository, obj):
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
    match obj["type"]:
        case "object":
            # An object is a file. But is it a dbGaP XML file?
            obj_name = obj["name"]
            if not obj_name.lower().endswith(".xml"):
                # Doesn't look like a dbGaP XML file.
                logging.debug(
                    f"Skipping file {obj_name} as it doesn't end with `.xml`."
                )
            else:
                # It looks like a dbGaP XML file: check it for duplicates.
                load_dbgap_xml_file(lakefs, repository, obj_name)
        case "directory":
            # Recurse into this directory.
            for inner_obj in lakefs.ls(obj["name"], detail=True):
                load_lakefs_object(lakefs, repository, inner_obj)
        case _:
            raise RuntimeError(
                f"Unknown type {obj['type']} in object {json.dumps(obj)}"
            )


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
@click.option(
    "--output",
    "-o",
    type=click.File("w"),
    metavar="FILE",
    default="-",
    help="Path to the output CSV file. If not specified, the output will be printed to stdout.",
)
def generate_lakefs_dbgap_xml_index(repositories, output):
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
    for repository_reference in repositories:
        # Handle repositories with branch names (generally represented as e.g. `heal-studies:v2.0`).
        if ":" in repository_reference:
            repository, branch_name = repository_reference.split(":", 2)
        else:
            repository = repository_reference
            branch_name = DEFAULT_LAKEFS_BRANCH

        # Check repository for duplicates.
        logging.info(
            f"Checking repository {repository} at branch {branch_name} for duplicates."
        )
        for obj in lakefs.ls(f"lakefs://{repository}/{branch_name}/", detail=True):
            load_lakefs_object(lakefs, repository_reference, obj)

    # Generate an overall report in CSV.
    fieldnames = ["HDPID", "repository_count"]
    fieldnames.extend(sorted(repositories))
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for study_id in sorted(studies_by_study_id.keys()):
        row = {"HDPID": study_id}
        repository_count = 0
        for repository in repositories:
            filtered_studies = list(
                filter(
                    lambda s: s.repository == repository, studies_by_study_id[study_id]
                )
            )

            section_count = 0
            variable_count = 0
            for study in filtered_studies:
                for section in study.sections:
                    section_count += 1
                    variable_count += len(section.variables)

            if len(filtered_studies) == 0:
                row[repository] = ""
            else:
                row[repository] = (
                    f"{len(filtered_studies)} DDs containing {section_count} sections containing {variable_count} variables"
                )
                repository_count += 1

        row["repository_count"] = repository_count
        writer.writerow(row)


if __name__ == "__main__":
    generate_lakefs_dbgap_xml_index()
