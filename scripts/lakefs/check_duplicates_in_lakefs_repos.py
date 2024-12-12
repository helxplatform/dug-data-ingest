#!/usr/bin/python
#
# check_duplicates_in_lakefs_repos.py - Report on duplicates among a set of LakeFS repositories.
#
# For some applications (at the moment, HEAL), we have data dictionaries flowing into LakeFS from multiple
# sources -- one repository from the Platform MDS, one repository from the GitHub repository, and so on.
# At the moment, Roger doesn't have any built-in support for checking for duplicates.
#
# This script is intended to be provided with a list of LakeFS repositories (either on the command line or
# in a newline-delimited text file) as well as LakeFS authorization information (in the same LAKEFS_HOST/
# LAKEFS_USERNAME/LAKEFS_PASSWORD environment variables as the other scripts in this repo). It then recursively
# searches through each repository for dbGaP-formatted XML files and reads the study_id, which is used by Roger and
# Dug to assign the data dictionary to a study. (It will also look for other IDs, such as APPL IDs). It will
# then produce a report about duplicate study IDs (and, optionally, just all the available study IDs).
#
# At the moment, this is planned to be an independent script, but in the future it might be useful to integrate
# them into ingest scripts to generate before-and-after reports or something.
#
# See ticket at https://renci.atlassian.net/browse/DUG-374

import click

@click.command()
@click.option(
    "--repository",
    "-r",
    "repositories",
    multiple=True,
    metavar="REPO_NAME",
    required=True,
    help="One or more LakeFS repositories to check for duplicates.",
)
def check_duplicates_in_lakefs_repos(repositories):
    """
    Report on duplicates among a set of LakeFS repositories.

    :param repositories: One or more LakeFS repositories to check for duplicates.
    """
    for repository in repositories:
        print(repository)


if __name__ == "__main__":
    check_duplicates_in_lakefs_repos()