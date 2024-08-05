"""
get_dbgap_data_dicts.py - Download data dictionaries from dbGaP in a format that Dug can ingest.

Based on get_ncpi_data_dicts.py.
"""

import logging
import os
import shutil
from ftplib import FTP, error_perm, error_temp
import csv
import click
import requests
from urllib.parse import urljoin

# Default to logging at the INFO level.
logging.basicConfig(level=logging.INFO)

# FTP timeout in seconds
FTP_TIMEOUT = 100


# Helper function
def download_dbgap_study(dbgap_accession_id, dbgap_output_dir):
    """
    Download a dbGaP study to a specific directory.

    :param dbgap_accession_id: The dbGaP study identifier to use. This should include the version number, e.g. `phs002206.v2.p1`.
    :param dbgap_output_dir: The directory to download files to.
    :return: The number of downloaded variables.
    """

    count_downloaded_vars = 0

    ftp = FTP('ftp.ncbi.nlm.nih.gov', timeout=FTP_TIMEOUT)
    ftp.login()
    ftp.sendcmd('PASV')
    study_variable = dbgap_accession_id.split('.')[0]

    # The output directory already includes the study accession number.
    local_path = dbgap_output_dir # os.path.join(dbgap_output_dir, dbgap_accession_id)
    os.makedirs(local_path, exist_ok=True)

    study_id_path = f"/dbgap/studies/{study_variable}/{dbgap_accession_id}"

    # Step 1: First we try and get all the data_dict files
    try:
        ftp.cwd(f"{study_id_path}/pheno_variable_summaries")
    except error_perm as e1:
        logging.warning(f"Exception {e1} thrown when trying to access {study_id_path}/pheno_variable_summaries on the dbGaP FTP server.")
        # Delete subdirectory so we don't think it's full
        shutil.rmtree(local_path)
        try:
            files_in_dir = ftp.nlst(study_id_path)
        except error_perm as e2:
            logging.error(f"dbGaP study accession identifier not found on dbGaP server ({e2}): {study_id_path}")
            return 0

        logging.warning(f"No data dictionaries available for study {dbgap_accession_id}: {files_in_dir}")
        return 0

    ftp_filelist = ftp.nlst(".")
    ftp.quit()

    for ftp_filename in ftp_filelist:
        if 'data_dict' in ftp_filename:
            with open(f"{local_path}/{ftp_filename}", "wb") as data_dict_file:
                logging.debug(f"Downloading {ftp_filename} to {local_path}/{ftp_filename}")

                # ftp.retrbinary() seems to cause this program to crash.
                # Luckily, dbGaP is also available on HTTP!
                filename_url = f"https://ftp.ncbi.nlm.nih.gov/{study_id_path}/pheno_variable_summaries/{ftp_filename}"
                response = requests.get(filename_url)
                if not response.ok:
                    logging.error(f"Could not download {filename_url}: {response}")
                    continue

                data_dict_file.write(response.content)
                logging.info(f"Downloaded {ftp_filename} to {local_path}/{ftp_filename} in {response.elapsed.microseconds} microseconds.")
            count_downloaded_vars += 1

    # Sometimes we've timed out on the FTP server by this point. So let's disconnect and reconnect.
    ftp = FTP('ftp.ncbi.nlm.nih.gov', timeout=FTP_TIMEOUT)
    ftp.login()
    ftp.sendcmd('PASV')

    # Step 2: Check to see if there's a GapExchange file in the parent folder
    #         and if there is, get it.
    try:
        ftp.cwd(study_id_path)
    except error_temp as e:
        logging.error("FTP session timed out. Reconnecting.")
        ftp = FTP('ftp.ncbi.nlm.nih.gov', timeout=FTP_TIMEOUT)
        ftp.login()
        ftp.sendcmd('PASV')
        resp = ftp.cwd(study_id_path)
        if resp[:1] == '2':
            logging.info("command success")
    ftp_filelist = ftp.nlst(".")
    for ftp_filename in ftp_filelist:
        if 'GapExchange' in ftp_filename:
            with open(f"{local_path}/{ftp_filename}", "wb") as data_dict_file:
                ftp.retrbinary(f"RETR {ftp_filename}", data_dict_file.write)
                logging.info(f"Downloaded {ftp_filename} to {local_path}/{ftp_filename}")
    ftp.quit()
    return count_downloaded_vars

@click.command()
@click.argument('input_file', type=click.File('r'))
@click.option('--format', help='The format of the input file.', type=click.Choice(['CSV', 'TSV']), default='TSV')
@click.option('--field', help='The field name containing dbGaP study IDs or accession IDs.', default=['dbgap_study_accession'], type=str, multiple=True)
@click.option('--outdir', help='The output directory to create and write dbGaP files to.', type=click.Path(file_okay=False, dir_okay=True, exists=False), default='data/dbgap')
@click.option('--group-by', help='Create subdirectories for the specified fields.', type=str, multiple=True)
@click.option('--skip', help='dbGaP identifier to skip when downloading.', type=str, multiple=True)
def get_dbgap_data_dicts(input_file, format, field, outdir, group_by, skip):
    """
    Given a TSV or CSV file with a `dbgap_study_id` field, download all dbGaP variables for Dug ingest.

    SYNOPSIS

    python get_dbgap_data_dicts.py [input_file]

    Where input_file is the TSV file to read data from. (To use a CSV file instead, add `--format csv`).

    EXAMPLE

    python get_dbgap_data_dicts.py data/ncpi-dataset-catalog-results.tsv --format tsv --field "Study Accession" --outdir

    :param input_file: The input file containing dbGaP identifiers.
    :param format: The format of the input file.
    :param field: A list of field names to look for dbGaP identifiers in.
    :param outdir: The output directory to use. This must not exist when this code is called.
    :param group_by: Group the outputs into subdirectories based on the specified fields.
    :return: Exit code (0 on success, something else on errors)
    """
    output_dir = click.format_filename(outdir)
    dbgap_ids_to_skip = set(skip)

    # `field` is given to us as a tuple. For easier processing, we cast it into a list().
    fields = list(field)

    # Make new output dir
    os.makedirs(f"{output_dir}/", exist_ok=True)

    # We support two dialects:
    #   - "excel": CSV (https://docs.python.org/3/library/csv.html#csv.excel)
    #   - "excel_tab": TSV (https://docs.python.org/3/library/csv.html#csv.excel_tab)
    if format == 'CSV':
        dialect = 'excel'
    elif format == 'TSV':
        dialect = 'excel-tab'
    else:
        raise RuntimeError(f"Unknown --format specified: {format}")

    count_rows = 0
    count_downloaded = 0
    reader = csv.DictReader(input_file, dialect=dialect)
    for (row_index, row) in enumerate(reader):
        line_num = row_index + 1
        count_rows += 1

        # We allow the user to specify multiple fields. In this case, we use dbGaP identifiers from every field,
        # and only produce an error if every row doesn't have at least one identifier.
        dbgap_ids = set()
        for fname in fields:
            if fname in row and row[fname] != '':
                dbgap_ids.add(row[fname])

        if not dbgap_ids:
            raise RuntimeError(f"No dbGaP identifiers found in fields {fields} on line {line_num} of input file: {row}")

        # Determine the output directory. If no group-by fields are specified, just use output_dir.
        # If multiple group-by fields are specified, we use them in order.
        output_dir_for_row = output_dir
        for group_name in list(group_by):
            if group_name in row and row[group_name].strip() != '':
                if '|' in row[group_name]:
                    raise RuntimeError(
                        f"Pipe-separated multiple values in group-by field {group_name} not supported:" +
                        f"{row[group_name]} (line {line_num})"
                    )
                output_dir_for_row = os.path.join(output_dir_for_row, row[group_name])
            else:
                output_dir_for_row = os.path.join(output_dir_for_row, '__missing__')

        logging.debug(f"Row {line_num} containing dbGaP IDs {dbgap_ids} will be written to {output_dir_for_row}")
        os.makedirs(output_dir_for_row, exist_ok=True)

        for dbgap_id in sorted(list(dbgap_ids)):
            # TODO: this skip logic was added to deal with phs000285.v3.p2 and phs000007.v32.p13, which doesn't work
            # for some reason.
            if dbgap_id in dbgap_ids_to_skip:
                logging.info(f"Skipping dbGaP accession {dbgap_id}")
                continue

            dbgap_dir = os.path.join(output_dir_for_row, dbgap_id)
            # Try to download to output folder if the study hasn't already been downloaded
            if not os.path.exists(dbgap_dir):
                logging.info(f"Downloading {dbgap_id} to {dbgap_dir}")
                try:
                    count_downloaded += download_dbgap_study(dbgap_id, dbgap_dir)
                except Exception as ex:
                    logging.error(f"Exception occurred while downloading {dbgap_id} to {dbgap_dir}: {ex}")
                    shutil.rmtree(dbgap_dir, ignore_errors=True)
                    logging.error(f"Deleted {dbgap_dir} as it is probably incomplete.")
                    logging.error("Re-run this script to ensure that all variables are downloaded.")

    logging.info(f"Downloaded {count_downloaded} data dictionaries from {count_rows} rows in input files.")


if __name__ == "__main__":
    get_dbgap_data_dicts()
