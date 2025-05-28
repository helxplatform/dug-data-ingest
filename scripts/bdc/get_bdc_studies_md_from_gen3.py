# This scripts download the medta from Gen3 Meta Data servce.
# Some of the studies in Gen3 only have PhsID for new studies and acts as placeholder.
# The studies which do not have Title, decription and other essential field are removed.


import csv
import json
import logging
import os
import re
import sys
import urllib.parse
import xml.dom.minidom as minidom
import xml.etree.ElementTree as ETree
from collections import defaultdict, Counter
from datetime import datetime
import requests
import argparse

# Configuration
GEN3_DOWNLOAD_LIMIT = 50

class Gen3DataProcessor:
    def __init__(self, output_dir="gen3_output"):
        # Use user-specified output directory
        self.output_dir = output_dir
        self.setup_directory()
        self.setup_logging()
        
    def setup_directory(self):
        os.makedirs(self.output_dir, exist_ok=True)
        
    def setup_logging(self):
        self.log_file = os.path.join(self.output_dir, "gen3_processing.log")

        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
            
        # Configure logging format
        logging_format = '%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=logging_format,
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def download_gen3_list(self, input_url, download_limit=GEN3_DOWNLOAD_LIMIT):
        complete_list = []
        offset = 0
        while True:
            url = input_url + f"&limit={download_limit}&offset={offset}"
            self.logger.debug(f"Requesting GET {url} from Gen3")
            
            try:
                partial_list_response = requests.get(url)
                partial_list_response.raise_for_status()
                
                partial_list = partial_list_response.json()
                complete_list.extend(partial_list)
                
                if len(partial_list) < download_limit:
                    break
                    
                offset += download_limit
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error downloading from Gen3: {e}")
                raise
        
        if len(complete_list) != len(set(complete_list)):
            duplicate_ids = sorted([ident for ident, count in Counter(complete_list).items() if count > 1])
            raise ValueError(f"Found duplicate discovery_metadata: {duplicate_ids}")
        return complete_list

    def download_raw_data(self):
        """Download and save raw data from Gen3"""
        self.raw_file = os.path.join(self.output_dir, "gen3_studies_raw.csv")
        self.logger.info(f"Starting raw data download. Output file: {self.raw_file}")
        
        # Download metadata
        base_url = 'https://gen3.biodatacatalyst.nhlbi.nih.gov/'
        mds_discovery_metadata_url = urllib.parse.urljoin(
            base_url,
            f'/mds/metadata?_guid_type=discovery_metadata'
        )
        
        discovery_list = self.download_gen3_list(mds_discovery_metadata_url)
        self.logger.info(f"Downloaded {len(discovery_list)} discovery_metadata entries")
        
        sorted_study_ids = sorted(discovery_list)
        
        # Write to CSV
        with open(self.raw_file, 'w', newline='') as csvfile:
            csv_writer = csv.DictWriter(csvfile, fieldnames=[
                'Accession', 'Consent', 'Study Name', 'Program', 
                'Last modified', 'Description'
            ])
            csv_writer.writeheader()
            
            for study_id in sorted_study_ids:
                # Reset variables
                study_name = ''
                program_names = []
                description = ''
                last_modified = ''#str(datetime.now().date())
                
                # Get study info
                url = urllib.parse.urljoin(base_url, f'/mds/metadata/{study_id}')
                try:
                    study_info_response = requests.get(url)
                    study_info_response.raise_for_status()
                    study_info = study_info_response.json()
                    
                    if 'gen3_discovery' in study_info:
                        gen3_discovery = study_info['gen3_discovery']
                        
                        # Handle study name
                        if 'full_name' in gen3_discovery:
                            study_name = gen3_discovery['full_name']
                        elif 'name' in gen3_discovery:
                            study_name = gen3_discovery['name']
                        elif 'short_name' in gen3_discovery:
                            study_name = gen3_discovery['short_name']
                        else:
                            study_name = '(no name)'
                        
                        # Handle program name
                        try:
                            if 'authz' in gen3_discovery:
                                match = re.fullmatch(r'^/programs/(.*)/projects/(.*)$', gen3_discovery['authz'])
                                if match:
                                    program_names.append(match.group(1))
                        except Exception as e:
                            self.logger.warning(f"Error parsing 'authz' value for study {study_id}: {str(e)}")

                        description = gen3_discovery.get('study_description', '')
                    
                    # Extract accession and consent
                    m = re.match(r'^(phs.*?)(?:\.(c\d+))?$', study_id)
                    if not m:
                        self.logger.warning(f"Skipping study_id '{study_id}' as non-dbGaP identifiers are not supported")
                        continue
                    
                    accession = m.group(1)
                    consent = m.group(2) if m.group(2) else ''
                    
                    # Write row
                    csv_writer.writerow({
                        'Accession': accession,
                        'Consent': consent,
                        'Study Name': study_name,
                        'Description': description,
                        'Program': '|'.join(sorted(set(filter(None, program_names)))),
                        'Last modified': last_modified,
                    })
                    
                except requests.exceptions.RequestException as e:
                    self.logger.error(f"Error processing study {study_id}: {e}")
                    continue
        
        return self.raw_file
    
    def filter_studies(self):
        """Filter studies which have missing information fields"""
        self.filtered_file = os.path.join(self.output_dir, "gen3_studies_filtered.csv")
        self.logger.info(f"Starting study filtering process. Output file: {self.filtered_file}")
        
        total_studies = 0
        valid_studies = 0
        skipped_studies = 0
        valid_study_details = []
        skipped_details = []
        
        try:
            with open(self.raw_file, 'r', encoding='utf-8') as infile, \
                 open(self.filtered_file, 'w', newline='', encoding='utf-8') as outfile:
                
                reader = csv.DictReader(infile)
                writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
                writer.writeheader()
                
                for row in reader:
                    total_studies += 1
                    is_valid, invalid_reason = self.validate_study(row)
                    
                    if is_valid:
                        writer.writerow(row)
                        valid_studies += 1
                        study_id = f"{row['Accession']}.{row.get('Consent', '')}"
                        valid_study_details.append(study_id)
                    else:
                        skipped_studies += 1
                        study_id = f"{row['Accession']}.{row.get('Consent', '')}"
                        skipped_details.append(f"Study {study_id}: {invalid_reason}")
            
            # Write summary
            self.write_summary(total_studies, valid_studies, skipped_studies, 
                             valid_study_details, skipped_details)
            
        except Exception as e:
            self.logger.error(f"An error occurred during filtering: {str(e)}")
            raise
    # This will clean the studies and look for the required fields. 
    def validate_study(self, row):
        """Validate if a study has all required fields that are required to xml file"""
        required_fields = ['Accession', 'Consent', 'Study Name', 'Program', 'Description']
        missing_fields = []
        
        for field in required_fields:
            if not row.get(field) or row[field].strip() == '':
                missing_fields.append(field)
        
        if missing_fields:
            return False, f"Missing required fields: {', '.join(missing_fields)}"
        return True, None

    def write_summary(self, total, valid, skipped, valid_details, skipped_details):
        """Write summary statistics to log"""
        self.logger.info("=" * 80)
        self.logger.info("PROCESSING SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"Total studies processed: {total}")
        self.logger.info(f"Valid studies (included in output): {valid}")
        self.logger.info(f"Skipped studies: {skipped}")
        
        if valid_details:
            self.logger.info("\nValid studies included in output:")
            for study in sorted(valid_details):
                self.logger.info(f"- {study}")
        
        if skipped_details:
            self.logger.info("\nSkipped studies and reasons:")
            for detail in sorted(skipped_details):
                self.logger.info(f"- {detail}")
        
        self.logger.info("\nOutput files:")
        self.logger.info(f"- Output directory: {os.path.abspath(self.output_dir)}")
        self.logger.info(f"- Raw data file: {os.path.abspath(self.raw_file)}")
        self.logger.info(f"- Filtered data file: {os.path.abspath(self.filtered_file)}")
        self.logger.info(f"- Log file: {os.path.abspath(self.log_file)}")
        self.logger.info("=" * 80)

def main():
    parser = argparse.ArgumentParser(description='Gen3 Data Extraction and Cleaning Tool')
    parser.add_argument('--output-dir', type=str, help='Directory to save output files')
    args = parser.parse_args()
    output_dir = args.output_dir
    if not output_dir:
        # Check environment variable
        env_output_path = os.environ.get("GEN3_OUTPUT_PATH")
        if env_output_path:
            output_dir = env_output_path
        else:
            # Default to current directory
            output_dir = os.path.join(os.getcwd(), "gen3_md")
    os.makedirs(output_dir, exist_ok=True)
    print(f"Using output directory: {output_dir}")
    processor = Gen3DataProcessor(output_dir=output_dir)    
    print(f"Starting complete Gen3 data processing pipeline...")
    processor.download_raw_data()
    processor.filter_studies()
    print(f"Processing completed successfully!")
    sys.exit(0)
        


if __name__ == "__main__":
    main()