#!/usr/bin/env python
"""
PicSure Data Extraction and Cleaning Tool

This script extracts metadata from PicSure API based on user access to studies
and cleans the extracted data according to specific criteria. To run this will need a PICSURE_TOKEN

Usage:
    python picsure_tool.py [--output-dir OUTPUT_DIR] [--token TOKEN]

Requirements:
    - PicSureClient
    - PicSureBdcAdapter
    - pandas
    - numpy
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

try:
    import PicSureClient
    import PicSureBdcAdapter
except ImportError as e:
    sys.exit(f"Error: Could not import PicSure modules. {str(e)}")


def clean_data(data):
    """
    Clean data based on specific criteria
    """
    df = data.copy()
    
    mask = (
        df['dtId'].notna() &
        df['varId'].notna() &
        df['derived_var_name'].notna() &
        df['description'].notna() &
        df['varId'].str.startswith('phv')
    )
    
    cleaned_df = df[mask]
    cleaned_df = cleaned_df.reset_index(drop=True)
    
    return cleaned_df


def extract_picsure_data(output_dir, token=None):
    """
    Extract metadata from PicSure API
    """
    log_file = output_dir / "picsure_extraction.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(str(log_file)),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    output_file = output_dir / "pic_sure_data.csv"
    
    try:
        logging.info("Starting PicSure data extraction")
        
        if token is None:
            if "PICSURE_TOKEN" not in os.environ:
                logging.error("PICSURE_TOKEN environment variable not set")
                sys.exit("Error: PICSURE_TOKEN environment variable not set")
            token = os.environ["PICSURE_TOKEN"]
        
        PICSURE_network_URL = "https://picsure.biodatacatalyst.nhlbi.nih.gov/picsure"
        
        logging.info(f"Connecting to PicSure at {PICSURE_network_URL}")
        bdc = PicSureBdcAdapter.Adapter(PICSURE_network_URL, token)
        
        logging.info("Setting up dictionary")
        dictionary = bdc.useDictionary().dictionary()
        
        logging.info("Retrieving all accessible variables")
        all_variables = dictionary.find()
        
        logging.info("Converting to dataframe")
        all_variables_df = all_variables.dataframe()
        
        num_unique_studyIds = all_variables_df['studyId'].nunique()
        unique_study_ids = all_variables_df['studyId'].unique()
        
        logging.info(f"Number of unique studyIds: {num_unique_studyIds}")
        
        logging.info("Unique study IDs:")
        for study_id in unique_study_ids:
            logging.info(f"  - {study_id}")
            
        logging.info(f"All unique study IDs: {', '.join(map(str, unique_study_ids))}")
        
        variable_count = all_variables.count()
        logging.info(f"Total variable count: {variable_count}")
        
        logging.info(f"Saving data to {output_file}")
        all_variables_df.to_csv(output_file, index=False)
        
        logging.info("Data extraction completed successfully")
        print(f"Data extraction completed successfully. Results saved to: {output_file}")
        print(f"Log file saved to: {log_file}")
        print(f"Number of unique studyIds: {num_unique_studyIds}")
        print(f"Unique study IDs: {', '.join(map(str, unique_study_ids))}")
        
        return output_file
    
    except Exception as e:
        logging.error(f"Error during execution: {e}", exc_info=True)
        print(f"An error occurred. Please check the log file for details: {log_file}")
        sys.exit(1)

#Clean picsure csv
def process_and_clean_file(file_path, output_dir=None):
    try:
        df = pd.read_csv(file_path)
        cleaned_df = clean_data(df)
        
        if output_dir is None:
            cleaned_file = file_path.parent / f"cleaned_{file_path.name}"
        else:
            cleaned_file = output_dir / f"cleaned_{file_path.name}"
        
        cleaned_df.to_csv(cleaned_file, index=False)
        
        print(f"Original rows: {len(df)}")
        print(f"Rows after cleaning: {len(cleaned_df)}")
        print(f"Removed rows: {len(df) - len(cleaned_df)}")
        print(f"Cleaned data saved to: {cleaned_file}")
        
        return cleaned_file
    
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return None


def main():
    """
    Main function to run PicSure extraction and data cleaning workflow
    """
    parser = argparse.ArgumentParser(description='PicSure Data Extraction and Cleaning Tool')
    parser.add_argument('--output-dir', type=str, help='Directory to save output files')
    parser.add_argument('--token', type=str, help='PicSure API token (if not using environment variable)')
    
    args = parser.parse_args()
    
    output_dir = args.output_dir
    if not output_dir:
        env_output_path = os.environ.get("PICSURE_OUTPUT_PATH")
        if env_output_path:
            output_dir = env_output_path
        else:
            output_dir = os.getcwd()
    
    output_path = Path(output_dir) / "picsure_md"
    
    try:
        output_path.mkdir(parents=True, exist_ok=True)
        print(f"Output directory created: {output_path}")
    except Exception as e:
        sys.exit(f"Error creating output directory: {e}")
    
    # Extract and clean mode
    print(f"Downloading PicSure data to: {output_path}")
    extracted_file = extract_picsure_data(output_path, args.token)
    
    print("\nCleaning downloaded data...")
    process_and_clean_file(extracted_file, output_path)
    
    print(f"\nProcess completed. All files saved in: {output_path}")


if __name__ == "__main__":
    main()