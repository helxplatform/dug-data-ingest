#!/usr/bin/env python3
"""
Script for downloading dbGaP data dictionaries with XML generation fallback.
Handles cases where studies are not found on dbGaP by gracefully falling back 
to XML generation via run_xml_generator.
"""

import os
import sys
import shutil
import logging
import pandas as pd
import argparse
from datetime import datetime
from get_dbgap_data_dicts import download_dbgap_study
# Import the XML generator functionality directly
from xml_generator import run_xml_generation
from xml_utils import (
    generate_xml_for_study_safe,
    setup_logging_safe
)

# Configuration with defaults (can be overridden via command line)
DEFAULT_CONFIG = {
    "GEN3_CSV": "/path/to/gen3_studies_filtered.csv",
    "PICSURE_CSV": "/path/to/cleaned_pic_sure_data.csv",
    "OUTPUT_DIR": "output",
    "ACCESSION_FIELD": "Accession",
    "STUDY_ID_FIELD": "study_id",
    "STUDY_NAME_FIELD": "Study Name",
}

def setup_config():
    parser = argparse.ArgumentParser(
        description='Download dbGaP data dictionaries with XML generation fallback'
    )

    # File path arguments
    parser.add_argument('--gen3-csv', help='Path to Gen3 CSV file (filtered)')
    parser.add_argument('--picsure-csv', help='Path to PicSure CSV file')
    parser.add_argument('--output-dir', help='Base output directory')
    
    # Processing arguments
    parser.add_argument('--study', help='Process a single study (accession ID)')
    parser.add_argument('--always-generate', action='store_true', 
                        help='Always use XML generation (skip dbGaP download attempts)')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Create config dictionary
    config = DEFAULT_CONFIG.copy()
    
    # Override with environment variables if set
    if os.environ.get("GEN3_CSV_PATH"):
        config["GEN3_CSV"] = os.environ.get("GEN3_CSV_PATH")
    if os.environ.get("PICSURE_CSV_PATH"):
        config["PICSURE_CSV"] = os.environ.get("PICSURE_CSV_PATH")
    if os.environ.get("OUTPUT_DIR_PATH"):
        config["OUTPUT_DIR"] = os.environ.get("OUTPUT_DIR_PATH")
    
    # Override with command line args if provided
    if args.gen3_csv:
        config["GEN3_CSV"] = args.gen3_csv
    if args.picsure_csv:
        config["PICSURE_CSV"] = args.picsure_csv
    if args.output_dir:
        config["OUTPUT_DIR"] = args.output_dir
    
    # Add processing options
    config["SINGLE_STUDY"] = args.study
    config["ALWAYS_GENERATE"] = args.always_generate
    
    return config

def read_study_Gen3(gen3_metadata_file):
    try:
        logging.info(f"Reading metadata file: {gen3_metadata_file}")
        df = pd.read_csv(gen3_metadata_file)
        df['study_id'] = df['Accession'].str.split('.').str[0]
        logging.info(f"Found {len(df)} studies in metadata file")
        return df.set_index('study_id')
    except Exception as e:
        logging.error(f"Error reading study metadata file: {e}")
        raise

def create_output_directory(base_dir):
    os.makedirs(base_dir, exist_ok=True)
    return base_dir

def write_summary(timestamp_dir, summary_df):
    # Calculate statistics
    total_studies = len(summary_df)
    successful = summary_df[summary_df['status'] == 'SUCCESS']
    downloaded = summary_df[summary_df['method'] == 'dbGaP_download']
    generated = summary_df[summary_df['method'] == 'XML_generator']
    failed = summary_df[summary_df['status'] == 'FAILED']
    
    # Create summary content
    summary_content = [
        "\n" + "="*50,
        "PROCESSING SUMMARY",
        "="*50,
        f"Total studies processed: {total_studies}"
    ]
    
    if total_studies > 0:
        success_rate = len(successful)/total_studies*100 if total_studies > 0 else 0
        download_rate = len(downloaded)/total_studies*100 if total_studies > 0 else 0  
        generated_rate = len(generated)/total_studies*100 if total_studies > 0 else 0
        failure_rate = len(failed)/total_studies*100 if total_studies > 0 else 0
        
        summary_content.extend([
            f"Successfully processed: {len(successful)} ({success_rate:.1f}%)",
            f"  - Downloaded from dbGaP: {len(downloaded)} ({download_rate:.1f}%)",
            f"  - Generated with XML fallback: {len(generated)} ({generated_rate:.1f}%)",
            f"Failed: {len(failed)} ({failure_rate:.1f}%)"
        ])
    
    if not downloaded.empty:
        summary_content.append("\nStudies downloaded from dbGaP:")
        for i, (_, study) in enumerate(downloaded.iterrows(), 1):
            summary_content.append(
                f"  {i}. {study['study_id']} ({study['accession_id']}) - {study['details']}"
            )
    
    if not generated.empty:
        summary_content.append("\nStudies generated using XML generator:")
        for i, (_, study) in enumerate(generated.iterrows(), 1):
            summary_content.append(
                f"  {i}. {study['study_id']} ({study['accession_id']}) - Reason: {study['details']}"
            )
    
    if not failed.empty:
        summary_content.append("\nFailed studies:")
        for i, (_, study) in enumerate(failed.iterrows(), 1):
            summary_content.append(
                f"  {i}. {study['study_id']} ({study['accession_id']}) - Reason: {study['details']}"
            )
    
    # Write to a separate summary file
    summary_txt_path = os.path.join(timestamp_dir, 'processing_summary.txt')
    with open(summary_txt_path, 'w') as f:
        for line in summary_content:
            f.write(line + "\n")
    for line in summary_content:
        logging.info(line)
        
    return summary_txt_path

def get_program_dir(gen3_metadata):
    # Extract program from metadata
    if isinstance(gen3_metadata, pd.Series):
        program = gen3_metadata.get('Program', '')
    else:
        program = gen3_metadata.get('Program', '')
    
    # Clean program name for directory use
    if not program or pd.isna(program):
        program = 'unknown_program'
    else:
        # Handle multi-program case (pipe separated in Gen3)
        if '|' in program:
            program = program.split('|')[0]  # Use first program
        
        # Clean program name for directory use
        program = program.strip().replace(' ', '_').replace('/', '_').lower()
    
    return program

def cleanup_empty_directory(directory):
    try:
        if os.path.exists(directory):
            has_content = False
            for root, dirs, files in os.walk(directory):
                if files:
                    has_content = True
                    break
            
            if not has_content:
                shutil.rmtree(directory)
                logging.info(f"Removed empty directory: {directory}")
    except Exception as e:
        logging.warning(f"Failed to clean up directory {directory}: {str(e)}")

def process_study(study_id, dbgap_id, study_name, output_dir, config, summary_df):
    """Process a single study with download attempt and fallback"""
    process_msg = f"\nProcessing study: {study_id} (dbGaP: {dbgap_id})"
    logging.info(process_msg)
    
    # Get study row from Gen3 metadata to extract program
    try:
        gen3_df = pd.read_csv(config["GEN3_CSV"])
        gen3_df['study_id'] = gen3_df['Accession'].str.split('.').str[0]
        study_row = gen3_df[gen3_df['study_id'] == study_id].iloc[0]
        program_name = get_program_dir(study_row)
        
        # Create program directory
        program_dir = os.path.join(output_dir, program_name)
        os.makedirs(program_dir, exist_ok=True)
        
        # Update study directory to be within program directory
        study_dir = os.path.join(program_dir, dbgap_id)
    except (IndexError, KeyError) as e:
        logging.warning(f"Could not determine program for study {study_id}: {str(e)}")
        logging.warning("Using default output directory")
        program_name = 'unknown_program'
        program_dir = os.path.join(output_dir, program_name)
        os.makedirs(program_dir, exist_ok=True)
        study_dir = os.path.join(program_dir, dbgap_id)
    
    # If always generate is set, skip download attempt
    if config["ALWAYS_GENERATE"]:
        logging.info("Skipping dbGaP download attempt (--always-generate flag set)")
        success = fallback_to_xml_generation(
            study_id, dbgap_id, study_dir, 
            config["PICSURE_CSV"], config["GEN3_CSV"], 
            reason="always_generate"
        )
        
        status = 'SUCCESS' if success else 'FAILED'
        method = 'XML_generator' if success else 'XML_generation_failed'
        details = 'always_generate_flag' if success else 'generation_failed'
        
        # Clean up empty directories if operation failed
        if not success:
            cleanup_empty_directory(study_dir)
            # Also check if program directory is now empty
            cleanup_empty_directory(program_dir)
        
        return pd.DataFrame([{
            'study_id': study_id,
            'accession_id': dbgap_id,
            'status': status,
            'method': method,
            'details': details,
            'program': program_name
        }])
    
    # Try to download from dbGaP
    try:
        logging.info(f"Attempting to download from dbGaP...")
        
        downloaded_vars = download_dbgap_study(dbgap_id, study_dir, study_name)
        
        if downloaded_vars > 0:
            success_msg = f"Successfully downloaded {downloaded_vars} variables for {dbgap_id}"
            logging.info(success_msg)
            
            return pd.DataFrame([{
                'study_id': study_id,
                'accession_id': dbgap_id,
                'status': 'SUCCESS',
                'method': 'dbGaP_download',
                'details': f"{downloaded_vars} variables",
                'program': program_name
            }])
        else:
            logging.info(f"Download returned no variables - study likely not found in dbGaP")
            
            # Fallback to XML generation
            success = fallback_to_xml_generation(
                study_id, dbgap_id, study_dir, 
                config["PICSURE_CSV"], config["GEN3_CSV"], 
                reason="download_empty"
            )
            
            status = 'SUCCESS' if success else 'FAILED'
            method = 'XML_generator' if success else 'both_methods_failed'
            details = 'download_empty' if success else 'generation_failed_after_download_empty'
            
            # Clean up empty directories if both methods failed
            if not success:
                cleanup_empty_directory(study_dir)
                # Also check if program directory is now empty
                cleanup_empty_directory(program_dir)
            
            return pd.DataFrame([{
                'study_id': study_id,
                'accession_id': dbgap_id,
                'status': status,
                'method': method,
                'details': details,
                'program': program_name
            }])
                    
    except Exception as e:
        error_msg = f"Error downloading {dbgap_id}: {str(e)}"
        logging.error(error_msg)
        
        # Check for specific error indicating study not found (if available in API)
        not_found = "not found" in str(e).lower() or "404" in str(e)
        reason = "not_found" if not_found else "download_exception"
        
        # Fallback to XML generation
        success = fallback_to_xml_generation(
            study_id, dbgap_id, study_dir, 
            config["PICSURE_CSV"], config["GEN3_CSV"], 
            reason=reason
        )
        
        status = 'SUCCESS' if success else 'FAILED'
        method = 'XML_generator' if success else 'both_methods_failed'
        details = reason if success else f'generation_failed_after_{reason}'
        
        # Clean up empty directories if both methods failed
        if not success:
            cleanup_empty_directory(study_dir)
            # Also check if program directory is now empty
            cleanup_empty_directory(program_dir)
        
        return pd.DataFrame([{
            'study_id': study_id,
            'accession_id': dbgap_id,
            'status': status,
            'method': method,
            'details': details,
            'program': program_name
        }])

def fallback_to_xml_generation(study_id, dbgap_id, study_dir, picsure_csv, gen3_csv, reason="not_found"):
    logging.info(f"Falling back to XML generation for {dbgap_id} (Reason: {reason})...")
    
    # Clean any partial files if they exist
    if os.path.exists(study_dir):
        for f in os.listdir(study_dir):
            try:
                os.remove(os.path.join(study_dir, f))
            except:
                pass
    else:
        os.makedirs(study_dir, exist_ok=True)
    
    # Use the run_xml_generation function directly
    try:
        # Call XML generation with the specific study and output directory
        success = run_xml_generation(
            study_accession=dbgap_id,
            picsure_csv_path=picsure_csv,
            gen3_csv_path=gen3_csv,
            output_dir=study_dir
        )
        
        # Check success by looking for files
        if os.path.exists(study_dir) and os.listdir(study_dir):
            logging.info(f"Successfully generated XML files for {dbgap_id}")
            return True
        else:
            logging.error(f"XML generation produced no files for {dbgap_id}")
            # Clean up the empty directory immediately
            cleanup_empty_directory(study_dir)
            return False
    except Exception as e:
        logging.error(f"Error during XML generation fallback: {str(e)}")
        # Clean up the directory in case of error
        cleanup_empty_directory(study_dir)
        return False

def run_dbgap_download(config):
    """Run the dbGaP download with XML generation fallback for all studies"""
    
    try:
        # Create base output directory
        os.makedirs(config["OUTPUT_DIR"], exist_ok=True)
        
        # Create output directory for this run
        output_dir = create_output_directory(config["OUTPUT_DIR"])
        
        # Setup logging
        log_file = setup_logging_safe(output_dir)
        
        # Create summary dataframe to track results
        summary_df = pd.DataFrame(columns=[
            'study_id', 'accession_id', 'status', 'method', 'details', 'program'
        ])
        
        # Log initial configuration
        logging.info("Configuration:")
        logging.info(f"  Gen3 CSV (input): {config['GEN3_CSV']}")
        logging.info(f"  PicSure CSV: {config['PICSURE_CSV']}")
        logging.info(f"  Output directory: {output_dir}")
        if config["ALWAYS_GENERATE"]:
            logging.info("  XML Generation Only Mode: Enabled")
        
        # Validate input files
        for file_path in [config["GEN3_CSV"], config["PICSURE_CSV"]]:
            if not os.path.exists(file_path):
                logging.error(f"Input file not found: {file_path}")
                sys.exit(1)
        
        # Read Gen3 CSV file
        try:
            gen3_df = read_study_Gen3(config["GEN3_CSV"])
            logging.info(f"Successfully read Gen3 CSV: {len(gen3_df)} studies")
        except Exception as e:
            logging.error(f"Error reading Gen3 CSV: {e}")
            sys.exit(1)
        
        # Process a single study if specified
        if config["SINGLE_STUDY"]:
            dbgap_id = config["SINGLE_STUDY"]
            logging.info(f"Single study mode: {dbgap_id}")
            
            # Find the study in Gen3 data
            study_base_id = dbgap_id.split('.')[0]
            study_found = False
            
            for study_id, row in gen3_df.iterrows():
                if row[config["ACCESSION_FIELD"]] == dbgap_id or study_id == study_base_id:
                    study_found = True
                    study_name = row.get(config["STUDY_NAME_FIELD"], "")
                    
                    # Get program name and create directory structure
                    program_name = get_program_dir(row)
                    program_dir = os.path.join(output_dir, program_name)
                    os.makedirs(program_dir, exist_ok=True)
                    study_dir = os.path.join(program_dir, dbgap_id)
                    
                    # Process the single study
                    result_df = process_study(
                        study_id, dbgap_id, study_name, output_dir, config, summary_df
                    )
                    summary_df = pd.concat([summary_df, result_df], ignore_index=True)
                    break
            
            if not study_found:
                logging.error(f"Study {dbgap_id} not found in Gen3 metadata")
                sys.exit(1)
        else:
            # No need to modify this section since process_study handles program directory creation
            # Process each study in Gen3 file
            for study_id, row in gen3_df.iterrows():
                study_num = list(gen3_df.index).index(study_id) + 1
                
                # Get dbGaP accession ID and study name
                if config["ACCESSION_FIELD"] not in row or pd.isna(row[config["ACCESSION_FIELD"]]):
                    logging.warning(f"No dbGaP accession ID found for study {study_id}")
                    continue
                    
                dbgap_id = str(row[config["ACCESSION_FIELD"]]).strip()
                study_name = str(row.get(config["STUDY_NAME_FIELD"], "")).strip() if config["STUDY_NAME_FIELD"] in row else None
                
                logging.info(f"Processing study {study_num}/{len(gen3_df)}: {study_id} (dbGaP: {dbgap_id})")
                
                # Process the study with program directory organization
                result_df = process_study(
                    study_id, dbgap_id, study_name, output_dir, config, summary_df
                )
                summary_df = pd.concat([summary_df, result_df], ignore_index=True)
        
        # Save summary to CSV
        summary_path = os.path.join(output_dir, 'processing_summary.csv')
        summary_df.to_csv(summary_path, index=False)
        
        # Write detailed summary to log and separate file
        summary_txt = write_summary(output_dir, summary_df)
        
        final_msg = f"\nProcessing complete! Summary saved to: {summary_path} and {summary_txt}"
        logging.info(final_msg)
        logging.info(f"Log file: {log_file}")
        logging.info(f"All files saved to: {output_dir}")
        
        return True
            
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    config = setup_config()
    run_dbgap_download(config)