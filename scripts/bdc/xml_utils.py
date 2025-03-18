#!/usr/bin/env python3
"""
Utility functions for handling NaN values in XML generation process.
With improved logging and study ID handling.
"""

import logging
import os
import pandas as pd
import numpy as np
import sys
from xml_generator import process_study
from xml_generator import read_gen3_metadata

def fix_nan_values(df):
    """Replace NaN values in DataFrame with appropriate empty values"""
    # Make a copy to avoid modifying the original
    df_fixed = df.copy()
    
    # Replace NaN with empty string for object/string columns
    for col in df_fixed.select_dtypes(include=['object']).columns:
        df_fixed[col] = df_fixed[col].fillna('')
    
    # Replace NaN with appropriate values for numeric columns 
    for col in df_fixed.select_dtypes(include=['number']).columns:
        df_fixed[col] = df_fixed[col].fillna(0)
    
    # Handle specific columns that might cause issues
    if 'derived_group_name' in df_fixed.columns:
        df_fixed['derived_group_name'] = df_fixed['derived_group_name'].fillna('unnamed_group')
    
    if 'dtId' in df_fixed.columns:
        df_fixed['dtId'] = df_fixed['dtId'].fillna('default_table')
        
    if 'values' in df_fixed.columns:
        df_fixed['values'] = df_fixed['values'].fillna('[]')
        
    return df_fixed

def fix_series_nan(series):
    """Replace NaN values in a pandas Series with empty strings"""
    if isinstance(series, pd.Series):
        return series.fillna('').to_dict()
    return series

def process_study_safe(study_id, study_df, study_Gen3, output_dir):
    """Process a single study with NaN value handling"""
    try:
        # Extract base study ID if it contains version
        base_study_id = study_id.split('.')[0] if '.' in study_id else study_id
        
        logging.info(f"Processing study {base_study_id} with NaN handling")
        _log_to_file(f"Processing study {base_study_id} with NaN handling")
        
        # Fix NaN values in DataFrames
        study_df_fixed = fix_nan_values(study_df)
        
        # Fix NaN values in Gen3 data
        study_Gen3_fixed = fix_series_nan(study_Gen3)
        
        # Create study directory
        study_dir = os.path.join(output_dir, base_study_id)
        os.makedirs(study_dir, exist_ok=True)
        
        # Import process_study here to avoid circular imports

        result = process_study(base_study_id, study_df_fixed, study_Gen3_fixed, study_dir)
        
        if result:
            logging.info(f"Successfully processed study {base_study_id}")
            _log_to_file(f"Successfully processed study {base_study_id}")
        else:
            logging.error(f"Failed to process study {base_study_id}")
            _log_to_file(f"Failed to process study {base_study_id}")
            
        return result
        
    except Exception as e:
        error_msg = f"Error processing study {study_id}: {str(e)}"
        logging.error(error_msg)
        _log_to_file(error_msg)
        return False

def generate_xml_for_study_safe(accession_id, picsure_csv, gen3_csv, output_dir):
    """
    Generate XML files with NaN handling.
    Ensures proper study ID format (phs000993 format).
    """
    try:
        # Extract base study ID without version (e.g., phs000993 from phs000993.v3.p2)
        study_id = accession_id.split('.')[0]
        
        logging.info(f"Generating XML files for study {study_id} using local data")
        _log_to_file(f"Generating XML files for study {study_id} using local data")
        
        # Read PicSure data for this study
        picsure_df = pd.read_csv(picsure_csv)
        study_df = picsure_df[picsure_df['studyId'] == study_id].copy()
        
        if study_df.empty:
            warning_msg = f"No PicSure data found for study {study_id}"
            logging.warning(warning_msg)
            _log_to_file(warning_msg)
            return False
            
        # Read Gen3 data
        gen3_full = read_gen3_metadata(gen3_csv)
        if study_id not in gen3_full.index:
            warning_msg = f"No Gen3 data found for study {study_id}"
            logging.warning(warning_msg) 
            _log_to_file(warning_msg)
            return False
            
        study_meta = gen3_full.loc[study_id].copy()
        
        # Process the study with NaN handling
        # Pass only the base study ID to ensure proper format
        return process_study_safe(study_id, study_df, study_meta, output_dir)
        
    except Exception as e:
        error_msg = f"Error generating XML for study {accession_id}: {str(e)}"
        logging.error(error_msg)
        _log_to_file(error_msg)
        return False

def _log_to_file(message):
    """
    Ensures a message is written to the log file regardless of handler setup
    This serves as a backup to make sure something is written to the log file
    """
    try:
        # Find existing log file
        log_file = None
        for handler in logging.root.handlers:
            if isinstance(handler, logging.FileHandler):
                log_file = handler.baseFilename
                break
                
        if log_file:
            # Write directly to log file as backup
            with open(log_file, 'a') as f:
                timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"{timestamp} - {message}\n")
    except:
        pass  # Silent failure if backup logging fails

def setup_logging_safe(timestamp_dir):
    """
    Setup logging configuration with guaranteed file output.
    Includes direct file writes to ensure log content.
    """
    # First remove any existing handlers to avoid duplicates
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Create log file path within timestamp directory
    log_filename = os.path.join(timestamp_dir, 'process.log')
    
    # Create handlers
    file_handler = logging.FileHandler(log_filename, mode='w')
    console_handler = logging.StreamHandler(sys.stdout)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to root logger
    logging.root.addHandler(file_handler)
    logging.root.addHandler(console_handler)
    logging.root.setLevel(logging.INFO)
    
    # Verify log file is writable
    test_message = f"Starting processing with log file: {log_filename}"
    logging.info(test_message)
    
    # Write directly to file as well to ensure content
    with open(log_filename, 'a') as f:
        f.write(f"{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO - {test_message}\n")
    
    return log_filename