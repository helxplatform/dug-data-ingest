import logging
import os, pandas as pd, sys

def setup_logging_safe(timestamp_dir):
    """
    Setup logging configuration.
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