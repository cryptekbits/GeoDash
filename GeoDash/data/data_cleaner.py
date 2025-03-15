"""
CSV cleaning utility for GeoDash data imports.
"""
import csv
from pathlib import Path
from datetime import datetime

def clean_city_data(input_path: str, output_path: str = None, error_path: str = None) -> tuple:
    """
    Clean city data CSV by removing entries without country_code,
    while saving the removed entries to an error file.
    
    Args:
        input_path: Path to input CSV file
        output_path: Path for cleaned CSV (default: input_path with _cleaned suffix)
        error_path: Path for error CSV (default: input_path with _errors suffix)
    
    Returns:
        Tuple of (valid_count, invalid_count, error_file_path)
    """
    input_file = Path(input_path)
    
    # Set default output paths if not provided
    if not output_path:
        output_path = input_file.with_stem(input_file.stem + "_cleaned").as_posix()
    if not error_path:
        error_path = input_file.with_stem(input_file.stem + "_errors").as_posix()
    
    valid_count = 0
    invalid_count = 0
    
    with open(input_path, 'r', encoding='utf-8') as f_in, \
         open(output_path, 'w', newline='', encoding='utf-8') as f_out, \
         open(error_path, 'w', newline='', encoding='utf-8') as f_err:
        
        reader = csv.DictReader(f_in)
        writer_valid = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
        writer_error = csv.DictWriter(f_err, fieldnames=reader.fieldnames + ['error_reason'])
        
        writer_valid.writeheader()
        writer_error.writeheader()
        
        for row in reader:
            # Check for missing country_code
            if not row.get('country_code') or row['country_code'].strip() == '':
                writer_error.writerow({
                    **row,
                    'error_reason': 'missing_country_code'
                })
                invalid_count += 1
            else:
                writer_valid.writerow(row)
                valid_count += 1
    
    # Add metadata to error file
    with open(error_path, 'a', encoding='utf-8') as f_err:
        f_err.write(f"\n# Metadata\n")
        f_err.write(f"# Cleaned: {datetime.utcnow().isoformat()}\n")
        f_err.write(f"# Original file: {input_path}\n")
        f_err.write(f"# Total entries processed: {valid_count + invalid_count}\n")
        f_err.write(f"# Valid entries: {valid_count}\n")
        f_err.write(f"# Invalid entries: {invalid_count}\n")
    
    return valid_count, invalid_count, error_path 