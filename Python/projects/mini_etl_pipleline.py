"""
Mini ETL Script
Build a small pipeline:
. Read CSV
. Clean data
. Transform
. Write output file
. Add logging"""

import csv
import logging
import os
from datetime import datetime

# --- CONFIGURATION & LOGGING ---
logging.basicConfig(
    filename='ETLpipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- ETL FUNCTIONS ---

def extract(file_path):
    """EXTRACT: Read raw CSV data."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file {file_path} is missing!")
    
    with open(file_path, mode='r', encoding='utf-8') as f:
        # returns a list of dictionaries
        return list(csv.DictReader(f))

def transform(raw_data):
    """TRANSFORM: Clean dates, handle missing values, and calculate tax."""
    processed_records = []
    skipped_count = 0

    for row in raw_data:
        try:
            # 1. Handle Missing Values & Type Conversion for Amount
            # We use a try-except here specifically for numeric conversion
            raw_amount = row.get('amount', '')
            amount = float(raw_amount)
            
            # 2. Date Transformation
            # Challenge: Convert string to datetime object
            raw_date = row.get('date', '')
            clean_date = datetime.strptime(raw_date, "%Y-%m-%d")
            
            # 3. Business Logic: Calculate 18% GST/Tax
            row['total_with_tax'] = round(amount * 1.18, 2)
            row['date'] = clean_date.date() # Convert to simple date for CSV
            
            processed_records.append(row)

        except (ValueError, TypeError, KeyError) as e:
            logging.warning(f"SKIPPING ROW ID {row.get('id', 'Unknown')}: Error {e}")
            skipped_count += 1
            continue

    logging.info(f"Transformation complete. Success: {len(processed_records)}, Skipped: {skipped_count}")
    return processed_records

def load(data, output_path):
    """LOAD: Write the cleaned data to a new CSV file."""
    if not data:
        logging.error("No data available to load.")
        return

    # Extract headers from the first dictionary in the list
    headers = data[0].keys()

    with open(output_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)

# --- MAIN CONTROLLER ---

def run_pipeline():
    INPUT = "raw_sales.csv"
    OUTPUT = "clean_sales.csv"

    try:
        logging.info("--- Starting Mini ETL Job ---")
        
        # 1. Extract
        raw_rows = extract(INPUT)
        
        # 2. Transform
        clean_rows = transform(raw_rows)
        
        # 3. Load
        load(clean_rows, OUTPUT)
        
        logging.info(f"--- ETL Job Finished Successfully! Output: {OUTPUT} ---")

    except Exception as e:
        logging.critical(f"PIPELINE FAILED: {e}")

if __name__ == "__main__":
    run_pipeline()
