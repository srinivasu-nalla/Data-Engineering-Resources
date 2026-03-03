# File Input source_db.csv
"""
id,data,updated_at
101,Login Event,2026-03-01 10:00:00
102,Purchase,2026-03-01 12:30:00
103,Logout,2026-03-02 08:15:00
104,Update Profile,2026-03-03 14:00:00
105,New Post,2026-03-03 16:45:00
"""
import csv
import os
import logging
from datetime import datetime

# --- Configuration ---
SOURCE_FILE = 'source_db.csv'
STATE_FILE = 'last_run.txt'
OUTPUT_FILE = 'incremental_sync.csv'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def get_last_run_timestamp():
    """Reads the 'Watermark' from a file. If not found, starts from the beginning of time."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return datetime.strptime(f.read().strip(), "%Y-%m-%d %H:%M:%S")
    # Default to a very old date if this is the first run
    return datetime(1900, 1, 1)

def update_last_run_timestamp(new_timestamp):
    """Saves the new 'Watermark' for the next run."""
    with open(STATE_FILE, 'w') as f:
        f.write(new_timestamp.strftime("%Y-%m-%d %H:%M:%S"))

def run_incremental_etl():
    last_run_time = get_last_run_timestamp()
    logging.info(f"Last successful run was at: {last_run_time}")

    new_records = []
    latest_timestamp_found = last_run_time

    try:
        with open(SOURCE_FILE, mode='r') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                # Convert string to datetime for comparison
                current_row_time = datetime.strptime(row['updated_at'], "%Y-%m-%d %H:%M:%S")
                
                # --- The Incremental Logic ---
                if current_row_time > last_run_time:
                    logging.info(f"New record found: ID {row['id']}")
                    new_records.append(row)
                    
                    # Track the latest timestamp in this batch to update the watermark
                    if current_row_time > latest_timestamp_found:
                        latest_timestamp_found = current_row_time

        # --- Load: Save only the new rows ---
        if new_records:
            # Append mode ('a') is used because we are adding to our history
            file_exists = os.path.isfile(OUTPUT_FILE)
            with open(OUTPUT_FILE, mode='a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=new_records[0].keys())
                if not file_exists:
                    writer.writeheader()
                writer.writerows(new_records)
            
            # Update the state file so we don't process these again
            update_last_run_timestamp(latest_timestamp_found)
            logging.info(f"✅ Sync complete. {len(new_records)} new records added.")
        else:
            logging.info("⏸️ No new data since last run.")

    except Exception as e:
        logging.error(f"ETL Failed: {e}")

if __name__ == "__main__":
    run_incremental_etl()
