# File Input server_logs.txt 
"""
2026-03-03 10:15:22 | ERROR | Connection Timeout
2026-03-03 10:45:10 | INFO | User 101 Logged In
2026-03-03 11:20:05 | ERROR | 404 Page Not Found
2026-03-03 11:55:30 | ERROR | Database Unreachable
2026-03-04 09:10:12 | ERROR | Authentication Failure
2026-03-04 09:45:00 | INFO | System Health Check
2026-03-04 14:30:45 | ERROR | Memory Limit Exceeded
"""
import csv
import logging
from collections import Counter
from datetime import datetime

# --- Configuration & Logging ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def analyze_server_logs(input_file: str):
    daily_errors = Counter()
    hourly_errors = Counter()
    
    try:
        logging.info(f"Starting analysis of {input_file}...")
        
        with open(input_file, 'r') as f:
            for line_num, line in enumerate(f, 1):
                # We only care about ERROR logs
                if "ERROR" in line:
                    try:
                        # Split line: Date | Level | Message
                        parts = [p.strip() for p in line.split('|')]
                        
                        # Extract timestamp and convert to datetime object
                        timestamp = datetime.strptime(parts[0], "%Y-%m-%d %H:%M:%S")
                        
                        # . Count errors per day
                        daily_errors[timestamp.date()] += 1
                        
                        # . Track errors by hour to find the peak
                        hourly_errors[timestamp.hour] += 1
                        
                    except (ValueError, IndexError) as e:
                        logging.warning(f"Skipping malformed line {line_num}: {e}")

        # --- Save Summary to CSV ---
        output_file = 'error_summary_report.csv'
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Date', 'Total_Errors'])
            for date, count in sorted(daily_errors.items()):
                writer.writerow([date, count])
        
        # --- Identify Peak Error Hour ---
        if hourly_errors:
            peak_hour, peak_count = hourly_errors.most_common(1)[0]
            logging.info(f"✅ Success! Report saved to {output_file}")
            print(f"\n--- 📊 LOG ANALYSIS SUMMARY ---")
            print(f"Total Errors Found: {sum(daily_errors.values())}")
            print(f"Peak Error Window: {peak_hour}:00 - {peak_hour}:59 ({peak_count} errors)")
        else:
            logging.warning("No error logs were found in the file.")

    except FileNotFoundError:
        logging.critical(f"Input file '{input_file}' not found.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    analyze_server_logs('server_logs.txt')
