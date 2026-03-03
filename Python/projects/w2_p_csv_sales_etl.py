# Project 2: CSV Sales ETL
# File Input sales_raw.csv
"""
id,date,price,quantity,item
1,2026-03-03,100.50,2,Laptop
2,2026-03-03,invalid,1,Mouse
3,2026-03-03,50.00,4,Keyboard
4,2026-03-04,200.00,1,Monitor
5,2026-03-04,,5,Cables
6,2026-03-04,10.00,10,USB-Hub
"""
import csv
import logging

# --- Configuration & Logging ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def sales_revenue_etl(input_file: str, output_file: str):
    # Dictionary to store Date: Total_Revenue (This is our aggregation bucket)
    daily_revenue = {}
    total_records = 0
    valid_records = 0

    try:
        logging.info(f"Processing sales data from {input_file}...")
        
        with open(input_file, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                total_records += 1
                try:
                    # 1. Transform: Convert price and quantity to numbers
                    # We use float() for price and int() for quantity
                    price = float(row['price'])
                    quantity = int(row['quantity'])
                    
                    # 2. Transform: Calculate Total Revenue for this row
                    line_total = price * quantity
                    date = row['date']
                    
                    # 3. Aggregate: Add to the specific date's total
                    # If date exists, add to it; if not, start at 0
                    daily_revenue[date] = daily_revenue.get(date, 0.0) + line_total
                    valid_records += 1
                    
                except (ValueError, TypeError):
                    # This catches 'invalid' strings or empty fields
                    logging.warning(f"Skipping Record ID {row['id']}: Invalid price or quantity.")

        # --- Load: Write the Summary to a new CSV ---
        with open(output_file, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Date', 'Total_Daily_Revenue'])
            
            # Write aggregated results sorted by date
            for date in sorted(daily_revenue.keys()):
                writer.writerow([date, round(daily_revenue[date], 2)])

        logging.info(f"✅ ETL Complete. {valid_records}/{total_records} records processed.")
        print(f"\nReport generated: {output_file}")

    except FileNotFoundError:
        logging.error(f"Error: The file {input_file} does not exist.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    sales_revenue_etl('sales_raw.csv', 'revenue_summary.csv')
