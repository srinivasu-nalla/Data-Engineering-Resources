# File Input api_response.json
"""
[
  {
    "id": 101,
    "user": {
      "name": "Alice Johnson",
      "contact": {"email": "alice@example.com", "phone": "555-0101"},
      "meta": {"city": "Hyderabad", "signup_date": "2026-01-15"}
    }
  },
  {
    "id": 102,
    "user": {
      "name": "Bob Smith",
      "contact": {"email": "bob@example.com", "phone": null},
      "meta": null
    }
  },
  {
    "id": 103,
    "user": {
      "name": "Charlie Brown",
      "contact": {"email": "charlie@example.com"},
      "meta": {"city": "Bangalore"}
    }
  }
]
"""
import json
import csv
import logging

# --- Configuration & Logging ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def process_api_data(input_json: str, output_csv: str):
    flattened_data = []

    try:
        logging.info(f"Reading API response from {input_json}...")
        
        with open(input_json, 'r') as f:
            raw_data = json.load(f)

        for entry in raw_data:
            # 1. Access the main 'user' block safely
            user = entry.get('user', {})
            
            # 2. Extract nested fields with safe defaults
            # Use .get() and 'or {}' to handle cases where a key might be None/null
            contact = user.get('contact') or {}
            meta = user.get('meta') or {}

            # 3. Build the Flat Record (The Transformation)
            flat_record = {
                "user_id": entry.get('id'),
                "full_name": user.get('name', 'N/A'),
                "email": contact.get('email', 'N/A'),
                "city": meta.get('city', 'Remote'),
                "signup_date": meta.get('signup_date', '2026-01-01')
            }
            
            flattened_data.append(flat_record)

        # --- Load: Save to CSV for the Analytics Team ---
        if flattened_data:
            headers = flattened_data[0].keys()
            with open(output_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(flattened_data)
            
            logging.info(f"✅ Successfully flattened {len(flattened_data)} records to {output_csv}")
        
    except json.JSONDecodeError:
        logging.error("Failed to parse JSON. File might be corrupted.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    process_api_data('api_response.json', 'flattened_users.csv')
