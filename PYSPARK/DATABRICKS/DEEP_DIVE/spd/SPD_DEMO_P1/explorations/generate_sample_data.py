# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # E-Commerce Sample Data Generator
# MAGIC
# MAGIC This notebook generates sample data for the Lakeflow Spark Declarative Pipeline.
# MAGIC
# MAGIC **Output:**
# MAGIC - Catalog: `db_spd_lab`
# MAGIC - Schema: `db_spd`
# MAGIC - Volume: `raw_volume`
# MAGIC - Files: `/Volumes/db_spd_lab/db_spd/raw_volume/orders/orders_1.json` and `orders_2.json`
# MAGIC
# MAGIC **Run this notebook once before starting the pipeline.**

# COMMAND ----------

import json
from datetime import datetime, timedelta
import random

# Step 1: Create catalog, schema, and volume if they don't exist
spark.sql("""
  CREATE CATALOG IF NOT EXISTS db_spd_lab
""")

spark.sql("""
  CREATE SCHEMA IF NOT EXISTS db_spd_lab.db_spd
""")

spark.sql("""
  CREATE VOLUME IF NOT EXISTS db_spd_lab.db_spd.raw_volume
""")

print("✅ Created catalog, schema, and volume")

# COMMAND ----------

# Step 2: Create directory for orders
volume_path = "/Volumes/db_spd_lab/db_spd/raw_volume/orders/"
dbutils.fs.mkdirs(volume_path)

# Step 3: Generate 100 sample e-commerce orders
def generate_orders(num_orders):
    orders = []
    statuses = ['completed', 'pending', 'cancelled', 'processing']
    base_date = datetime(2024, 1, 1)
    
    for i in range(1, num_orders + 1):
        order_date = (base_date + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
        
        # Make 5% of amounts null for data quality testing
        if random.random() < 0.05:
            amount = None
        else:
            amount = round(random.uniform(10.0, 1000.0), 2)
        
        order = {
            "order_id": f"ORD-{i:05d}",
            "customer_id": f"CUST-{random.randint(1, 50):03d}",
            "amount": amount,
            "order_date": order_date,
            "status": random.choice(statuses)
        }
        orders.append(order)
    
    return orders

# Generate orders
all_orders = generate_orders(100)

print(f"✅ Generated {len(all_orders)} orders")

# COMMAND ----------

# Step 4: Split into 2 files and save as JSON
orders_1 = all_orders[:50]
orders_2 = all_orders[50:]

# Write orders_1.json
orders_1_json = '\n'.join([json.dumps(order) for order in orders_1])
dbutils.fs.put(volume_path + 'orders_1.json', orders_1_json, overwrite=True)

# Write orders_2.json
orders_2_json = '\n'.join([json.dumps(order) for order in orders_2])
dbutils.fs.put(volume_path + 'orders_2.json', orders_2_json, overwrite=True)

print(f"✅ Generated 100 orders across 2 files in {volume_path}")
print(f"   - orders_1.json: {len(orders_1)} records")
print(f"   - orders_2.json: {len(orders_2)} records")
print(f"   - ~5% of records have null amounts for data quality testing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC
# MAGIC Check that files were created successfully:

# COMMAND ----------

# List files in the volume
files = dbutils.fs.ls(volume_path)
for file in files:
    print(f"📄 {file.name} - {file.size} bytes")

# COMMAND ----------

# Preview first few records from orders_1.json
sample = spark.read.json(volume_path + "orders_1.json")
display(sample.limit(10))