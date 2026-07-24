-- BRONZE LAYER: Ingest raw JSON files using Auto Loader
CREATE OR REFRESH STREAMING TABLE db_spd_lab.db_spd.bronze_orders
COMMENT "Bronze raw orders via Auto Loader"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * 
FROM STREAM(read_files(
  '/Volumes/db_spd_lab/db_spd/raw_volume/orders/',
  format => 'json',
  rescuedDataColumn => '_rescued_data',
  inferColumnTypes => true
));

-- SILVER LAYER: Cleaned data with data quality constraints
CREATE OR REFRESH STREAMING TABLE db_spd_lab.db_spd.silver_orders
(
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
)
COMMENT "Silver cleaned orders with data quality checks"
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
  *,
  current_timestamp() as processed_at
FROM STREAM(db_spd_lab.db_spd.bronze_orders)
WHERE _rescued_data IS NULL;

-- GOLD LAYER: Daily sales aggregation for BI
CREATE OR REFRESH MATERIALIZED VIEW db_spd_lab.db_spd.gold_daily_sales
COMMENT "Gold daily sales aggregation for BI dashboards"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
  order_date,
  status,
  SUM(amount) as total_sales,
  COUNT(*) as total_orders
FROM db_spd_lab.db_spd.silver_orders
GROUP BY order_date, status;