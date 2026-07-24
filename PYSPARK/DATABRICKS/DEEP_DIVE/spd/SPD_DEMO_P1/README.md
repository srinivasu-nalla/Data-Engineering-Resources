# Orders Data Flow - Production Spark Declarative Pipeline

## Overview

This pipeline implements a production-grade medallion architecture (Bronze → Silver → Gold) for e-commerce orders data processing using Databricks Lakeflow Spark Declarative Pipelines (SDP). It demonstrates enterprise best practices for data engineering, including robust data quality checks, schema evolution, audit trails, and performance optimization.

## Architecture

### Medallion Layers

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────────┐
│   BRONZE    │────▶│   SILVER    │────▶│       GOLD          │
│  Raw Data   │     │  Clean Data │     │  Business Metrics   │
└─────────────┘     └─────────────┘     └─────────────────────┘
     │                    │                      │
     │                    │                      ├─ daily_sales_summary
     │                    │                      └─ customer_analytics
     │                    │
     │                    ├─ orders_cleaned
     │                    └─ order_items_cleaned
     │
     ├─ bronze_orders_raw
     └─ bronze_order_items_raw
```

### Data Flow

1. **Bronze Layer** (Ingestion)
   - Auto Loader reads JSON files from cloud storage/volumes
   - Schema inference with evolution enabled
   - Rescued data column captures unparseable records
   - Audit columns track ingestion metadata
   - No data quality filters - capture everything

2. **Silver Layer** (Cleansing & Enrichment)
   - Type conversions and data standardization
   - Strict data quality validation with `expect_or_drop`
   - Deduplication on primary keys
   - Business rule enforcement
   - Partitioned for query performance

3. **Gold Layer** (Analytics)
   - Pre-aggregated business metrics
   - Materialized views for fast query access
   - Daily sales summaries and customer analytics
   - Optimized for BI tool consumption

## File Structure

```
/POC_DEEP_DIVE/DLT_DEMO_P2/
├── README.md                          # This file
├── transformations/
│   └── orders_data_flow.py           # Main pipeline definition
└── data/
    ├── raw_orders/                   # Source: orders JSON files
    └── raw_order_items/              # Source: order items JSON files
```

## Data Quality Framework

### Bronze Layer DQ
- **No filtering** - capture all data including malformed records
- Rescued data column (`_rescued_data`) preserves unparseable fields
- Schema hints guide type inference
- Schema evolution adapts to new fields automatically

### Silver Layer DQ

#### orders_cleaned
| Rule | Type | Description |
|------|------|-------------|
| valid_order_id | DROP | Order ID must be positive integer |
| valid_customer_id | DROP | Customer ID must be positive integer |
| valid_total_amount | DROP | Total amount must be positive |
| valid_order_status | DROP | Status must be in allowed list |
| valid_order_date | DROP | Order date must be valid timestamp |
| reasonable_order_date | WARN | Flag orders from future or >5 years old |
| reasonable_total_amount | WARN | Flag orders <$1 or >$100K |

#### order_items_cleaned
| Rule | Type | Description |
|------|------|-------------|
| valid_order_item_id | DROP | Order item ID must be positive |
| valid_order_id_fk | DROP | Must reference valid order |
| valid_product_id | DROP | Product ID must be positive |
| valid_quantity | DROP | Quantity must be positive |
| valid_unit_price | DROP | Unit price must be positive |
| reasonable_quantity | WARN | Flag quantities >1000 |
| reasonable_unit_price | WARN | Flag prices >$10K |

### Gold Layer DQ
- No filtering - aggregations are trusted
- Completeness checks ensure all dates represented
- Consistency validations on calculated fields

## Setup Instructions

### Prerequisites

1. **Unity Catalog Setup**
   - Catalog created (configured in pipeline settings)
   - Schema created (configured in pipeline settings)
   - Appropriate permissions (CREATE TABLE, MODIFY, SELECT)

2. **Source Data**
   - Orders JSON files in `/Volumes/{catalog}/{schema}/raw_data/orders/`
   - Order items JSON files in `/Volumes/{catalog}/{schema}/raw_data/order_items/`

3. **Compute**
   - Databricks Runtime 14.3 LTS or higher recommended
   - Photon enabled for optimal performance
   - Serverless recommended for production

### Pipeline Configuration

1. **Navigate to Pipeline Settings**
   - Open pipeline `DLT_DEMO_P2` in Databricks UI

2. **Configure Target Catalog/Schema**
   ```yaml
   catalog: <your_catalog>
   schema: <your_schema>
   ```

3. **Update Source Paths**
   - Edit `orders_data_flow.py`
   - Update `ORDERS_SOURCE_PATH` and `ORDER_ITEMS_SOURCE_PATH` constants
   ```python
   ORDERS_SOURCE_PATH = "/Volumes/<catalog>/<schema>/raw_data/orders"
   ORDER_ITEMS_SOURCE_PATH = "/Volumes/<catalog>/<schema>/raw_data/order_items"
   ```

4. **Pipeline Settings**
   ```yaml
   name: DLT_DEMO_P2
   photon: true
   serverless: true  # Recommended for production
   channel: CURRENT
   ```

### First-Time Deployment

1. **Validate Configuration**
   - Start a dry run to validate syntax and dependencies
   - Check for permission issues or missing sources

2. **Initial Load**
   - Run a full refresh to process all historical data
   - Monitor progress in the pipeline UI
   - Review data quality metrics

3. **Verify Outputs**
   ```sql
   -- Check record counts
   SELECT 'bronze_orders' AS layer, COUNT(*) AS cnt FROM bronze_orders_raw
   UNION ALL
   SELECT 'silver_orders', COUNT(*) FROM orders_cleaned
   UNION ALL
   SELECT 'gold_daily_sales', COUNT(*) FROM daily_sales_summary;
   
   -- Check data quality metrics
   SELECT * FROM event_log
   WHERE details:flow_progress.metrics.num_output_rows > 0;
   ```

## Operational Guide

### Running the Pipeline

#### Development/Testing
```bash
# Dry run (validate only, no data processing)
databricks pipelines run-dry <pipeline-id>

# Regular update (incremental processing)
databricks pipelines update <pipeline-id>

# Full refresh (reprocess all data from scratch)
databricks pipelines update <pipeline-id> --full-refresh
```

#### Production Schedule
Recommended: Configure continuous mode for real-time processing
```yaml
continuous: true
```

Or schedule via Databricks Jobs:
- **Incremental updates**: Every 15 minutes or hourly
- **Full refresh**: Weekly during maintenance window

### Monitoring

#### Key Metrics to Track

1. **Pipeline Health**
   - Update success rate
   - Average update duration
   - Records processed per layer
   - Data quality expectation violations

2. **Data Quality**
   ```sql
   -- Query expectation metrics from event log
   SELECT 
     details:flow_progress.metrics.num_output_rows AS output_rows,
     details:flow_progress.data_quality.expectations
   FROM event_log
   WHERE event_type = 'flow_progress'
     AND origin.flow_name IN ('orders_cleaned', 'order_items_cleaned');
   ```

3. **Performance**
   - Processing latency (end-to-end)
   - Records per second throughput
   - Auto Loader file discovery time
   - Cluster utilization

#### Alerts

Set up alerts for:
- Pipeline failures
- Data quality violations exceeding threshold (e.g., >5% drop rate)
- Unexpected delays in data arrival
- Schema evolution events

### Troubleshooting

#### Common Issues

**Issue: Pipeline fails with "Path does not exist"**
- **Cause**: Source paths not configured or volumes not mounted
- **Fix**: Verify `ORDERS_SOURCE_PATH` and `ORDER_ITEMS_SOURCE_PATH` exist
- **Check**: `dbutils.fs.ls("/Volumes/<catalog>/<schema>/raw_data/orders")`

**Issue: High data quality violation rate**
- **Cause**: Source data quality degradation
- **Fix**: Review rescued data column and expectation metrics
- **Query**:
  ```sql
  SELECT _rescued_data, COUNT(*) AS cnt
  FROM bronze_orders_raw
  WHERE _rescued_data IS NOT NULL
  GROUP BY _rescued_data;
  ```

**Issue: Schema evolution breaks downstream queries**
- **Cause**: New fields added without explicit schema management
- **Fix**: Add schema hints for critical fields in Auto Loader
- **Best Practice**: Use `cloudFiles.inferColumnTypes` carefully

**Issue: Slow Gold layer aggregations**
- **Cause**: Full table scans without partitioning/clustering
- **Fix**: Already partitioned by date - verify partition pruning
- **Query**:
  ```sql
  DESCRIBE DETAIL daily_sales_summary;
  ```

**Issue: "UNRESOLVED_COLUMN" errors**
- **Cause**: Upstream schema doesn't contain expected columns
- **Fix**: Check bronze layer schema hints and source data
- **Verify**: Inspect first few records from source files

## Performance Tuning

### Optimization Techniques Implemented

1. **Auto Loader Configuration**
   - `cloudFiles.useNotifications`: False (file listing for small datasets)
   - `cloudFiles.schemaEvolutionMode`: "addNewColumns" (adapt to new fields)
   - `cloudFiles.inferColumnTypes`: True (intelligent type inference)

2. **Partitioning Strategy**
   - Silver layer: Partitioned by `order_date` (date granularity)
   - Gold layer: Partitioned by `order_date` for time-based queries
   - Enables partition pruning for date range queries

3. **Streaming State Management**
   - Stateless operations where possible
   - Deduplication on bounded windows (implicit state management)

4. **Materialized Views**
   - Gold layer uses MVs for pre-computed aggregations
   - Automatic incremental refresh on serverless
   - Fast query response for BI tools

### Performance Tuning Recommendations

1. **For Large Volumes (>1TB)**
   - Enable Z-ordering on frequently filtered columns
   ```python
   table_properties={
       "delta.dataSkippingNumIndexedCols": "10"
   }
   ```
   - Run OPTIMIZE regularly on Silver/Gold tables

2. **For High Throughput Streaming**
   - Use Kafka/Event Hubs instead of file-based Auto Loader
   - Enable Photon for 3-5x faster processing
   - Increase cluster size or switch to serverless

3. **For Complex Joins**
   - Consider broadcast hints for small dimension tables
   - Pre-aggregate before joining where possible
   - Use streaming-to-static joins carefully

4. **For Cost Optimization**
   - Use serverless for variable workloads
   - Enable auto-scaling for classic clusters
   - Set `pipelines.trigger.interval` for batch windows

## Data Quality Metrics

### Accessing DQ Results

Expectation results are stored in the pipeline event log:

```sql
-- Summary of all expectations
SELECT 
  origin.flow_name,
  details:flow_progress.data_quality.expectations,
  details:flow_progress.metrics.num_output_rows
FROM event_log
WHERE event_type = 'flow_progress'
  AND timestamp > current_timestamp() - INTERVAL 1 DAY
ORDER BY timestamp DESC;

-- Detailed expectation violations
SELECT 
  origin.flow_name,
  explode(details:flow_progress.data_quality.expectations) AS expectation
FROM event_log
WHERE event_type = 'flow_progress'
  AND timestamp > current_timestamp() - INTERVAL 1 DAY;
```

### DQ Dashboard

Create a Lakeview dashboard to monitor:
- Expectation pass rates over time
- Top violated expectations
- Records dropped vs. warned
- Data completeness trends

## Schema Reference

### bronze_orders_raw
| Column | Type | Description |
|--------|------|-------------|
| order_id | STRING | Raw order identifier |
| customer_id | STRING | Raw customer identifier |
| order_date | STRING | Raw order timestamp |
| total_amount | STRING | Raw total amount |
| order_status | STRING | Raw order status |
| _ingestion_time | TIMESTAMP | Auto Loader ingestion timestamp |
| _file_path | STRING | Source file path |
| _rescued_data | STRING | Unparseable/extra fields (JSON) |

### orders_cleaned
| Column | Type | Description |
|--------|------|-------------|
| order_id | LONG | Validated order identifier (PK) |
| customer_id | LONG | Validated customer identifier |
| order_date | TIMESTAMP | Validated order timestamp |
| total_amount | DECIMAL(10,2) | Validated total amount |
| order_status | STRING | Validated status (pending, completed, cancelled, shipped, refunded) |
| _ingestion_time | TIMESTAMP | When record was ingested |
| _processing_time | TIMESTAMP | When record was cleaned |
| _file_path | STRING | Source file lineage |

### order_items_cleaned
| Column | Type | Description |
|--------|------|-------------|
| order_item_id | LONG | Order item identifier (PK) |
| order_id | LONG | Foreign key to orders_cleaned |
| product_id | LONG | Product identifier |
| quantity | INT | Quantity ordered |
| unit_price | DECIMAL(10,2) | Price per unit |
| _ingestion_time | TIMESTAMP | When record was ingested |
| _processing_time | TIMESTAMP | When record was cleaned |

### daily_sales_summary
| Column | Type | Description |
|--------|------|-------------|
| order_date | DATE | Business date (partition key) |
| total_orders | LONG | Count of orders |
| total_revenue | DECIMAL(20,2) | Sum of order totals |
| avg_order_value | DECIMAL(10,2) | Average order value |
| unique_customers | LONG | Count of distinct customers |
| pending_orders | LONG | Orders with pending status |
| completed_orders | LONG | Orders with completed status |
| cancelled_orders | LONG | Orders with cancelled status |

### customer_analytics
| Column | Type | Description |
|--------|------|-------------|
| customer_id | LONG | Customer identifier (PK) |
| total_orders | LONG | Lifetime order count |
| total_spent | DECIMAL(20,2) | Lifetime spend |
| avg_order_value | DECIMAL(10,2) | Average order value |
| first_order_date | TIMESTAMP | Date of first order |
| last_order_date | TIMESTAMP | Date of most recent order |
| days_since_last_order | INT | Recency metric |
| customer_lifetime_days | INT | Days since first order |

## Best Practices Implemented

### Code Quality
- ✅ Type hints and docstrings on all functions
- ✅ Constants for configuration (no magic strings)
- ✅ Modular design (one dataset per function)
- ✅ Comprehensive inline comments
- ✅ Current SDP API (`from pyspark import pipelines as dp`)

### Data Engineering
- ✅ Medallion architecture (Bronze/Silver/Gold)
- ✅ Schema evolution support
- ✅ Audit trail columns
- ✅ Idempotent processing
- ✅ Partition by time for performance

### Data Quality
- ✅ Layered validation (warn vs. drop)
- ✅ Business rule enforcement
- ✅ Rescued data capture
- ✅ Comprehensive expectations
- ✅ Monitoring-friendly metrics

### Operations
- ✅ Clear error messages
- ✅ Descriptive table/column comments
- ✅ Performance-optimized (Photon, partitioning)
- ✅ Production-ready configuration
- ✅ Documented troubleshooting

## Maintenance

### Regular Tasks

**Daily**
- Monitor pipeline run status
- Review data quality metrics
- Check for new schema evolution events

**Weekly**
- Analyze expectation violation trends
- Review processing performance
- Validate Gold layer accuracy with business stakeholders

**Monthly**
- Run OPTIMIZE on Silver/Gold tables
- Review and update data quality rules
- Capacity planning based on growth trends

**Quarterly**
- Schema governance review
- Performance benchmarking
- Cost optimization analysis

## Change Log

### Version 1.0 (Current)
- Initial production release
- Medallion architecture implementation
- Comprehensive data quality framework
- Auto Loader with schema evolution
- Partitioned tables for performance
- Gold layer analytics (daily sales, customer analytics)

## Support & Contact

For issues or questions:
1. Check this README troubleshooting section
2. Review pipeline event log for errors
3. Contact data engineering team
4. Refer to [Databricks SDP documentation](https://docs.databricks.com)

## References

- [Spark Declarative Pipelines](https://docs.databricks.com/delta-live-tables/index.html)
- [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Data Quality Expectations](https://docs.databricks.com/delta-live-tables/expectations.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Performance Tuning](https://docs.databricks.com/delta-live-tables/performance.html)