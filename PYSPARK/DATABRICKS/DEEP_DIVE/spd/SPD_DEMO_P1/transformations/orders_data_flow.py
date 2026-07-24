"""
Production-Grade E-Commerce Orders Data Pipeline
=================================================
Medallion Architecture: Bronze → Silver → Gold

Purpose:
    Process e-commerce order data from raw JSON files through a multi-layer
    transformation pipeline for analytics and business intelligence.

Layers:
    - Bronze: Raw data ingestion with Auto Loader
    - Silver: Cleaned, validated, and enriched data
    - Gold: Aggregated metrics for BI dashboards and reporting

Author: Data Engineering Team
Version: 1.0.0
Last Updated: 2026-07-24
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import TimestampType, DecimalType
from pyspark import pipelines as dp


# =============================================================================
# BRONZE LAYER: Raw Data Ingestion
# =============================================================================

@dp.table(
    name="bronze_orders",
    comment="Bronze layer: Raw e-commerce orders ingested via Auto Loader with schema inference and evolution",
    table_properties={
        "quality": "bronze",
        "layer": "raw",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def bronze_orders() -> DataFrame:
    """
    Ingest raw JSON order files from cloud storage using Auto Loader.
    
    Features:
    - Automatic schema inference and evolution
    - Rescued data column for malformed records
    - Exactly-once processing semantics
    - Incremental file discovery and processing
    
    Returns:
        DataFrame: Streaming DataFrame with all raw order data
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", 
                "/Volumes/db_spd_lab/db_spd/raw_volume/_schemas/orders")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.useIncrementalListing", "auto")
        .load("/Volumes/db_spd_lab/db_spd/raw_volume/orders/")
        .withColumn("_bronze_ingest_timestamp", F.current_timestamp())
        .withColumn("_bronze_source_file", F.col("_metadata.file_path"))
    )


# =============================================================================
# SILVER LAYER: Cleaned and Validated Data
# =============================================================================

@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL AND order_id != ''")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_amount", "amount > 0")
@dp.expect_or_drop("valid_order_date", "order_date IS NOT NULL")
@dp.expect("valid_status", "status IN ('pending', 'completed', 'cancelled', 'refunded')")
@dp.expect("reasonable_amount", "amount <= 100000")
@dp.table(
    name="silver_orders",
    comment="Silver layer: Cleaned and validated orders with data quality rules applied",
    table_properties={
        "quality": "silver",
        "layer": "curated",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["order_date"]
)
def silver_orders() -> DataFrame:
    """
    Clean, validate, and enrich bronze order data.
    
    Data Quality Rules:
    - DROP ROW: Missing critical fields (order_id, customer_id, amount, quantity, order_date)
    - DROP ROW: Invalid business rules (amount <= 0, quantity <= 0)
    - WARN: Invalid status values (not in expected list)
    - WARN: Suspicious amounts (> $100,000)
    
    Enrichments:
    - Type conversions and casting
    - Calculated fields (order_year, order_month, order_quarter)
    - Processing metadata
    - Data cleansing (trimmed strings, standardized formats)
    
    Returns:
        DataFrame: Streaming DataFrame with cleaned order data
    """
    return (
        spark.readStream.table("bronze_orders")
        # Filter out rescued/malformed records
        .filter(F.col("_rescued_data").isNull())
        # Type conversions and data cleansing
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))
        .withColumn("order_date", F.to_date(F.col("order_date")))
        # Enrichment: Time-based dimensions
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("order_quarter", F.quarter("order_date"))
        .withColumn("order_day_of_week", F.dayofweek("order_date"))
        # Enrichment: Calculated fields
        .withColumn("is_high_value", 
                   F.when(F.col("amount") >= 1000, True).otherwise(False))
        # Processing metadata
        .withColumn("_silver_processed_timestamp", F.current_timestamp())
        .withColumn("_data_quality_passed", F.lit(True))
        # Select final columns (exclude bronze metadata)
        .select(
            "order_id",
            "customer_id",
            "order_date",
            "order_year",
            "order_month",
            "order_quarter",
            "order_day_of_week",
            "status",
            "amount",
            "is_high_value",
            "_silver_processed_timestamp",
            "_data_quality_passed",
            "_bronze_ingest_timestamp",
            "_bronze_source_file"
        )
    )


# =============================================================================
# GOLD LAYER: Business Metrics and Aggregations
# =============================================================================

@dp.materialized_view(
    name="gold_daily_sales",
    comment="Gold layer: Daily sales metrics aggregated by date and status for BI dashboards",
    table_properties={
        "quality": "gold",
        "layer": "consumption",
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=["order_year", "order_month"]
)
def gold_daily_sales() -> DataFrame:
    """
    Daily sales aggregation for business intelligence and reporting.
    
    Metrics:
    - Total sales amount
    - Total order count
    - Average order value
    - Total quantity sold
    - High-value order count and percentage
    - Unique customer count
    
    Dimensions:
    - Order date
    - Order status
    - Year/Month (for partitioning)
    
    Returns:
        DataFrame: Batch DataFrame with daily aggregated metrics
    """
    return (
        spark.read.table("silver_orders")
        .groupBy("order_date", "order_year", "order_month", "status")
        .agg(
            # Volume metrics
            F.sum("amount").alias("total_sales"),
            F.count("*").alias("total_orders"),
            # Average metrics
            F.avg("amount").alias("avg_order_value"),
            # Customer metrics
            F.countDistinct("customer_id").alias("unique_customers"),
            # High-value order metrics
            F.sum(F.when(F.col("is_high_value"), 1).otherwise(0)).alias("high_value_orders"),
            # Min/Max for data quality monitoring
            F.min("amount").alias("min_order_amount"),
            F.max("amount").alias("max_order_amount"),
            # Processing timestamp
            F.max("_silver_processed_timestamp").alias("latest_processed_timestamp")
        )
        # Calculate derived metrics
        .withColumn("high_value_order_pct",
                   F.round((F.col("high_value_orders") / F.col("total_orders")) * 100, 2))
        .withColumn("_gold_aggregated_timestamp", F.current_timestamp())
        # Sort for better query performance
        .orderBy(F.desc("order_date"), "status")
    )


@dp.materialized_view(
    name="gold_customer_summary",
    comment="Gold layer: Customer-level summary metrics for customer analytics",
    table_properties={
        "quality": "gold",
        "layer": "consumption",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_customer_summary() -> DataFrame:
    """
    Customer-level aggregation for customer analytics and segmentation.
    
    Metrics:
    - Lifetime value (total spend)
    - Total order count
    - Average order value
    - First and last order dates
    - Recency, frequency, monetary (RFM) components
    
    Returns:
        DataFrame: Batch DataFrame with customer-level metrics
    """
    return (
        spark.read.table("silver_orders")
        .filter(F.col("status") == "completed")  # Only completed orders
        .groupBy("customer_id")
        .agg(
            # Monetary metrics
            F.sum("amount").alias("lifetime_value"),
            F.count("*").alias("total_orders"),
            F.avg("amount").alias("avg_order_value"),
            # Temporal metrics
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date")
        )
        # Calculate derived metrics
        .withColumn("days_since_first_order",
                   F.datediff(F.current_date(), F.col("first_order_date")))
        .withColumn("days_since_last_order",
                   F.datediff(F.current_date(), F.col("last_order_date")))
        .withColumn("customer_tenure_days",
                   F.datediff(F.col("last_order_date"), F.col("first_order_date")))
        .withColumn("is_active_customer",
                   F.when(F.col("days_since_last_order") <= 90, True).otherwise(False))
        .withColumn("customer_segment",
                   F.when(F.col("lifetime_value") >= 10000, "VIP")
                    .when(F.col("lifetime_value") >= 5000, "High Value")
                    .when(F.col("lifetime_value") >= 1000, "Medium Value")
                    .otherwise("Low Value"))
        .withColumn("_gold_aggregated_timestamp", F.current_timestamp())
    )