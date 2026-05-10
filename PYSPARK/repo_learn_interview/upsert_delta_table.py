from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def upsert_delta_table(spark, incoming_df, target_path, primary_keys, order_col="updated_at"):
    """
    Standard Production Upsert: 
    1. Deduplicates incoming batch.
    2. Checks if table exists (Init vs Merge).
    3. Version-safe updates (Only newer records overwrite target).
    """
    
    # 1. De-duplicate the incoming batch (In case of duplicate IDs in the same file)
    w = Window.partitionBy(*primary_keys).orderBy(F.col(order_col).desc())
    deduped_df = (
        incoming_df
        .withColumn("_row_num", F.row_number().over(w))
        .filter("_row_num = 1")
        .drop("_row_num")
    )

    # 2. Case: Initial Load (Table doesn't exist yet)
    if not DeltaTable.isDeltaTable(spark, target_path):
        return (
            deduped_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true") # Handles schema changes in first run
            .save(target_path)
        )

    # 3. Case: Existing Table (Perform Optimized Merge)
    delta_table = DeltaTable.forPath(spark, target_path)
    
    # Build join condition: target.id = source.id
    merge_condition = " AND ".join([f"t.{pk} = s.{pk}" for pk in primary_keys])
    
    # Update condition: Only if incoming data is strictly NEWER
    update_clause = f"s.{order_col} > t.{order_col}"

    return (
        delta_table.alias("t")
        .merge(deduped_df.alias("s"), merge_condition)
        .whenMatchedUpdateAll(condition=update_clause)
        .whenNotMatchedInsertAll()
        .execute()
    )
  """
  ==============================================================================
  DataSets
  ==============================================================================
  import pyspark.sql.functions as F
# Target Table (Current State)
target_data/Volumes/pyspark_learn/practice/files/interview_scn/upsert/target_data/ = [
    ("ORD-001", "C-100", 99.99, "pending", "2025-01-15 10:00"),
    ("ORD-002", "C-200", 49.99, "completed", "2025-01-14 09:00")
]

schema = ["order_id", "customer_id", "amount", "status", "updated_at"]
target_df = spark.createDataFrame(target_data, schema)
target_df.show(truncate=False)

#  Incoming Batch (Incremental Data)

incoming_data = [
    ("ORD-001", "C-100", 99.99, "completed", "2025-01-15 14:00"),
    ("ORD-003", "C-300", 29.99, "pending", "2025-01-15 11:00")
]

incoming_df = spark.createDataFrame(incoming_data, schema)

incoming_df.show(truncate=False)
==============================================================================
Test the Functions

# Execute the upsert
upsert_delta_table(
    spark=spark,
    incoming_df=target_df,
    target_path="/Volumes/pyspark_learn/practice/files/interview_scn/upsert/target_data/",
    primary_keys= ["order_id"],
    order_col="updated_at"
)

target_path = "/Volumes/pyspark_learn/practice/files/interview_scn/upsert/target_data/"

# Read the latest version of the table
df = spark.read.format("delta").load(target_path)

df.show()

# Execute the upsert
upsert_delta_table(
    spark=spark,
    incoming_df=incoming_df,
    target_path="/Volumes/pyspark_learn/practice/files/interview_scn/upsert/target_data/",
    primary_keys= ["order_id"],
    order_col="updated_at"
)

df.show(truncate=False)
==============================================================================
  """
