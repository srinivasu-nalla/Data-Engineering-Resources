################################### Data Sets ###############################
# To learn the Delta Lake Merge (Upsert) pattern, you need to think of it as a way to synchronize two datasets: your Target (the permanent table) and your Source (the new incoming data).

from delta.tables import DeltaTable
from pyspark.sql import functions as F

target_users_path = "/Volumes/pyspark_learn/practice/files/interview_scn/upsert/users/"

# 1. Create the initial Target Table
target_data = [
    (101, "Basic", "2026-05-10 10:00:00"),
    (102, "Pro", "2026-05-10 10:00:00"),
    (103, "Free", None)
]
schema = ["user_id", "plan", "updated_at"]
# Save this to your target_users_path first
spark.createDataFrame(target_data, schema).write.format("delta").mode("overwrite").save(target_users_path)

# 2. Define the Incoming Source Data with intentional duplicates
source_data = [
    (101, "Premium", "2026-05-11 08:00:00"), 
    (101, "Ultra", "2026-05-11 11:00:00"),  
    (102, "Basic", "2026-05-09 12:00:00"),   
    (103, "Free+", "2026-05-11 10:00:00"),   
    (104, "Pro", "2026-05-11 12:00:00")     
]
source_df = spark.createDataFrame(source_data, schema)
################################################STEP 2 , to Test by Droing the delta Table
# WARNING: This deletes the underlying Parquet files and the Transaction Log (_delta_log)
dbutils.fs.rm("/Volumes/pyspark_learn/practice/files/interview_scn/upsert/users/", recurse=True)

################################################ MERGE LOGIC
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Path setup
target_users_path = "/Volumes/pyspark_learn/practice/files/interview_scn/upsert/users/"


print(">>> Source Data: RAW (with duplicates/older data)")
source_df.orderBy("user_id", "updated_at").show(truncate=False)

# --- STEP 1: DEDUPLICATE SOURCE ---
# Using a Window to ensure we only have 1 record per user_id (the latest one)
window_spec = Window.partitionBy("user_id").orderBy(F.col("updated_at").desc_nulls_last())

dedup_df = (
    source_df
    .withColumn("_rn", F.row_number().over(window_spec))
    .filter("_rn = 1")
    .drop("_rn")
)

print(">>> Source Data: AFTER DEDUPLICATION")
dedup_df.orderBy("user_id").show(truncate=False)

# 1. Check if the Delta Table actually exists
if not DeltaTable.isDeltaTable(spark, target_users_path):
    print(">>> Target not found. Performing Initial Load...")
    (
        dedup_df.write.format("delta")
        .mode("overwrite") 
        .save(target_users_path)
    )
else:
    print(">>> Target found. Performing Atomic Merge...")
    # --- PRE-MERGE VIEW ---
    print(">>> Target Data: BEFORE MERGE")
    target_table = DeltaTable.forPath(spark, target_users_path)
    spark.read.format("delta").load(target_users_path).orderBy("user_id").show()
  
    (
        target_table.alias("t")
        .merge(F.broadcast(dedup_df).alias("s"), "t.user_id = s.user_id")
        .whenMatchedUpdateAll(
            condition="s.updated_at IS NOT NULL AND (t.updated_at IS NULL OR s.updated_at > t.updated_at)"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

# --- POST-MERGE VIEW ---
print(">>> Target Data: AFTER MERGE")
spark.read.format("delta").load(target_users_path).orderBy("user_id").show()


