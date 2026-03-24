from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# List the files info from Azure Storage

%fs ls "abfss://tokyo-olympic-data@olympicdatasnalla.dfs.core.windows.net/"

spark

# Reading the Data from Azure Storage

raw_files = "abfss://tokyo-olympic-data@olympicdatasnalla.dfs.core.windows.net/raw-data/"
athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load( raw_files + "athletes.csv")
coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load( raw_files + "coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").option("inferSchema","true").load( raw_files + "entriesGender.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load( raw_files + "medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load( raw_files + "teams.csv")

# Verify if the dataframes has proper data
teams.display()

# Do few transformations on the dataframes

entriesgender = entriesgender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))

# Find the top countries with the highest number of gold medals
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("TeamCountry","Gold").show()

# Calculate the average number of entries by gender for each discipline
average_entries_by_gender = entriesgender.withColumn(
    'Avg_Female', entriesgender['Female'] / entriesgender['Total']
).withColumn(
    'Avg_Male', entriesgender['Male'] / entriesgender['Total']
)
average_entries_by_gender.show()

# Write the files back to Azure Storage under transformed-data folder
# Verify if the files are written succuessuly to the Storage folder

transformed_files = "abfss://tokyo-olympic-data@olympicdatasnalla.dfs.core.windows.net/transformed-data/"
athletes.repartition(1).write.mode("overwrite").option("header",'true').csv( transformed_files + "athletes")
coaches.repartition(1).write.mode("overwrite").option("header","true").csv(transformed_files + "coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header","true").csv(transformed_files + "entriesgender")
medals.repartition(1).write.mode("overwrite").option("header","true").csv(transformed_files + "medals")
teams.repartition(1).write.mode("overwrite").option("header","true").csv(transformed_files + "teams")

"""
###### ISSUES FACED #####################
Issue 1 : Error while mounting DBFS 
Casue : Unity Catalog (UC) enabled clusters block `dbutils.fs.mount` because mounting is a legacy technique

We have to use new method that involves of below steps (below are high level steps) , you can watch below videos in references section.

Azure Side 
  Create Access Connector for Azure Databricks 
  Assign IAM Roles on Storage Account to Access Connector
  Register Provider : Microsoft.EventGrid is Registered in your Azure Subscription 
Databricks Side
  Create Storage Credential
  Create External Location

References :
=======================
**Databricks Unity Catalog: Storage Credentials and External Locations** https://www.youtube.com/watch?v=kRfNXFh9T3U
[Databricks Tutorial: Connecting to Azure Data Lake Storage Gen2 & Blob Storage | Mount Azure Storage](https://www.youtube.com/watch?v=VkjqViooMtQ)
"""
