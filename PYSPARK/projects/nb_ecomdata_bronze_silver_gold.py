## -----------------------------------------------------------------------------------------------------------------------------------
##                                             Bronze Transformations ---> nb_ecomdata_bronze
## -----------------------------------------------------------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("EcomDataPipeline").getOrCreate()

%fs ls "abfss://landing-zone-2@stecomdatapoccin.dfs.core.windows.net"

#Defining Storage RAW PAth
raw_files = "abfss://landing-zone-2@stecomdatapoccin.dfs.core.windows.net/"
bronze_tables = "abfss://landing-zone-2@stecomdatapoccin.dfs.core.windows.net/delta/tables/bronze/"


# Read parquet file from /mnt/ecomdata1/user-raw-2 folder
userDF = spark.read.format("parquet")\
    .option("header",'true')\
    .option("inferSchema",'true')\
    .load(raw_files +"users-raw-2")

userDF.write.format("delta")\
    .mode("overwrite")\
    .save( bronze_tables + "users")

buyersDF = spark.read.format("parquet")\
    .option("header",'true')\
    .option("inferSchema",'true')\
   .load(raw_files + "buyers-raw-2")

buyersDF.write.format("delta")\
    .mode("overwrite")\
    .save( bronze_tables + "buyers")

sellersDF = spark.read.format("parquet")\
    .option("header",'true')\
    .option("inferSchema",'true')\
    .load(raw_files + "sellers-raw-2")

sellersDF.write.format("delta")\
    .mode("overwrite")\
    .save( bronze_tables + "sellers")

countriesDF = spark.read.format("parquet")\
    .option("header",'true')\
    .option("inferSchema",'true')\
    .load(raw_files + "countries-raw-2")

countriesDF.write.format("delta")\
    .mode("overwrite")\
    .save( bronze_tables + "countries")
## -----------------------------------------------------------------------------------------------------------------------------------
##                                             Silver Transformations ---> nb_ecomdata_silver
## -----------------------------------------------------------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ECommerceDataPipeline").getOrCreate()

#Defining Tables Path for bronze & silver
bronze_tables = "abfss://landing-zone-2@stecomdatapoccin.dfs.core.windows.net/delta/tables/bronze/"
silver_tables = "abfss://landing-zone-2@stecomdatapoccin.dfs.core.windows.net/delta/tables/silver/"


#Loading Table from bronze_tables Path
usersDF = spark.read.format("delta").load( bronze_tables + "users")

usersDF = usersDF.withColumn("countrycode", upper(col("countrycode")))

# Handling multiple languages elegantly with `expr` and `case when`
usersDF = usersDF.withColumn("language_full", 
                             expr("CASE WHEN language = 'EN' THEN 'English' " +
                                  "WHEN language = 'FR' THEN 'French' " +
                                  "ELSE 'Other' END"))

# Correcting potential data entry errors in `gender` column
usersDF = usersDF.withColumn("gender", 
                             when(col("gender").startswith("M"), "Male")
                             .when(col("gender").startswith("F"), "Female")
                             .otherwise("Other"))

# Using `regexp_replace` to clean `civilitytitle` values
usersDF = usersDF.withColumn("civilitytitle_clean", 
                             regexp_replace("civilitytitle", "(Mme|Ms|Mrs)", "Ms"))

# Derive new column `years_since_last_login` from `dayssincelastlogin`
usersDF = usersDF.withColumn("years_since_last_login", col("dayssincelastlogin") / 365)

# Calculate age of account in years and categorize into `account_age_group`
usersDF = usersDF.withColumn("account_age_years", round(col("seniority") / 365, 2))
usersDF = usersDF.withColumn("account_age_group",
                             when(col("account_age_years") < 1, "New")
                             .when((col("account_age_years") >= 1) & (col("account_age_years") < 3), "Intermediate")
                             .otherwise("Experienced"))

# Add a column with the current year for comparison
usersDF = usersDF.withColumn("current_year", year(current_date()))

# Creatively combining strings to form a unique user descriptor
usersDF = usersDF.withColumn("user_descriptor", 
                             concat(col("gender"), lit("_"), 
                                    col("countrycode"), lit("_"), 
                                    expr("substring(civilitytitle_clean, 1, 3)"), lit("_"), 
                                    col("language_full")))

usersDF = usersDF.withColumn("flag_long_title", length(col("civilitytitle")) > 10)

usersDF = usersDF.withColumn("hasanyapp", col("hasanyapp").cast("boolean"))
usersDF = usersDF.withColumn("hasandroidapp", col("hasandroidapp").cast("boolean"))
usersDF = usersDF.withColumn("hasiosapp", col("hasiosapp").cast("boolean"))
usersDF = usersDF.withColumn("hasprofilepicture", col("hasprofilepicture").cast("boolean"))


usersDF = usersDF.withColumn("socialnbfollowers", col("socialnbfollowers").cast(IntegerType()))
usersDF = usersDF.withColumn("socialnbfollows", col("socialnbfollows").cast(IntegerType()))

usersDF = usersDF.withColumn("productspassrate", col("productspassrate").cast(DecimalType(10, 2)))
usersDF = usersDF.withColumn("seniorityasmonths", col("seniorityasmonths").cast(DecimalType(10, 2)))
usersDF = usersDF.withColumn("seniorityasyears", col("seniorityasyears").cast(DecimalType(10, 2)))

usersDF = usersDF.withColumn("dayssincelastlogin",
                             when(col("dayssincelastlogin").isNotNull(),
                                  col("dayssincelastlogin").cast(IntegerType()))
                             .otherwise(0))  #

#Writing Table from silver_tables Path
usersDF.write.format("delta").mode("overwrite").save( silver_tables + "users")

#Loading Table from bronze_tables Path
buyersDF = spark.read.format("delta").load( bronze_tables + "buyers")

# Casting Integer columns
integer_columns = [
    'buyers', 'topbuyers', 'femalebuyers', 'malebuyers',
    'topfemalebuyers', 'topmalebuyers', 'totalproductsbought',
    'totalproductswished', 'totalproductsliked', 'toptotalproductsbought',
    'toptotalproductswished', 'toptotalproductsliked'
]

for column_name in integer_columns:
    buyersDF = buyersDF.withColumn(column_name, col(column_name).cast(IntegerType()))

# Casting Decimal columns
decimal_columns = [
    'topbuyerratio', 'femalebuyersratio', 'topfemalebuyersratio',
    'boughtperwishlistratio', 'boughtperlikeratio', 'topboughtperwishlistratio',
    'topboughtperlikeratio', 'meanproductsbought', 'meanproductswished',
    'meanproductsliked', 'topmeanproductsbought', 'topmeanproductswished',
    'topmeanproductsliked', 'meanofflinedays', 'topmeanofflinedays',
    'meanfollowers', 'meanfollowing', 'topmeanfollowers', 'topmeanfollowing'
]

for column_name in decimal_columns:
    buyersDF = buyersDF.withColumn(column_name, col(column_name).cast(DecimalType(10, 2)))

# Normalize country names
buyersDF = buyersDF.withColumn("country", initcap(col("country")))

for col_name in integer_columns:
    buyersDF = buyersDF.fillna({col_name: 0})

# Calculate the ratio of female to male buyers
buyersDF = buyersDF.withColumn("female_to_male_ratio", 
                               round(col("femalebuyers") / (col("malebuyers") + 1), 2))

# Determine the market potential by comparing wishlist and purchases
buyersDF = buyersDF.withColumn("wishlist_to_purchase_ratio", 
                               round(col("totalproductswished") / (col("totalproductsbought") + 1), 2))

# Tag countries with a high engagement ratio
high_engagement_threshold = 0.5
buyersDF = buyersDF.withColumn("high_engagement",
                               when(col("boughtperwishlistratio") > high_engagement_threshold, True)
                               .otherwise(False))
                               
    # Flag markets with increasing female buyer participation
buyersDF = buyersDF.withColumn("growing_female_market",
                               when(col("femalebuyersratio") > col("topfemalebuyersratio"), True)
                               .otherwise(False))

#Writing Table from silver_tables Path
buyersDF.write.format("delta").mode("overwrite").save(silver_tables + "buyers")

#Loading Table from bronze_tables Path
sellersDF = spark.read.format("delta").load(bronze_tables + "sellers")

sellersDF = sellersDF \
    .withColumn("nbsellers", col("nbsellers").cast(IntegerType())) \
    .withColumn("meanproductssold", col("meanproductssold").cast(DecimalType(10, 2))) \
    .withColumn("meanproductslisted", col("meanproductslisted").cast(DecimalType(10, 2))) \
    .withColumn("meansellerpassrate", col("meansellerpassrate").cast(DecimalType(10, 2))) \
    .withColumn("totalproductssold", col("totalproductssold").cast(IntegerType())) \
    .withColumn("totalproductslisted", col("totalproductslisted").cast(IntegerType())) \
    .withColumn("meanproductsbought", col("meanproductsbought").cast(DecimalType(10, 2))) \
    .withColumn("meanproductswished", col("meanproductswished").cast(DecimalType(10, 2))) \
    .withColumn("meanproductsliked", col("meanproductsliked").cast(DecimalType(10, 2))) \
    .withColumn("totalbought", col("totalbought").cast(IntegerType())) \
    .withColumn("totalwished", col("totalwished").cast(IntegerType())) \
    .withColumn("totalproductsliked", col("totalproductsliked").cast(IntegerType())) \
    .withColumn("meanfollowers", col("meanfollowers").cast(DecimalType(10, 2))) \
    .withColumn("meanfollows", col("meanfollows").cast(DecimalType(10, 2))) \
    .withColumn("percentofappusers", col("percentofappusers").cast(DecimalType(10, 2))) \
    .withColumn("percentofiosusers", col("percentofiosusers").cast(DecimalType(10, 2))) \
    .withColumn("meanseniority", col("meanseniority").cast(DecimalType(10, 2)))

# Normalize country names and gender values
sellersDF = sellersDF.withColumn("country", initcap(col("country"))) \
                                                .withColumn("sex", upper(col("sex")))


#Add a column to categorize the number of sellers
sellersDF = sellersDF.withColumn("seller_size_category", 
                               when(col("nbsellers") < 500, "Small") \
                               .when((col("nbsellers") >= 500) & (col("nbsellers") < 2000), "Medium") \
                               .otherwise("Large"))

# Calculate the mean products listed per seller as an indicator of seller activity
sellersDF = sellersDF.withColumn("mean_products_listed_per_seller", 
                               round(col("totalproductslisted") / col("nbsellers"), 2))

# Identify markets with high seller pass rate
sellersDF = sellersDF.withColumn("high_seller_pass_rate", 
                               when(col("meansellerpassrate") > 0.75, "High") \
                               .otherwise("Normal"))

mean_pass_rate = sellersDF.select(round(avg("meansellerpassrate"), 2).alias("avg_pass_rate")).collect()[0]["avg_pass_rate"]

sellersDF = sellersDF.withColumn("meansellerpassrate",
                                 when(col("meansellerpassrate").isNull(), mean_pass_rate)
                                 .otherwise(col("meansellerpassrate")))


#Writing Table from silver_tables Path
sellersDF.write.format("delta").mode("overwrite").save(silver_tables + "sellers")

#Loading Table from bronze_tables Path
countriesDF = spark.read.format("delta").load(bronze_tables + "countries")

countriesDF = countriesDF \
    .withColumn("sellers", col("sellers").cast(IntegerType())) \
    .withColumn("topsellers", col("topsellers").cast(IntegerType())) \
    .withColumn("topsellerratio", col("topsellerratio").cast(DecimalType(10, 2))) \
    .withColumn("femalesellersratio", col("femalesellersratio").cast(DecimalType(10, 2))) \
    .withColumn("topfemalesellersratio", col("topfemalesellersratio").cast(DecimalType(10, 2))) \
    .withColumn("femalesellers", col("femalesellers").cast(IntegerType())) \
    .withColumn("malesellers", col("malesellers").cast(IntegerType())) \
    .withColumn("topfemalesellers", col("topfemalesellers").cast(IntegerType())) \
    .withColumn("topmalesellers", col("topmalesellers").cast(IntegerType())) \
    .withColumn("countrysoldratio", col("countrysoldratio").cast(DecimalType(10, 2))) \
    .withColumn("bestsoldratio", col("bestsoldratio").cast(DecimalType(10, 2))) \
    .withColumn("toptotalproductssold", col("toptotalproductssold").cast(IntegerType())) \
    .withColumn("totalproductssold", col("totalproductssold").cast(IntegerType())) \
    .withColumn("toptotalproductslisted", col("toptotalproductslisted").cast(IntegerType())) \
    .withColumn("totalproductslisted", col("totalproductslisted").cast(IntegerType())) \
    .withColumn("topmeanproductssold", col("topmeanproductssold").cast(DecimalType(10, 2))) \
    .withColumn("topmeanproductslisted", col("topmeanproductslisted").cast(DecimalType(10, 2))) \
    .withColumn("meanproductssold", col("meanproductssold").cast(DecimalType(10, 2))) \
    .withColumn("meanproductslisted", col("meanproductslisted").cast(DecimalType(10, 2))) \
    .withColumn("meanofflinedays", col("meanofflinedays").cast(DecimalType(10, 2))) \
    .withColumn("topmeanofflinedays", col("topmeanofflinedays").cast(DecimalType(10, 2))) \
    .withColumn("meanfollowers", col("meanfollowers").cast(DecimalType(10, 2))) \
    .withColumn("meanfollowing", col("meanfollowing").cast(DecimalType(10, 2))) \
    .withColumn("topmeanfollowers", col("topmeanfollowers").cast(DecimalType(10, 2))) \
    .withColumn("topmeanfollowing", col("topmeanfollowing").cast(DecimalType(10, 2)))

countriesDF = countriesDF.withColumn("country", initcap(col("country")))
# Calculating the ratio of top sellers to total sellers
countriesDF = countriesDF.withColumn("top_seller_ratio", 
                                        round(col("topsellers") / col("sellers"), 2))

# countriesDF countries with a high ratio of female sellers
countriesDF = countriesDF.withColumn("high_female_seller_ratio", 
                                        when(col("femalesellersratio") > 0.5, True).otherwise(False))

# Adding a performance indicator based on the sold/listed ratio
countriesDF = countriesDF.withColumn("performance_indicator", 
                                        round(col("toptotalproductssold") / (col("toptotalproductslisted") + 1), 2))

# Flag countries with exceptionally high performance
performance_threshold = 0.8
countriesDF = countriesDF.withColumn("high_performance", 
                                        when(col("performance_indicator") > performance_threshold, True).otherwise(False))

countriesDF = countriesDF.withColumn("activity_level",
                                       when(col("meanofflinedays") < 30, "Highly Active")
                                       .when((col("meanofflinedays") >= 30) & (col("meanofflinedays") < 60), "Moderately Active")
                                       .otherwise("Low Activity"))

#Writing Table from silver_tables Path
countriesDF.write.format("delta").mode("overwrite").save(silver_tables + "countries")

## -----------------------------------------------------------------------------------------------------------------------------------
##                                             Gold Transformations ---> nb_ecomdata_gold
## -----------------------------------------------------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, col,monotonically_increasing_id 
spark = SparkSession.builder.appName("GoldLayerCreation").getOrCreate()

#Defining Tables Path for silver & gold
silver_tables = "abfss://landing-zone-2@stecomdatapoccin.dfs.core.windows.net/delta/tables/silver/"
gold_tables = "abfss://landing-zone-2@stecomdatapoccin.dfs.core.windows.net/delta/tables/gold/"

# Read the necessary Silver tables
silver_users = spark.read.format("delta").load( silver_tables + "users")
silver_sellers = spark.read.format("delta").load( silver_tables + "sellers")
silver_buyers = spark.read.format("delta").load(silver_tables + "buyers")
silver_countries = spark.read.format("delta").load(silver_tables + "countries")

# Perform the join operations
comprehensive_user_table = silver_users \
    .join(silver_countries, ["country"], "outer") \
    .join(silver_buyers, ["country"], "outer") \
    .join(silver_sellers, ["country"], "outer")

# Select and alias columns from each dataframe to ensure uniqueness
comprehensive_user_table = comprehensive_user_table.select(
    silver_users["country"].alias("Country"),
    # From silver_users
    silver_users["productsSold"].alias("Users_productsSold"),
    silver_users["productsWished"].alias("Users_productsWished"),
    silver_users["account_age_years"].alias("Users_account_age_years"),
    silver_users["account_age_group"].alias("Users_account_age_group"),
    silver_users["hasanyapp"].alias("Users_hasanyapp"),
    silver_users["socialnbfollowers"].alias("Users_socialnbfollowers"),
    silver_users["flag_long_title"].alias("Users_flag_long_title"),
    # Continue with other silver_users columns as needed...
    
    # From silver_countries
    silver_countries["sellers"].alias("Countries_Sellers"),
    silver_countries["topsellers"].alias("Countries_TopSellers"),
    silver_countries["femalesellers"].alias("Countries_FemaleSellers"),
    silver_countries["malesellers"].alias("Countries_MaleSellers"),
    silver_countries["topfemalesellers"].alias("Countries_TopFemaleSellers"),
    silver_countries["topmalesellers"].alias("Countries_TopMaleSellers"),
    # Continue with other silver_countries columns as needed...
    
    # From silver_buyers
    silver_buyers["buyers"].alias("Buyers_Total"),
    silver_buyers["topbuyers"].alias("Buyers_Top"),
    silver_buyers["femalebuyers"].alias("Buyers_Female"),
    silver_buyers["malebuyers"].alias("Buyers_Male"),
    silver_buyers["topfemalebuyers"].alias("Buyers_TopFemale"),
    silver_buyers["topmalebuyers"].alias("Buyers_TopMale"),
    # Continue with other silver_buyers columns as needed...
    
    # From silver_sellers
    silver_sellers["nbsellers"].alias("Sellers_Total"),
    silver_sellers["sex"].alias("Sellers_Sex"),
    silver_sellers["meanproductssold"].alias("Sellers_MeanProductsSold"),
    silver_sellers["meanproductslisted"].alias("Sellers_MeanProductsListed"),
    # Continue with other silver_sellers columns as needed...
)


comprehensive_user_table.write.format("delta").mode("overwrite").save( gold_tables + "com_one_big_table")
