# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SCD_Hash_Example").getOrCreate()

# COMMAND ----------

from pyspark.sql import Row

# Initial source data
source_data = [
    Row(Emp_ID=1099, name="Akash", dept="HR"),
    Row(Emp_ID=2403, name="Avi", dept="CIU"),
    Row(Emp_ID=3845, name="James", dept="BFSI"),
    Row(Emp_ID=1004, name="Shreya", dept="RMG"),
    Row(Emp_ID=1081, name="Rinki", dept="Cyber Security")
]

# Create DataFrame
source_df = spark.createDataFrame(source_data)

# Show source table
source_df.show()


# COMMAND ----------

from pyspark.sql.functions import concat_ws, md5

source_df = source_df.withColumn("hash_key", md5(concat_ws("||", "name", "dept")))
source_df.show(truncate=False)


# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DateType

# Define the schema
schema = StructType([
    StructField("Emp_ID", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("hash_key", StringType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True)
])

# Simulate a target table (same as source for now)
target_data = [
    Row(Emp_ID=1099, name="Akash", dept="HR", hash_key="hash1", is_current=True, start_date="2024-01-01", end_date=None),
    Row(Emp_ID=2403, name="Avi", dept="CIU", hash_key="hash2", is_current=True, start_date="2024-01-01", end_date=None),
    Row(Emp_ID=3845, name="James", dept="BFSI", hash_key="hash3", is_current=True, start_date="2024-01-01", end_date=None),
    Row(Emp_ID=1004, name="Shreya", dept="RMG", hash_key="hash4", is_current=True, start_date="2024-01-01", end_date=None),
    Row(Emp_ID=1081, name="Rinki", dept="Cyber Security", hash_key="hash5", is_current=True, start_date="2024-01-01", end_date=None)
]

target_df = spark.createDataFrame(target_data, schema)

display(target_df)

# COMMAND ----------

from pyspark.sql.functions import md5, concat_ws
from pyspark.sql import Row

updated_source_data = [
    Row(Emp_ID=1099, name="Akash", dept="HR"),
    Row(Emp_ID=2403, name="Avi", dept="CIU"),
    Row(Emp_ID=3845, name="James", dept="BFSI"),
    Row(Emp_ID=1004, name="Shreya", dept="CIU"), # Changed dept
    Row(Emp_ID=1081, name="Rinki", dept="BFSI")  # Changed dept
]

updated_source_df = spark.createDataFrame(updated_source_data)
updated_source_df = updated_source_df.withColumn(
    "hash_key", 
    md5(concat_ws("||", "name", "dept"))
)

display(updated_source_df)

# COMMAND ----------

# SCD1 logic: overwrite if emp_id matches
scd1_df = target_df.drop("hash_key", "is_current", "start_date", "end_date") \
    .alias("t") \
    .join(updated_source_df.alias("s"), on="Emp_ID") \
    .select("s.*")  # overwrite everything from source

scd1_df.show()

# COMMAND ----------

from pyspark.sql.functions import current_date, lit, col, when

# Join source and target on primary key
join_df = updated_source_df.alias("s").join(target_df.alias("t"), on="Emp_ID", how="left")

# Identify changes (hash mismatch)
scd2_updates = join_df.filter(col("s.hash_key") != col("t.hash_key"))

# Close old records
expired_df = scd2_updates.select("t.*") \
    .withColumn("is_current", lit(False)) \
    .withColumn("end_date", current_date())

# Insert new updated record
new_df = scd2_updates.select("s.*") \
    .withColumn("is_current", lit(True)) \
    .withColumn("start_date", current_date()) \
    .withColumn("end_date", lit(None).cast("string"))

# Unchanged records
unchanged_df = join_df.filter(col("s.hash_key") == col("t.hash_key")) \
    .select("t.*")

# Final SCD2 table
final_scd2_df = unchanged_df.union(expired_df).union(new_df)
final_scd2_df.show(truncate=False)
