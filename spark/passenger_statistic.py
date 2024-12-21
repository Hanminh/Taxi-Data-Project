import findspark
findspark.init()

import uuid
import pyspark
import os
import sys
from env_variable import *
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit

# Environment settings
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder \
    .appName('Read Data from HDFS') \
    .config('spark.default.parallelism', 100) \
    .config('spark.cassandra.connection.host', 'localhost') \
    .getOrCreate()

# Initialize HDFS client
client = InsecureClient(HDFS_URL)

# Get list of files from HDFS directory
directory = client.list(HDFS_PATH)
paths = [f"{HDFS_NAMENODE_URL}{HDFS_PATH}{file}" for file in directory]

# Read data from Cassandra
df_cassandra = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="passenger_number_statistics", keyspace="statistics") \
    .load()

# Read and combine all CSV files into a single DataFrame
df = spark.read.format('csv') \
    .option("header", "true") \
        .option("inferSchema", "true") \
            .load(paths)

# Rename columns to lowercase
column_mapping = {
    "VendorID": "vendorid",
    "PULocationID": "pulocationid",
    "DOLocationID": "dolocationid",
    "RatecodeID": "ratecodeid"
}
for old_col, new_col in column_mapping.items():
    if old_col in df.columns:
        df = df.withColumnRenamed(old_col, new_col)

# Aggregate passenger count data
df_passenger_number = df.groupBy("passenger_count") \
    .count() \
        .withColumnRenamed("passenger_count", "passenger_number")\
            .withColumnRenamed("count", "trip_count")

# Merge with existing Cassandra data
df_merged = df_passenger_number.join(df_cassandra, "passenger_number", "full") \
    .withColumn("count", coalesce(col("trip_count"), lit(0)) + coalesce(col("count"), lit(0))) \
    .drop("trip_count")

# add the partition key
df_merged = df_merged.withColumn("partition_key", lit(1))
# Remove null values
df_merged = df_merged.na.drop()
# format the passenger_number column to integer
df_merged = df_merged.withColumn("passenger_number", df_merged["passenger_number"].cast("int"))
df_merged.show()

# Write updated data back to Cassandra
df_merged.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="passenger_number_statistics", keyspace="statistics") \
    .save()

print("Data successfully written to Cassandra")