import findspark
findspark.init()

import pyspark
import os
import sys
from env_variable import *
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, year, weekofyear, from_unixtime, unix_timestamp, expr, to_date, month, dayofmonth
from pyspark.sql.types import StringType

# Set environment variables for Spark
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

# Retrieve list of files in the directory
directory = client.list(HDFS_PATH)
paths = [f"{HDFS_NAMENODE_URL}{HDFS_PATH}{file}" for file in directory]

# Read the data files into a DataFrame
df = spark.read.format('csv') \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(paths)

# Rename relevant columns to lowercase
column_mapping = {
    "VendorID": "vendorid",
    "PULocationID": "pulocationid",
    "DOLocationID": "dolocationid",
    "RatecodeID": "ratecodeid"
}
for old_col, new_col in column_mapping.items():
    df = df.withColumnRenamed(old_col, new_col)

# Filter for trips after January 1, 2018
df = df.filter(df.tpep_pickup_datetime > '2018-01-01')

# Read existing data from the Cassandra table
df_cassandra = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="total_amount_statistics", keyspace="statistics") \
    .load()

# Aggregate the total amount per day
df_total_amount = df.groupBy(
    year("tpep_dropoff_datetime").alias("year"),
    month("tpep_dropoff_datetime").alias("month"),
    dayofmonth("tpep_dropoff_datetime").alias("day")
).agg({"total_amount": "sum"})

# join the new data with the existing data
df_total_amount = df_total_amount.join(df_cassandra, ['year', 'month', 'day'], 'full')\
    .withColumn('final_amount', coalesce(col('sum(total_amount)')) + coalesce(col('total_amount'), lit(0)))\
        .drop('sum(total_amount)')\
            .drop('total_amount')\
                .withColumnRenamed('final_amount', 'total_amount')

#add the timestamp column
df_total_amount = df_total_amount.withColumn("timestamp", to_date(expr("concat(year, '-', month, '-', day)")))

# Add the partition key
df_total_amount = df_total_amount.withColumn('partition_key', lit(1))

df_total_amount.show()

# Write the processed data back to the Cassandra table
df_total_amount.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode('append') \
    .options(table="total_amount_statistics", keyspace="statistics") \
    .save()

print('Data written to Cassandra successfully!')
