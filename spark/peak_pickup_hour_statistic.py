import findspark
findspark.init()

import uuid
import pyspark
import os
import sys
from env_variable import *
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, unix_timestamp, from_unixtime, hour, broadcast, coalesce, lit
from pyspark.sql.functions import expr, udf
from pyspark.sql.types import StringType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


spark = SparkSession.builder \
    .appName('Read Data from HDFS') \
        .config('spark.default.parallelism', 100) \
            .config('spark.cassandra.connection.host', 'localhost') \
                .getOrCreate()

client = InsecureClient(HDFS_URL)

directory = client.list(HDFS_PATH)

paths = [f"{HDFS_NAMENODE_URL}{HDFS_PATH}{file}" for file in directory]

df = spark.read.format('csv')\
        .option("header", "true")\
            .option("inferSchema", "true")\
                .load(paths)
                
# rename the column to lowercase
df = df.withColumnRenamed("VendorID", "vendorid")
df = df.withColumnRenamed("PULocationID", "pulocationid")
df = df.withColumnRenamed("DOLocationID", "dolocationid")
df = df.withColumnRenamed("RatecodeID", "ratecodeid")

df_pickup_hour = df.withColumn('pickup_hour', hour('tpep_pickup_datetime'))\
    .groupBy('pickup_hour')\
        .count()\
            .withColumnRenamed('count', 'pickup_count')\
                .sort('pickup_hour') 

# read the data from table cassandra
df_cassandra = spark.read\
    .format("org.apache.spark.sql.cassandra")\
        .options(table="pickup_hour_statistics", keyspace="statistics")\
            .load()

df_pickup_hour = df_pickup_hour.join(df_cassandra, 'pickup_hour', 'left')\
    .withColumn('total_pickup', col('pickup_count') + coalesce(col('count'), lit(0)) )\
        .drop('pickup_count')\
            .drop('count')\
                .withColumnRenamed('total_pickup', 'count')

# add the partition key
df_pickup_hour = df_pickup_hour.withColumn('partition_key', lit(1))

#change the column hour from int to format HH:MM
def format_hour(hour):
    return f"{hour:02}:00"

format_hour_udf = udf(format_hour, StringType())
df_pickup_hour = df_pickup_hour.withColumn('pickup_hour', format_hour_udf('pickup_hour'))


df_pickup_hour.show()   

# write the data to cassandra
df_pickup_hour.write \
    .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
            .options(table="pickup_hour_statistics", keyspace="statistics") \
                .save()
