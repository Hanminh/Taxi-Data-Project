import findspark
findspark.init()

import uuid
import pyspark
from env_variable import *
import os
import sys
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, unix_timestamp, from_unixtime
from pyspark.sql.functions import expr, udf
from pyspark.sql.types import StringType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def generate_time_uuid():
    return str(uuid.uuid1())

def classify_trip_duration(x):
    if x < 5:
        return 'Very Short'
    elif x < 10:
        return 'Short'
    elif x < 20:
        return 'Medium'
    else:
        return 'Long'

spark = SparkSession.builder \
    .appName('Read Data from HDFS') \
    .config('spark.default.parallelism', 100) \
        .config('spark.cassandra.connection.host', 'localhost') \
    .getOrCreate()

client = InsecureClient(HDFS_URL)

directory = client.list(HDFS_PATH)

paths = [f"{HDFS_NAMENODE_URL}{HDFS_PATH}{file}" for file in directory]

print(paths)

df = spark.read.format('csv')\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load(paths)
    
df.printSchema()

# Calculate trip duration and add a cloumn uuid
data = df.withColumn('trip_duration', (unix_timestamp('tpep_dropoff_datetime') - unix_timestamp('tpep_pickup_datetime'))/60)
data = data.withColumn('id', expr('uuid()'))

# Rename the column VendorID, PULocationID, DOLocationID, RatecodeID to lowercase
data = data.withColumnRenamed("VendorID", "vendorid")
data = data.withColumnRenamed("PULocationID", "pulocationid")
data = data.withColumnRenamed("DOLocationID", "dolocationid")
data = data.withColumnRenamed("RatecodeID", "ratecodeid")

#show the first 5 rows with col tpep_pickup_datetime, tpep_dropoff_datetime, trip_duration
data.select('tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_duration').show(5)
data.printSchema()

# #write data to cassandra 
# data.write \
#     .format("org.apache.spark.sql.cassandra") \
#         .mode('append') \
#             .options(table="tripdata", keyspace="test") \
#                 .save()
