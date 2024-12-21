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

def classify_trip_distance(distance):
    if distance < 1:
        return 'Short'
    elif distance < 10:
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

path = paths[0]

df = spark.read.format('csv')\
        .option("header", "true")\
            .option("inferSchema", "true")\
                .load(path)
    # rename the column to lowercase
df = df.withColumnRenamed("VendorID", "vendorid")
df = df.withColumnRenamed("PULocationID", "pulocationid")
df = df.withColumnRenamed("DOLocationID", "dolocationid")
df = df.withColumnRenamed("RatecodeID", "ratecodeid")

rdd = df.rdd

trip_distance = rdd.map(lambda x: (classify_trip_distance(x['trip_distance']), 1))\
    .reduceByKey(lambda x, y: x + y)
    
df_trip_distance = trip_distance.toDF(['trip_distance', 'trip_count'])

# read the data from table cassandra
df_cassandra = spark.read\
    .format("org.apache.spark.sql.cassandra")\
        .options(table="trip_distance_statistics", keyspace="statistics")\
            .load()
            
df_trip_distance = df_trip_distance.join(df_cassandra, 'trip_distance', 'full')\
    .withColumn('total_trip', coalesce(col('trip_count')) + coalesce(col('count'), lit(0)) )\
        .drop('trip_count')\
            .drop('count')\
                .withColumnRenamed('total_trip', 'count')

df_trip_distance.show()
df_trip_distance.printSchema()

# write the data to cassandra
df_trip_distance.write\
    .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
            .options(table="trip_distance_statistics", keyspace="statistics")\
                .save()
