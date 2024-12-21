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

# read data from hdfs
df = spark.read.format('csv')\
        .option("header", "true")\
            .option("inferSchema", "true")\
                .load(paths)
# rename the column to lowercase
column_mapping = {
    "VendorID": "vendorid",
    "PULocationID": "pulocationid",
    "DOLocationID": "dolocationid",
    "RatecodeID": "ratecodeid"
}
for old_col, new_col in column_mapping.items():
    if old_col in df.columns:
        df = df.withColumnRenamed(old_col, new_col)

df_payment_type = df.groupBy("payment_type").count()\
    .withColumnRenamed('count', 'trip_count')

# read the data from table cassandra
df_cassandra = spark.read\
    .format("org.apache.spark.sql.cassandra")\
        .options(table="payment_type_statistics", keyspace="statistics")\
            .load()

df_cassandra = df_cassandra.drop('payment_type_name')         

print('Data Frame from Cassandra')
df_cassandra.show()
# 1 = Credit card
# 2 = Cash
# 3 = No charge
# 4 = Dispute
# 5 = Unknown
# 6 = Voided trip

# create a spark dataframe
df_payment_type_references = spark.createDataFrame([
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip')
], ['payment_type', 'payment_type_name'])

print('Data Frame from References')
df_payment_type.show()

df_payment_type = df_payment_type.na.drop()

df_payment_type = df_payment_type.join(df_cassandra, 'payment_type', 'full')\
    .withColumn('total_trip',coalesce(col('trip_count')) + coalesce(col('count'), lit(0)) )\
        .drop('trip_count')\
            .drop('count')\
                .withColumnRenamed('total_trip', 'count')

df_payment_type = df_payment_type.join(df_payment_type_references, 'payment_type', 'left')
# add the partition_key column
df_payment_type = df_payment_type.withColumn('partition_key', lit(1))

print('Data Frame after join')
df_payment_type.show()

                
# write data to cassandra
df_payment_type.write \
    .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
            .options(table="payment_type_statistics", keyspace="statistics") \
                .save()
