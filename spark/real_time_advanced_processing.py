import findspark
findspark.init()
import pyspark
import os
import sys
from env_variable import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, coalesce, lit, year, month, dayofmonth, dayofyear, hour, udf, expr, to_date, window, count
from pyspark.sql.types import StringType, StructType, DoubleType, IntegerType, StructField

# Set up environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder \
    .appName('Advanced Spark Streaming with Cassandra') \
    .config('spark.sql.shuffle.partitions', 200) \
    .config('spark.sql.streaming.stateStore.compression.codec', 'lz4') \
    .config('spark.cassandra.connection.host', 'localhost') \
    .getOrCreate()

# Define schema for the incoming JSON
schema = StructType([
    StructField("vendorid", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("ratecodeid", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("pulocationid", IntegerType(), True),
    StructField("dolocationid", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True)
])

# Read streaming data from Kafka
streaming_data = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", 'test1') \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON from Kafka into a DataFrame
parsed_df = streaming_data.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# **1. Passenger Statistics with Stateful Aggregation**
def update_passenger_count(batch_df, batch_id):
    # Aggregate per batch
    passenger_counts = batch_df.groupBy("passenger_count").count().alias("batch_count")

    # Use `foreachBatch` for maintaining states
    def update_state(batch_passenger_df, epoch_id):
        batch_passenger_df.createOrReplaceTempView("current_batch")
        updated_query = """
            MERGE INTO statistics.passenger_number_statistics AS target
            USING (SELECT * FROM current_batch) AS source
            ON target.passenger_number = source.passenger_count
            WHEN MATCHED THEN UPDATE SET target.count = target.count + source.batch_count
            WHEN NOT MATCHED THEN INSERT VALUES (source.passenger_count, source.batch_count)
        """
        spark.sql(updated_query)

    passenger_counts.writeStream.foreachBatch(update_state).outputMode("update").start()


