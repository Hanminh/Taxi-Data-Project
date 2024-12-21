import findspark
findspark.init()
import pyspark
import os
import sys
from env_variable import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, coalesce, lit, year, month, dayofmonth, dayofyear, hour, udf, expr, to_date
from pyspark.sql.types import StringType, StructType, DoubleType, IntegerType, StructField

# Set up environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder \
    .appName('Kafka Streaming to Cassandra') \
    .config('spark.default.parallelism', 100) \
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

# Process passenger count statistics
def process_passenger_statistics(batch_df, batch_id):
    passenger_df = batch_df.groupBy("passenger_count").count().withColumnRenamed("count", "trip_count")\
        .withColumnRenamed("passenger_count", "passenger_number")

    df_cassandra = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="passenger_number_statistics", keyspace="statistics") \
        .load()

    updated_df = passenger_df.join(df_cassandra, "passenger_count", "full") \
        .withColumn("total_count", coalesce(col("trip_count"), lit(0)) + coalesce(col("count"), lit(0))) \
        .drop("trip_count").drop("count") \
        .withColumnRenamed("total_count", "count")

    updated_df = updated_df.withColumn("partition_key", lit(1))
    updated_df = updated_df.na.drop()
    updated_df = updated_df.withColumn("passenger_number", updated_df["passenger_number"].cast("int"))

    updated_df.write.format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="passenger_number_statistics", keyspace="statistics") \
        .save()

# Process payment type statistics
def process_payment_type_statistics(batch_df, batch_id):
    payment_type_df = batch_df.groupBy("payment_type").count().withColumnRenamed("count", "trip_count")

    df_cassandra = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="payment_type_statistics", keyspace="statistics") \
        .load()

    df_cassandra = df_cassandra.drop("payment_type_name")

    payment_type_ref = spark.createDataFrame([
        (1, 'Credit card'),
        (2, 'Cash'),
        (3, 'No charge'),
        (4, 'Dispute'),
        (5, 'Unknown'),
        (6, 'Voided trip')
    ], ["payment_type", "payment_type_name"])

    updated_df = payment_type_df.join(df_cassandra, "payment_type", "full") \
        .withColumn("total_trip", coalesce(col("trip_count"), lit(0)) + coalesce(col("count"), lit(0))) \
        .drop("trip_count").drop("count") \
        .withColumnRenamed("total_trip", "count")

    updated_df = updated_df.join(payment_type_ref, "payment_type", "left")
    updated_df = updated_df.withColumn("partition_key", lit(1))

    updated_df.write.format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="payment_type_statistics", keyspace="statistics") \
        .save()

def process_total_amount_statistics(batch_df, batch_id):
    total_amount_df = batch_df.groupBy(
        year("tpep_dropoff_datetime").alias("year"),
        month("tpep_dropoff_datetime").alias("month"),
        dayofmonth("tpep_dropoff_datetime").alias("day")
    ).agg({"total_amount": "sum"})
    
    df_cassandra = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="total_amount_statistics", keyspace="statistics") \
        .load()
    
    updated_df = total_amount_df.join(df_cassandra, ['year', 'month', 'day'], 'full')\
        .withColumn('final_amount', coalesce(col('sum(total_amount)')) + coalesce(col('total_amount'), lit(0)))\
            .drop('sum(total_amount)')\
                .drop('total_amount')\
                    .withColumnRenamed('final_amount', 'total_amount')
    
    updated_df = updated_df.withColumn("timestamp", to_date(expr("concat(year, '-', month, '-', day)")))
    
    updated_df = updated_df.withColumn('partition_key', lit(1))
    
    updated_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="total_amount_statistics", keyspace="statistics") \
        .save()

def process_peak_pickup_hour_statistics(batch_df, batch_id):
    df_pickup_hour = batch_df.withColumn('pickup_hour', hour('tpep_pickup_datetime'))\
        .groupBy('pickup_hour')\
            .count()\
                .withColumnRenamed('count', 'pickup_count')\
                    .sort('pickup_hour')
                    
    df_cassandra = spark.read\
        .format("org.apache.spark.sql.cassandra")\
            .options(table="pickup_hour_statistics", keyspace="statistics")\
                .load()
                
    df_pickup_hour = df_pickup_hour.join(df_cassandra, 'pickup_hour', 'left')\
        .withColumn('total_pickup', col('pickup_count') + coalesce(col('count'), lit(0)) )\
            .drop('pickup_count')\
                .drop('count')\
                    .withColumnRenamed('total_pickup', 'count')
                    
    df_pickup_hour = df_pickup_hour.withColumn('partition_key', lit(1))
    
    def format_hour(hour):
        return f"{hour:02}:00"
    
    format_hour_udf = udf(format_hour, StringType())
    df_pickup_hour = df_pickup_hour.withColumn('pickup_hour', format_hour_udf('pickup_hour'))
    
    # write the data to cassandra
    df_pickup_hour.write \
        .format("org.apache.spark.sql.cassandra") \
            .mode('append') \
                .options(table="pickup_hour_statistics", keyspace="statistics") \
                    .save()
    

# Start streaming query for passenger count statistics
query_passenger = parsed_df.writeStream \
    .foreachBatch(process_passenger_statistics) \
    .outputMode("update") \
    .start()

# Start streaming query for payment type statistics
query_payment_type = parsed_df.writeStream \
    .foreachBatch(process_payment_type_statistics) \
    .outputMode("update") \
    .start()
    
# Start streaming query for total amount statistics
query_total_amount = parsed_df.writeStream \
    .foreachBatch(process_total_amount_statistics) \
    .outputMode("update") \
    .start()
    
# Start streaming query for peak pickup hour statistics
query_peak_pickup_hour = parsed_df.writeStream \
    .foreachBatch(process_peak_pickup_hour_statistics) \
    .outputMode("update") \
    .start()

query_passenger.awaitTermination()
query_payment_type.awaitTermination()
query_total_amount.awaitTermination()
query_peak_pickup_hour.awaitTermination()