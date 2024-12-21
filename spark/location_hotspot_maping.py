import findspark
findspark.init()

import uuid
import pyspark
import os
import sys
from env_variable import *
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, broadcast
import folium
from folium.plugins import HeatMap
import pandas as pd

# Environment settings
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

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

zones = spark.read.csv('Taxi_Data/taxi_zone_lookup_coordinates.csv', header=True, inferSchema=True)

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
            
df_pickup = df.join(broadcast(zones), df.pulocationid == zones.LocationID, 'left')\
    .select(col('pulocationid'), col('latitude').alias('pickup_latitude'), col('longitude').alias('pickup_longitude'))
    
df_dropoff = df.join(broadcast(zones), df.dolocationid == zones.LocationID, 'left')\
    .select(col('dolocationid'), col('latitude').alias('dropoff_latitude'), col('longitude').alias('dropoff_longitude'))

df_pickup.na.drop()
df_dropoff.na.drop()
# Filter for valid latitude and longitude values
df_pickup = df_pickup.filter(
    (col("pickup_latitude").isNotNull()) &
    (col("pickup_longitude").isNotNull()) &
    (col("pickup_latitude").cast("float").isNotNull()) &  # Ensure convertible to float
    (col("pickup_longitude").cast("float").isNotNull()) &  # Ensure convertible to float
    (col("pickup_latitude").between(-90, 90)) &
    (col("pickup_longitude").between(-180, 180))
)
df_dropoff = df_dropoff.filter(
    (col("dropoff_latitude").isNotNull()) &
    (col("dropoff_longitude").isNotNull()) &
    (col("dropoff_latitude").cast("float").isNotNull()) &  # Ensure convertible to float
    (col("dropoff_longitude").cast("float").isNotNull()) &  # Ensure convertible to float
    (col("dropoff_latitude").between(-90, 90)) &
    (col("dropoff_longitude").between(-180, 180))
)

df_pickup_pd = df_pickup.toPandas()
df_dropoff_pd = df_dropoff.toPandas()
# Ensure data types are numeric in Pandas
df_pickup_pd["pickup_latitude"] = pd.to_numeric(df_pickup_pd["pickup_latitude"], errors="coerce")
df_pickup_pd["pickup_longitude"] = pd.to_numeric(df_pickup_pd["pickup_longitude"], errors="coerce")
df_dropoff_pd["dropoff_latitude"] = pd.to_numeric(df_dropoff_pd["dropoff_latitude"], errors="coerce")
df_dropoff_pd["dropoff_longitude"] = pd.to_numeric(df_dropoff_pd["dropoff_longitude"], errors="coerce")
# Drop rows with NaN values after conversion
df_pickup_pd = df_pickup_pd.dropna(subset=["pickup_latitude", "pickup_longitude"])
df_dropoff_pd = df_dropoff_pd.dropna(subset=["dropoff_latitude", "dropoff_longitude"])
# Create a base map
map = folium.Map(location=[40.7128, -74.0060], zoom_start=12)
custom_gradient = {
    0.2: 'blue',
    0.4: 'lime',
    0.6: 'orange',
    1.0: 'red'
}
# Add heatmap
HeatMap(data=df_pickup_pd[["pickup_latitude", "pickup_longitude"]].values.tolist(), radius=8).add_to(map)
# HeatMap(data=df_dropoff_pd[["dropoff_latitude", "dropoff_longitude"]].values.tolist(), radius=8).add_to(map)
# Save the map to an HTML file
map.save("pickup_heatmap.html")

print("Heatmap saved to 'pickup_heatmap.html'")
