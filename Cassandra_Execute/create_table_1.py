from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

session.execute("""drop keyspace if exists test""")

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS test
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")

# Connect to the keyspace
session.set_keyspace('test')

#Drop table if exists
session.execute("DROP TABLE IF EXISTS tripdata")

# Create table
session.execute("""
    CREATE TABLE IF NOT EXISTS tripdata (
        id UUID PRIMARY KEY,
        VendorID int,
        tpep_pickup_datetime timestamp,
        tpep_dropoff_datetime timestamp,
        passenger_count int,
        trip_distance float,
        RatecodeID int,
        store_and_fwd_flag text,
        PULocationID int,
        DOLocationID int,
        payment_type int,
        fare_amount float,
        extra float,
        mta_tax float,
        tip_amount float,
        tolls_amount float,
        improvement_surcharge float,
        total_amount float,
        trip_duration int,
        congestion_surcharge float
    )
""")
print("Table created successfully")

cluster.shutdown()