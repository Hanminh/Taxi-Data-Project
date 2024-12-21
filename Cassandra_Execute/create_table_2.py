from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

session.execute("""drop keyspace if exists statistics""")

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS statistics
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")

# Connect to the keyspace
session.set_keyspace('statistics')



# Create table
# root
#  |-- passenger_number: long (nullable = true)
#  |-- count: long (nullable = true)

# root
#  |-- payment_type: long (nullable = true)
#  |-- count: long (nullable = true)

# root
#  |-- pulocationid: integer (nullable = true)
#  |-- locationid: integer (nullable = true)
#  |-- borough: string (nullable = true)
#  |-- zone: string (nullable = true)
#  |-- service_zone: string (nullable = true)

# root
#  |-- dolocationid: integer (nullable = true)
#  |-- locationid: integer (nullable = true)
#  |-- borough: string (nullable = true)
#  |-- zone: string (nullable = true)
#  |-- service_zone: string (nullable = true)

#Drop table if exists
session.execute("DROP TABLE IF EXISTS passenger_number_statistics")

session.execute("""
    CREATE TABLE IF NOT EXISTS passenger_number_statistics (
        passenger_number text,
        count int,
        partition_key int,
        primary key (partition_key, passenger_number)
    )
""")

session.execute("DROP TABLE IF EXISTS payment_type_statistics")

session.execute("""
    CREATE TABLE IF NOT EXISTS payment_type_statistics (
        payment_type int,
        count int ,
        payment_type_name text,
        partition_key int,
        primary key (partition_key, payment_type)
    )
""")

session.execute("DROP TABLE IF EXISTS pickup_location_statistics")
session.execute("""
    CREATE TABLE IF NOT EXISTS pickup_location_statistics (
        pulocationid int,
        borough text,
        zone text,
        service_zone text,
        partition_key int,
        PRIMARY KEY (partition_key, pulocationid)
    )
""")

session.execute("DROP TABLE IF EXISTS dropoff_location_statistics")
session.execute("""
    CREATE TABLE IF NOT EXISTS dropoff_location_statistics (
        dolocationid int ,
        borough text,
        zone text,
        service_zone text,
        partition_key int,
        PRIMARY KEY (partition_key, dolocationid)
    )
""")

session.execute("DROP TABLE IF EXISTS pickup_hour_statistics")
session.execute("""
    CREATE TABLE IF NOT EXISTS pickup_hour_statistics (
        pickup_hour text ,
        count int,
        partition_key int,
        PRIMARY KEY (partition_key, pickup_hour)
    )
""")

session.execute('drop table if exists trip_distance_statistics')
session.execute("""
    CREATE TABLE IF NOT EXISTS trip_distance_statistics (
        trip_distance float PRIMARY KEY,
        count int
    )
""")

session.execute('drop table if exists df_pickup_hour_statistics')
session.execute("""
    CREATE TABLE IF NOT EXISTS df_pickup_hour_statistics (
        pickup_hour int ,
        count int,
        partition_key int,
        PRIMARY KEY (partition_key, pickup_hour)
    )
""")

# create table with primary key (month, year)
session.execute('drop table if exists total_amount_statistics')
session.execute("""
    CREATE TABLE IF NOT EXISTS total_amount_statistics (
        year text,
        week text,
        total_amount float,
        partition_key int,
        PRIMARY KEY (partition_key, year, week)
    )
""")

print("All table created successfully")

cluster.shutdown()