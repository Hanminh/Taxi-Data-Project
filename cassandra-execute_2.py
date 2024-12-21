from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# select first 5 rows from the table
row = session.execute("""
    select * from test.tripdata limit 20
""")

for r in row:
    print(r)