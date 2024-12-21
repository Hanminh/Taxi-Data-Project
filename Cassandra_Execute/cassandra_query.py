from cassandra.cluster import Cluster
from datetime import datetime
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()


session.set_keyspace('statistics')

session.execute('drop table if exists total_amount_statistics')
session.execute("""
    CREATE TABLE IF NOT EXISTS total_amount_statistics (
        year text,
        day text,
        month text,
        total_amount float,
        timestamp timestamp,
        partition_key int,
        PRIMARY KEY (partition_key, year, day, month)
    )
""")

print("All table created successfully")

#select data from table
rows = session.execute('select * from total_amount_statistics')

rows = sorted(rows, key= lambda x: (x.year, x.month, x.day))
for row in rows:
    print(row.year, row.month,row.day, row.total_amount, row.timestamp)

cluster.shutdown()

# print('Data updated successfully')