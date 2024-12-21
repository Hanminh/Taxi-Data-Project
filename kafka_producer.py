from kafka import KafkaProducer, KafkaConsumer
from hdfs import InsecureClient
from env_variable import *
import time
import json
import os
import pandas as pd

BATCH_SIZE = 100000

print('Kafka Producer is running...')

def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        retries=5,
        retry_backoff_ms=1000,
        request_timeout_ms=30000,
        batch_size= 16000
    )
    return producer

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic: {msg.topic()}, partition: {msg.partition()}, offset: {msg.offset()}")


def send_csv_to_kafka(csv_file, batch_size= BATCH_SIZE):
    for df in pd.read_csv(csv_file, chunksize=batch_size):
        batch = df.to_dict(orient="records")
        producer = create_producer()
        try:
            for record in batch:
                producer.send(KAFKA_TOPIC, value=record)
                # print(f"Sent record: {record}")
                producer.flush()
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            producer.flush()
            print('Batch sent successfully')
        # time.sleep(1)
    

if __name__ == "__main__":
    # make an array of all csv files in Taxi Data Folder
    list_dir = os.listdir("Taxi_Data")
    list_csv_file = list_dir[3:]
    list_csv_file = list_csv_file[-4:]
    #remove the last file in list_csv_file
    list_csv_file = list_csv_file[:-1]
    for path in list_csv_file:
        send_csv_to_kafka(f"Taxi_Data/{path}")
        print(f"Sent {path} to Kafka")