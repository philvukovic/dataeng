import csv
import json
import pandas as pd
from kafka import KafkaProducer

from time import time

def main():
    begin = time()

    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # HW 6 Q5
    schema = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount']

    csv_file = 'data/green_tripdata_2019-10.csv'  # change to your CSV file path if needed
    df = pd.read_csv(csv_file, usecols=schema)

    for _, row in df.iterrows():
        # Each row will be a dictionary keyed by the CSV headers
        # Send data to Kafka topic "green-data"
        producer.send('green-data-2', value=dict(row))

    # Make sure any remaining messages are delivered
    producer.flush()
    producer.close()
    print(time() - begin)

if __name__ == "__main__":
    main()
