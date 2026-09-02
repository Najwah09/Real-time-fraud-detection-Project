from kafka import KafkaProducer
import pandas as pd
import json
import time

df = pd.read_csv("data/PS_20174392719_1491204439457_log.csv")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for _, row in df.iterrows():
    transaction = row.to_dict()

    producer.send(
        "transactions",
        value=transaction
    )

    print("Sent:", transaction)

    time.sleep(0.1)

producer.flush()