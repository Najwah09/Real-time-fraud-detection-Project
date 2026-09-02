import json
import os
import time

import pandas as pd
from kafka import KafkaProducer


# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions"

# PaySim dataset location
DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "data",
    "PS_20174392719_1491204439457_log.csv"
)

# Delay between transactions
STREAM_DELAY = 0.1


def create_producer():
    """Create and return a Kafka producer."""

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def stream_transactions():
    """Read PaySim transactions and send them to Kafka."""

    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(
            f"PaySim dataset not found at: {DATA_PATH}"
        )

    print("Loading PaySim dataset...")

    df = pd.read_csv(DATA_PATH)

    print(f"Loaded {len(df):,} transactions.")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print("Starting transaction stream...")
    print("Press CTRL+C to stop.")

    producer = create_producer()

    try:
        for index, row in df.iterrows():

            transaction = {
                "transaction_id": str(index),
                "step": int(row["step"]),
                "type": str(row["type"]),
                "amount": float(row["amount"]),
                "nameOrig": str(row["nameOrig"]),
                "oldbalanceOrg": float(row["oldbalanceOrg"]),
                "newbalanceOrig": float(row["newbalanceOrig"]),
                "nameDest": str(row["nameDest"]),
                "oldbalanceDest": float(row["oldbalanceDest"]),
                "newbalanceDest": float(row["newbalanceDest"]),
                "isFraud": int(row["isFraud"]),
                "isFlaggedFraud": int(row["isFlaggedFraud"]),
            }

            producer.send(
                KAFKA_TOPIC,
                value=transaction
            )

            print(
                f"Sent transaction {index + 1:,} | "
                f"Type: {transaction['type']} | "
                f"Amount: {transaction['amount']:.2f} | "
                f"Fraud: {transaction['isFraud']}"
            )

            time.sleep(STREAM_DELAY)

    except KeyboardInterrupt:
        print("\nTransaction streaming stopped.")

    finally:
        producer.flush()
        producer.close()
        print("Kafka producer closed.")


if __name__ == "__main__":
    stream_transactions()