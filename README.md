**# Real-Time Financial Fraud Detection Pipeline**
A real-time streaming data pipeline designed to detect potentially fraudulent financial transactions.



**Day 1–3: Infrastructure**

The infrastructure consists of:


\- Apache Kafka

\- Zookeeper

\- Apache Cassandra

\- Docker



\## Architecture



```text

PaySim Dataset

    |

    v

Python Producer

     |

    v

Kafka Topic

"transactions"

    |

     v

Kafka Consumer

     |

     v

Cassandra

     |

    v

Fraud Detection \& Analytics

**Infrastructure**

**Apache Kafka**


Kafka is used as the real-time event streaming platform.

Topic:

transactions

Configuration:

Broker: localhost:9092

Partitions: 3

Replication Factor: 1

**Cassandra**
Cassandra is used as the distributed NoSQL database for storing transaction data.
Keyspace:fraud\_detection
Table:transactions

**Docker**
All infrastructure services are provisioned using Docker Compose.
Services:zookeeper
kafka
Cassandra

**Project Structure**

real time fraud detection/

│

├── data/

├── producer/

├── consumer/

├── cassandra/

├── kafka/

├── docker-compose.yml

└── README.md

**Current Progress**

*Docker infrastructure configured
* Zookeeper provisioned
* Apache Kafka provisioned
*Kafka transactions topic created
*Cassandra provisioned
* Cassandra keyspace created
* Transactions table created
* PaySim data producer
* Kafka consumer
* Fraud detection model
* Real-time dashboard



**Day 4–7: Python Producer.**

Day 4–7: Python Producer — PaySim Transaction Streaming


A Python-based Kafka producer was developed to simulate a continuous stream of financial transactions from the historical PaySim dataset.

The producer reads transactions from the PaySim CSV file, converts each transaction into a JSON message, and publishes the messages to the Kafka `transactions` topic.

#### Producer Architecture

```text
PaySim CSV Dataset
        │
        ▼
Python Kafka Producer
        │
        ├── Read transaction
        ├── Convert data types
        ├── Create JSON message
        └── Publish to Kafka
        │
        ▼
Kafka Broker
        │
        ▼
transactions Topic
```

#### Dataset Used

The producer uses the PaySim synthetic mobile-money transaction dataset.

Dataset file:

```text
data/
└── PS_20174392719_1491204439457_log.csv
```

The dataset contains:

* **6,362,620 transactions**
* Transaction type information
* Transaction amounts
* Origin account balances
* Destination account balances
* Fraud labels

The large CSV dataset is intentionally excluded from GitHub using `.gitignore`.

#### Kafka Configuration

The producer connects to the Kafka broker running locally through Docker.

```text
Kafka Broker: localhost:9092
Kafka Topic: transactions
```

The Kafka topic was created during the infrastructure setup.

Topic verification:

```powershell
docker exec -it fraud-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

The `transactions` topic was successfully created and verified.

#### Python Producer Implementation

The producer script is located at:

```text
producer/
└── paysim_producer.py
```

The producer uses:

* Python
* Pandas
* `kafka-python`
* JSON serialization

The main processing flow is:

```text
1. Locate PaySim CSV
        ↓
2. Load dataset using Pandas
        ↓
3. Read transactions sequentially
        ↓
4. Convert each row into a transaction dictionary
        ↓
5. Serialize transaction to JSON
        ↓
6. Send message to Kafka
        ↓
7. Wait briefly
        ↓
8. Process next transaction
```

#### Transaction Message Structure

Each PaySim transaction is converted into a JSON message containing the important transaction attributes.

Example structure:

```json
{
  "transaction_id": "0",
  "step": 1,
  "type": "PAYMENT",
  "amount": 9839.64,
  "nameOrig": "C1231006815",
  "oldbalanceOrg": 170136.0,
  "newbalanceOrig": 160296.36,
  "nameDest": "M1979787155",
  "oldbalanceDest": 0.0,
  "newbalanceDest": 0.0,
  "isFraud": 0,
  "isFlaggedFraud": 0
}
```

#### Producer Configuration

The Kafka producer is configured with JSON serialization:

```python
KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)
```

This allows Python dictionaries to be converted into JSON messages before they are published to Kafka.

#### Continuous Streaming Simulation

The PaySim dataset is historical batch data, so a small delay was introduced between messages to simulate real-time transaction arrival.

```python
STREAM_DELAY = 0.1
```

This means the producer waits approximately 0.1 seconds between transactions.

The producer can therefore simulate a continuous financial transaction stream:

```text
Transaction 1 → Kafka
       ↓
    0.1 sec
       ↓
Transaction 2 → Kafka
       ↓
    0.1 sec
       ↓
Transaction 3 → Kafka
       ↓
      ...
```

The producer continues processing transactions until:

* The dataset is completely processed, or
* The user stops the producer using `CTRL+C`.

#### Error Handling and Shutdown

The producer checks whether the PaySim dataset exists before starting.

If the file cannot be found, a `FileNotFoundError` is raised.

The streaming process also handles `CTRL+C` using `KeyboardInterrupt`.

Before shutting down, the producer:

1. Flushes pending Kafka messages.
2. Closes the Kafka producer connection.
3. Prints a confirmation message.

This helps ensure that buffered messages are sent before the producer exits.

#### Running the Producer

Make sure the virtual environment is active:

```powershell
.\.venv\Scripts\Activate.ps1
```

Start the Kafka and Cassandra infrastructure:

```powershell
docker compose up -d
```

Then run the producer:

```powershell
python producer\paysim_producer.py
```

Expected output:

```text
Loading PaySim dataset...
Loaded 6,362,620 transactions.
Kafka topic: transactions
Starting transaction stream...
Press CTRL+C to stop.

Sent transaction 1 | Type: PAYMENT | Amount: 9839.64 | Fraud: 0
Sent transaction 2 | Type: PAYMENT | Amount: 1864.28 | Fraud: 0
Sent transaction 3 | Type: TRANSFER | Amount: 181.00 | Fraud: 1
...
```

The producer can be stopped safely with:

```text
CTRL+C
```

#### Kafka Message Verification

After starting the producer, Kafka messages were verified using the Kafka console consumer.

Command:

```powershell
docker exec -it fraud-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions --from-beginning --max-messages 5
```

Kafka successfully returned transaction messages in JSON format.

This confirmed that:

* The Python producer successfully connected to Kafka.
* Transactions were successfully serialized into JSON.
* Messages were successfully published to the `transactions` topic.
* Kafka successfully stored the streamed transaction messages.

#### Streaming Test

During testing, the producer successfully streamed **2,736 transactions** before being stopped manually with `CTRL+C`.

The Kafka consumer was then used to read messages from the `transactions` topic, confirming successful end-to-end message delivery.

#### Files Added

```text
producer/
└── paysim_producer.py
```

#### Dependencies

The Python producer requires:

```text
pandas
kafka-python
```

These dependencies are listed in:

```text
requirements.txt
```

Install the project dependencies with:

```powershell
pip install -r requirements.txt
```

#### Day 4–7 Completion Status

| Task                            | Status      |
| ------------------------------- | ----------- |
| Load PaySim dataset             | ✅ Completed |
| Create Python Kafka producer    | ✅ Completed |
| Convert transactions to JSON    | ✅ Completed |
| Create Kafka transaction stream | ✅ Completed |
| Publish to `transactions` topic | ✅ Completed |
| Simulate continuous streaming   | ✅ Completed |
| Verify Kafka messages           | ✅ Completed |
| Handle graceful shutdown        | ✅ Completed |
| Add producer to GitHub project  | ✅ Completed |

**Day 4–7 Result:** A functional Python streaming producer was successfully implemented and verified. Historical PaySim transactions can now be continuously published to the Kafka `transactions` topic, providing the real-time data ingestion layer for the fraud detection pipeline.

 Week 2: Batch Analysis & Model Training

### Day 1–3: Historical Batch Analysis & Isolation Forest

The historical PaySim dataset was used to train an unsupervised anomaly detection model.

#### Dataset

- Total historical transactions: 6,362,620
- Training sample: 100,000 transactions
- Sampling method: Random sampling
- Random state: 42

#### Features Used

The following transaction and balance features were selected:

- `amount`
- `oldbalanceOrg`
- `newbalanceOrig`
- `oldbalanceDest`
- `newbalanceDest`

#### Data Processing

1. Loaded the historical PaySim CSV dataset.
2. Selected a reproducible sample of 100,000 transactions.
3. Converted feature values to numeric format.
4. Handled missing and infinite values.
5. Standardized features using `StandardScaler`.

#### Anomaly Detection Model

An `IsolationForest` model was trained with:

- Number of estimators: 100
- Contamination: 0.01
- Random state: 42
- Parallel processing: enabled

The trained model and scaler are generated locally in:

```text
model/artifacts/

Week 2 Architecture

Historical PaySim CSV
        ↓
Data Sampling
        ↓
Feature Selection
        ↓
Data Cleaning
        ↓
StandardScaler
        ↓
Isolation Forest
        ↓
Anomaly Detection