# Real-Time Financial Fraud Detection Pipeline

A real-time financial fraud detection system built using Kafka, Apache Spark, Isolation Forest, Cassandra, Grafana, Docker, and Python.

The project processes financial transactions as streaming events, detects anomalous transactions using machine learning, stores the results in Cassandra, and visualizes system and fraud metrics through Grafana.

---

## Project Architecture

```text
PaySim Dataset
      │
      ▼
Python Transaction Producer
      │
      ▼
Apache Kafka
  Topic: transactions
      │
      ▼
Spark Structured Streaming
      │
      ├── Data Transformation
      │
      ├── Feature Preparation
      │
      ▼
Isolation Forest
      │
      ├── Normal Transactions
      │
      └── Anomaly / Fraud Candidates
              │
              ▼
          Cassandra
        ┌───────────────┐
        │ transactions  │
        │ fraud_alerts  │
        └───────────────┘
              │
              ▼
           Grafana
          Dashboard

#**Development Timeline**

Week 1 – Infrastructure & Streaming Setup
Day 1–3: Infrastructure Setup

The first week focused on establishing the infrastructure required for real-time processing.

Completed Tasks

Set up Docker and Docker Compose.
Configured Apache Kafka.
Configured Zookeeper.
Configured Apache Cassandra.
Configured Grafana.
Created the Kafka topic:

transactions

Verified communication between Kafka, Spark, and Cassandra.
Created the Python virtual environments for Windows and WSL development.

Infrastructure

Docker
 ├── Kafka
 ├── Zookeeper
 ├── Cassandra
 └── Grafana

Day 4–7: Python Transaction Producer

A Python producer was developed to read transactions from the PaySim dataset and publish them to Kafka.

Producer
producer/
└── paysim_producer.py

The producer:

Reads PaySim transaction data.
Converts transactions into JSON.
Sends transactions to Kafka.
Publishes them to the transactions topic.

Data Flow

PaySim CSV
    ↓
Python Producer
    ↓
Kafka
    ↓
transactions topic


Week 2 – Spark Streaming & Fraud Detection

The second week focused on building the real-time processing and machine-learning layer.

Day 1–3: Spark Structured Streaming

Apache Spark Structured Streaming was connected to Kafka.

The Spark application:

Reads transactions from Kafka.
Parses incoming JSON records.
Converts transaction fields into structured columns.
Processes transactions in micro-batches.
Prepares data for fraud detection.

Spark Application
streaming/
└── spark_fraud_scoring.py

Day 4–7: Machine Learning Fraud Detection

An Isolation Forest model was integrated into the streaming pipeline.

The model evaluates transaction features and identifies unusual transactions.

Prediction Logic
-1 → Anomaly / Fraud Candidate
 1 → Normal Transaction

The system calculates an anomaly score for each transaction.

Example:

Transactions received: 3087
Fraud candidates: 24

The detected anomaly information is then passed to Cassandra.

Week 3 – Cassandra Storage & Real-Time Alerting

The third week focused on storing processed transactions and fraud alerts.

Day 1–3: Cassandra Integration

Spark was connected to Cassandra using the Spark Cassandra Connector.

The project uses the following keyspace:

fraud_detection

Main Tables

transactions
fraud_alerts

Transactions Table

The transactions table stores processed transaction records.

Important fields include:

transaction_id
amount
transaction_type
is_flagged_fraud
is_fraud
new_balance_destination
new_balance_origin
old_balance_destination
old_balance_origin

Fraud Alerts Table

The fraud_alerts table stores transactions identified as anomaly candidates.

Important fields include:

transaction_id
amount
prediction
anomaly_score
is_flagged_fraud
is_fraud
alert_reason
event_time
processed_time

The alert reason used by the pipeline is:

Isolation Forest anomaly detected


Day 4–7: Validation & Real-Time Alert Testing

The streaming pipeline was tested using live Kafka transactions.

Cassandra was validated using:

USE fraud_detection;

SELECT COUNT(*) FROM transactions;

SELECT COUNT(*) FROM fraud_alerts;

Fraud-labelled alerts were also verified:

SELECT transaction_id,
       amount,
       prediction,
       is_fraud,
       alert_reason
FROM fraud_alerts
WHERE is_fraud = 1
ALLOW FILTERING;

Example validated fraud records included transactions with:

transaction_id: 10396
amount: 5.46e+06
prediction: -1
is_fraud: 1

and:

transaction_id: 4441
amount: 1e+07
prediction: -1
is_fraud: 1

Week 4 – Analytics Dashboarding & Optimization

The final week focused on monitoring, performance optimization, documentation, and testing.

Day 1–3: Grafana Dashboard

Grafana was connected to the underlying fraud-detection data.

The dashboard is designed to monitor:

Total transaction volume
Fraud/anomaly alerts
Confirmed fraud
Transaction amounts
Alert activity
Processing activity
Streaming system health

Dashboard Architecture

Cassandra
    │
    ▼
Grafana
    │
    ├── Transaction Metrics
    ├── Fraud Alerts
    ├── Fraud Count
    ├── Transaction Volume
    └── System Monitoring


Day 4–5: Kafka & Spark Optimization

The streaming application was tuned for the local development environment.

Spark Configuration
Spark Driver Memory: 2 GB
Spark Executor Memory: 2 GB
Spark Shuffle Partitions: 3
Streaming Trigger
Processing Time: 5 seconds

The application therefore processes incoming Kafka data using micro-batches at a 5-second processing interval.

Kafka partitions were also configured to support parallel transaction processing.

Day 6–7: Documentation & Load Testing

The final stage focused on completing the project documentation and validating the complete architecture.

Completed
Documented system architecture.
Documented Kafka → Spark → Cassandra data flow.
Documented machine-learning fraud detection.
Documented Cassandra storage.
Documented Grafana monitoring.
Validated Docker services.
Validated Kafka streaming.
Validated Spark processing.
Validated Cassandra writes.
Validated fraud alerts.
Performed simulated streaming/load testing.
Verified the project repository and GitHub documentation.


Final System

The completed system provides an end-to-end streaming fraud detection pipeline:

                ┌──────────────────┐
                │  PaySim Dataset  │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │ Python Producer  │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │  Apache Kafka    │
                │  transactions    │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │ Apache Spark     │
                │ Structured      │
                │ Streaming       │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │ Isolation Forest │
                │ Fraud Detection  │
                └────────┬─────────┘
                         │
                  ┌──────┴──────┐
                  │             │
                  ▼             ▼
          ┌──────────────┐ ┌──────────────┐
          │ Transactions │ │ Fraud Alerts │
          └──────┬───────┘ └──────┬───────┘
                 │                │
                 └───────┬────────┘
                         ▼
                ┌──────────────────┐
                │    Cassandra     │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │     Grafana      │
                │    Dashboard     │
                └──────────────────┘
Key Project Results

The completed pipeline successfully demonstrates:

Real-time transaction ingestion through Kafka.
Spark Structured Streaming micro-batch processing.
Machine-learning anomaly detection using Isolation Forest.
Transaction persistence in Cassandra.
Fraud-alert persistence in Cassandra.
Identification of fraud-labelled transactions.
Grafana-based monitoring and dashboarding.
Dockerized infrastructure.
Performance configuration and optimization.
End-to-end streaming validation.

Technologies Used

Technology	           Purpose
Python	              Transaction producer and processing
Apache Kafka	      Real-time message streaming
Apache Spark	      Stream processing
Isolation Forest       Anomaly detection
Apache Cassandra       Transaction and alert storage
Grafana  	       Dashboard and monitoring
Docker	               Containerization
Zookeeper	       Kafka coordination
WSL	               Linux development environment

Project Outcome

This project demonstrates an end-to-end real-time financial fraud detection architecture combining:

Data Engineering + Streaming + Machine Learning + Distributed Storage + Monitoring

The system is designed to process financial transactions continuously, identify anomalous behaviour, persist fraud alerts, and provide operational visibility through a monitoring dashboard.

Future Improvements
Improve Cassandra partition-key design.
Add advanced fraud-detection features.
Add model monitoring and automated retraining.
Add automated unit and integration tests.
Perform larger-scale load testing.
Improve Kafka partition tuning based on measured throughput.
Add detailed processing-latency metrics.
Deploy the complete architecture to a cloud environment.
Author

Najwah AP

Data Analytics / Data Engineering Portfolio Project