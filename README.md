**# Real-Time Financial Fraud Detection Pipeline**



A real-time streaming data pipeline designed to detect potentially fraudulent financial transactions.



&#x20;**Day 1–3: Infrastructure**



The infrastructure consists of:



\- Apache Kafka

\- Zookeeper

\- Apache Cassandra

\- Docker



\## Architecture



```text

PaySim Dataset

&#x20;     |

&#x20;     v

Python Producer

&#x20;     |

&#x20;     v

Kafka Topic

"transactions"

&#x20;     |

&#x20;     v

Kafka Consumer

&#x20;     |

&#x20;     v

Cassandra

&#x20;     |

&#x20;     v

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



Keyspace:



fraud\_detection



Table:



transactions



**Docker**



All infrastructure services are provisioned using Docker Compose.



Services:



zookeeper

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

* &#x20;Docker infrastructure configured
* &#x20;Zookeeper provisioned
* &#x20;Apache Kafka provisioned
* &#x20;Kafka transactions topic created
* &#x20;Cassandra provisioned
* &#x20;Cassandra keyspace created
* &#x20;Transactions table created
* &#x20;PaySim data producer
* &#x20;Kafka consumer
* &#x20;Fraud detection model
* &#x20;Real-time dashboard



Save and close.



\---



\# Step 10 — Check Git



Run:



```powershell

git status



**Day 4–7: Python Producer.**

