
CREATE KEYSPACE IF NOT EXISTS fraud_detection
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE fraud_detection;

CREATE TABLE IF NOT EXISTS fraud_alerts (
    transaction_id text,
    event_time timestamp,
    step int,
    type text,
    amount double,
    name_orig text,
    old_balance_org double,
    new_balance_orig double,
    name_dest text,
    old_balance_dest double,
    new_balance_dest double,
    is_fraud int,
    is_flagged_fraud int,
    anomaly_score double,
    prediction int,
    alert_reason text,
    PRIMARY KEY (transaction_id)
);

