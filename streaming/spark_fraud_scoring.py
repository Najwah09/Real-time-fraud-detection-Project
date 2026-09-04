import os
import joblib

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions"

CHECKPOINT_LOCATION = "/home/najwah/fraud_scoring_checkpoint"

MODEL_PATH = "model/artifacts/isolation_forest.pkl"
SCALER_PATH = "model/artifacts/scaler.pkl"


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():

    return (
        SparkSession.builder
        .appName("RealTimeFraudScoring")
        .master("local[*]")
 	.config(
    	    "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,"
    	    "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1"
	)
        .config(
            "spark.sql.streaming.forceDeleteTempCheckpointLocation",
            "true"
        )
        .config(
            "spark.hadoop.fs.file.impl.disable.cache",
            "true"
        )
        .getOrCreate()
    )


# ============================================================
# TRANSACTION SCHEMA
# ============================================================

def create_transaction_schema():

    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("step", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("nameOrig", StringType(), True),
        StructField("oldbalanceOrg", DoubleType(), True),
        StructField("newbalanceOrig", DoubleType(), True),
        StructField("nameDest", StringType(), True),
        StructField("oldbalanceDest", DoubleType(), True),
        StructField("newbalanceDest", DoubleType(), True),
        StructField("isFraud", IntegerType(), True),
        StructField("isFlaggedFraud", IntegerType(), True),
    ])


# ============================================================
# LOAD MODEL
# ============================================================

def load_model():

    print("\nLoading trained Isolation Forest model...")

    model = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)

    print("Isolation Forest model loaded successfully.")
    print("Scaler loaded successfully.")
    print(f"Model features: {model.n_features_in_}")
    print(f"Scaler features: {scaler.n_features_in_}")

    return model, scaler


# ============================================================
# FRAUD SCORING
# ============================================================
def load_model():

    print("\nLoading trained Isolation Forest model...")

    model = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)

    print("Isolation Forest model loaded successfully.")
    print("Scaler loaded successfully.")
    print(f"Model features: {model.n_features_in_}")
    print(f"Scaler features: {scaler.n_features_in_}")

    return model, scaler

def process_batch(batch_df, batch_id, model, scaler):

    print("\n" + "=" * 70)
    print(f"PROCESSING BATCH: {batch_id}")
    print("=" * 70)

    if batch_df.isEmpty():
        print("No transactions in this batch.")
        return

    # Convert Spark DataFrame to Pandas
    pandas_df = batch_df.toPandas()

    # Features used for scoring

    # Make sure required columns exist
        # Features used for scoring
    # Must match the 5 features used during model training
    feature_columns = [
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
    ]

    # Make sure required columns exist
    missing_columns = [
        column
        for column in feature_columns
        if column not in pandas_df.columns
    ]

    if missing_columns:
        print(f"Missing columns: {missing_columns}")
        return

    # Fill missing numeric values
    pandas_df[feature_columns] = (
        pandas_df[feature_columns]
        .fillna(0)
    )

    # Scale features
    X_scaled = scaler.transform(
        pandas_df[feature_columns]
    )

    # Isolation Forest prediction
    predictions = model.predict(X_scaled)

    # Anomaly score
    anomaly_scores = model.decision_function(X_scaled)

    # -1 = anomaly/fraud candidate
    #  1 = normal transaction
    pandas_df["prediction"] = predictions
    pandas_df["anomaly_score"] = anomaly_scores

    fraud_df = pandas_df[
        pandas_df["prediction"] == -1
    ].copy()

    print(f"Transactions received: {len(pandas_df)}")
    print(f"Fraud candidates: {len(fraud_df)}")

    if len(fraud_df) == 0:
        print("No fraud candidates detected.")
        return

    # ========================================================
    # WRITE FRAUD ALERTS TO CASSANDRA
    # ========================================================

    print("\nFraud candidates detected:")

    print(
        fraud_df[
            [
                "transaction_id",
                "type",
                "amount",
                "prediction",
                "anomaly_score",
                "isFraud",
            ]
        ].to_string(index=False)
    )

    # Convert back to Spark DataFrame
    fraud_spark_df = spark.createDataFrame(fraud_df)

    fraud_alerts = (
        fraud_spark_df
        .withColumn(
            "event_time",
            current_timestamp()
        )
        .withColumn(
            "alert_reason",
            lit("Isolation Forest anomaly detected")
        )
        .select(
            "transaction_id",
            "alert_reason",
            "amount",
            "anomaly_score",
            "event_time",
            "isFlaggedFraud",
            "isFraud",
            "nameDest",
            "nameOrig",
            "newbalanceDest",
            "newbalanceOrig",
            "oldbalanceDest",
            "oldbalanceOrg",
            "prediction",
            "step",
            "type",
        )
    )

    fraud_alerts = (
        fraud_alerts
        .withColumnRenamed("isFlaggedFraud", "is_flagged_fraud")
        .withColumnRenamed("isFraud", "is_fraud")
        .withColumnRenamed("nameDest", "name_dest")
        .withColumnRenamed("nameOrig", "name_orig")
        .withColumnRenamed("newbalanceDest", "new_balance_dest")
        .withColumnRenamed("newbalanceOrig", "new_balance_orig")
        .withColumnRenamed("oldbalanceDest", "old_balance_dest")
        .withColumnRenamed("oldbalanceOrg", "old_balance_org")
    )
    (
        fraud_alerts
        .write
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", "fraud_detection")
        .option("table", "fraud_alerts")
        .mode("append")
        .save()
    )

    print(
        f"\nSUCCESS: {len(fraud_df)} fraud alert(s) "
        "written to Cassandra."
    )


# ============================================================
# START STREAMING
# ============================================================

def start_streaming():

    print("=" * 70)
    print("REAL-TIME FRAUD DETECTION")
    print("Spark + Kafka + Isolation Forest + Cassandra")
    print("=" * 70)

    global spark

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    model, scaler = load_model()

    schema = create_transaction_schema()

    print("\nConnecting to Kafka...")
    print(f"Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic : {KAFKA_TOPIC}")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )
        .option(
            "subscribe",
            KAFKA_TOPIC
        )
        .option(
            "startingOffsets",
            "latest"
        )
        .load()
    )

    transactions = (
        kafka_df
        .selectExpr(
            "CAST(value AS STRING) AS json_value"
        )
        .select(
            from_json(
                col("json_value"),
                schema
            ).alias("transaction")
        )
        .select("transaction.*")
    )

    print("\nStarting fraud scoring stream...")
    print("Processing mode: micro-batch")
    print("Trigger interval: 5 seconds")
    print("Press CTRL+C to stop.")

    query = (
        transactions
        .writeStream
        .foreachBatch(
            lambda df, batch_id:
            process_batch(
                df,
                batch_id,
                model,
                scaler
            )
        )
        .option(
            "checkpointLocation",
            CHECKPOINT_LOCATION
        )
        .trigger(
            processingTime="5 seconds"
        )
        .start()
    )

    query.awaitTermination()


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    start_streaming()
