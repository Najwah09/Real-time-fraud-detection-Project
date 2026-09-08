import os
import joblib

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    current_timestamp,
    when,
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

CHECKPOINT_LOCATION = (
    "/tmp/fraud_detection_checkpoint"
)

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
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "3")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,"
            "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1"
        )
        .config(
            "spark.sql.adaptive.enabled",
            "false"
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
# PROCESS BATCH
# ============================================================

def process_batch(batch_df, batch_id, model, scaler):

    print("\n" + "=" * 70)
    print(f"PROCESSING BATCH: {batch_id}")
    print("=" * 70)

    if batch_df.isEmpty():
        print("No transactions in this batch.")
        return

    # --------------------------------------------------------
    # Convert Spark DataFrame to Pandas
    # --------------------------------------------------------

    pandas_df = batch_df.toPandas()

    # --------------------------------------------------------
    # MODEL FEATURES
    # --------------------------------------------------------

    feature_columns = [
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
    ]

    missing_columns = [
        column
        for column in feature_columns
        if column not in pandas_df.columns
    ]

    if missing_columns:

        print(
            f"Missing columns: {missing_columns}"
        )

        return

    # --------------------------------------------------------
    # HANDLE MISSING VALUES
    # --------------------------------------------------------

    pandas_df[feature_columns] = (
        pandas_df[feature_columns]
        .fillna(0)
    )

    # --------------------------------------------------------
    # SCALE FEATURES
    # --------------------------------------------------------

    X_scaled = scaler.transform(
        pandas_df[feature_columns]
    )

    # --------------------------------------------------------
    # ISOLATION FOREST
    # --------------------------------------------------------

    predictions = model.predict(X_scaled)

    anomaly_scores = (
        model.decision_function(X_scaled)
    )

    pandas_df["prediction"] = predictions
    pandas_df["anomaly_score"] = anomaly_scores

    # -1 = anomaly
    #  1 = normal

    fraud_df = pandas_df[
        pandas_df["prediction"] == -1
    ].copy()

    print(
        f"Transactions received: {len(pandas_df)}"
    )

    print(
        f"Fraud candidates: {len(fraud_df)}"
    )

    # ========================================================
    # WRITE ALL TRANSACTIONS TO CASSANDRA
    # ========================================================

    print(
        "\nWriting ALL transactions to Cassandra..."
    )

    all_transactions_spark_df = (
        spark.createDataFrame(pandas_df)
    )

    all_transactions = (
        all_transactions_spark_df

        # Convert 0/1 to boolean
        .withColumn(
            "is_flagged_fraud",
            when(
                col("isFlaggedFraud") == 1,
                True
            ).otherwise(False)
        )

        .withColumn(
            "is_fraud",
            when(
                col("isFraud") == 1,
                True
            ).otherwise(False)
        )

        .withColumnRenamed(
            "newbalanceDest",
            "new_balance_destination"
        )

        .withColumnRenamed(
            "newbalanceOrig",
            "new_balance_origin"
        )

        .withColumnRenamed(
            "oldbalanceDest",
            "old_balance_destination"
        )

        .withColumnRenamed(
            "oldbalanceOrg",
            "old_balance_origin"
        )

        .withColumnRenamed(
            "type",
            "transaction_type"
        )

        .select(
            "transaction_id",
            "amount",
            "is_flagged_fraud",
            "is_fraud",
            "new_balance_destination",
            "new_balance_origin",
            "old_balance_destination",
            "old_balance_origin",
            "transaction_type"
        )
    )

    (
        all_transactions.write
        .format(
            "org.apache.spark.sql.cassandra"
        )
        .option(
            "keyspace",
            "fraud_detection"
        )
        .option(
            "table",
            "transactions"
        )
        .mode("append")
        .save()
    )

    print(
        f"SUCCESS: {len(pandas_df)} transaction(s) "
        "written to Cassandra."
    )

    # ========================================================
    # WRITE FRAUD ALERTS
    # ========================================================

    if len(fraud_df) == 0:

        print(
            "\nNo fraud candidates detected."
        )

        return

    print(
        "\nFraud candidates detected:"
    )

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

    # --------------------------------------------------------
    # Convert fraud dataframe back to Spark
    # --------------------------------------------------------

    fraud_spark_df = (
        spark.createDataFrame(fraud_df)
    )

    # --------------------------------------------------------
    # Prepare fraud alerts
    # --------------------------------------------------------

    fraud_alerts = (
        fraud_spark_df

        .withColumn(
            "processed_time",
            current_timestamp()
        )

        .withColumn(
            "alert_reason",
            lit(
                "Isolation Forest anomaly detected"
            )
        )

        # Convert 0/1 to Cassandra-compatible
        # boolean values
        .withColumn(
            "is_flagged_fraud",
            when(
                col("isFlaggedFraud") == 1,
                True
            ).otherwise(False)
        )

        .withColumn(
            "is_fraud",
            when(
                col("isFraud") == 1,
                True
            ).otherwise(False)
        )

        .withColumnRenamed(
            "nameDest",
            "name_dest"
        )

        .withColumnRenamed(
            "nameOrig",
            "name_orig"
        )

        .withColumnRenamed(
            "newbalanceDest",
            "new_balance_dest"
        )

        .withColumnRenamed(
            "newbalanceOrig",
            "new_balance_orig"
        )

        .withColumnRenamed(
            "oldbalanceDest",
            "old_balance_dest"
        )

        .withColumnRenamed(
            "oldbalanceOrg",
            "old_balance_org"
        )

        .select(
            "transaction_id",
            "alert_reason",
            "amount",
            "anomaly_score",
            "event_time",
            "is_flagged_fraud",
            "is_fraud",
            "name_dest",
            "name_orig",
            "new_balance_dest",
            "new_balance_orig",
            "old_balance_dest",
            "old_balance_org",
            "prediction",
            "processed_time",
            "step",
            "type",
        )
    )

    # --------------------------------------------------------
    # Write fraud alerts
    # --------------------------------------------------------

    (
        fraud_alerts.write
        .format(
            "org.apache.spark.sql.cassandra"
        )
        .option(
            "keyspace",
            "fraud_detection"
        )
        .option(
            "table",
            "fraud_alerts"
        )
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
    print(
        "Spark + Kafka + Isolation Forest + Cassandra"
    )
    print("=" * 70)

    global spark

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    model, scaler = load_model()

    schema = create_transaction_schema()

    print("\nConnecting to Kafka...")
    print(
        f"Kafka broker: "
        f"{KAFKA_BOOTSTRAP_SERVERS}"
    )
    print(
        f"Kafka topic : {KAFKA_TOPIC}"
    )

    # --------------------------------------------------------
    # READ FROM KAFKA
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # PARSE JSON
    # --------------------------------------------------------

    transactions = (
        kafka_df

        .selectExpr(
            "CAST(value AS STRING) AS json_value",
            "timestamp AS event_time"
        )

        .select(
            from_json(
                col("json_value"),
                schema
            ).alias("transaction"),

            col("event_time")
        )

        .select(
            "transaction.*",
            "event_time"
        )
    )

    # --------------------------------------------------------
    # START STREAM
    # --------------------------------------------------------

    print(
        "\nStarting fraud scoring stream..."
    )

    print(
        "Processing mode: micro-batch"
    )

    print(
        "Trigger interval: 5 seconds"
    )

    print(
        "Press CTRL+C to stop."
    )

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
