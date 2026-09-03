from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("RealTimeFraudDetection")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
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


def start_streaming():

    print("=" * 70)
    print("REAL-TIME FRAUD DETECTION")
    print("Apache Spark Structured Streaming + Kafka")
    print("=" * 70)

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

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
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    transactions = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(
            from_json(
                col("json_value"),
                schema
            ).alias("transaction")
        )
        .select("transaction.*")
    )

    print("\nStarting Structured Streaming...")
    print("Processing mode: micro-batch")
    print("Trigger interval: 5 seconds")
    print("Press CTRL+C to stop.")

    query = (
    transactions
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 10)
    .option("checkpointLocation", "/home/najwah/fraud_checkpoint")
    .trigger(processingTime="5 seconds")
    .start()
)

    query.awaitTermination()


if __name__ == "__main__":
    start_streaming()
