from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("KafkaConnectorTest")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .getOrCreate()
)

print("=" * 60)
print("SPARK KAFKA CONNECTOR TEST")
print("=" * 60)
print(f"Spark version: {spark.version}")
print("Kafka connector configured successfully!")
print("=" * 60)

spark.stop()