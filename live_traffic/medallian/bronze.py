from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# -----------------------------
# Initialize Spark session
# -----------------------------

spark = (
    SparkSession.builder
    .appName(
        "live_traffic_delta_lake"
    )
    .getOrCreate()
)

# -----------------------------
# Read Kafka configuration
# -----------------------------

kafka_key = dbutils.secrets.get(
    catalog="live_traffic_kafka",
    schema="bronze",
    key="confluent_kafka_key"
)

kafka_secret = dbutils.secrets.get(
    catalog="live_traffic_kafka",
    schema="bronze",
    key="confluent_kafka_secret"
)

kafka_server = dbutils.secrets.get(
    catalog="live_traffic_kafka",
    schema="bronze",
    key="kafka_bootstrap_server"
)

kafka_topic = "traffic-topic"


# -----------------------------
# Kafka Raw Stream
# -----------------------------

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        kafka_server
    )
    .option(
        "subscribe",
        kafka_topic
    )
    .option(
        "startingOffsets",
        "latest"                                 #latest or earliest
    )
    .option(
        "kafka.security.protocol",
        "SASL_SSL"
    )
    .option(
        "kafka.sasl.mechanism",
        "PLAIN"
    )
    .option(
        "kafka.sasl.jaas.config",
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{kafka_key}" '
        f'password="{kafka_secret}";'
    )
    .load()
)


# -----------------------------
# Convert Kafka data
# -----------------------------

bronze_stream = raw_stream.select(

    col("key").cast("string").alias(
        "kafka_key"
    ),

    col("value").cast("string").alias(
        "raw_json"
    ),

    col("topic").alias(
        "kafka_topic"
    ),

    col("partition").alias(
        "kafka_partition"
    ),

    col("offset").alias(
        "kafka_offset"
    ),

    col("timestamp").alias(
        "kafka_timestamp"
    )
)


# -----------------------------
# Bronze Delta Write
# -----------------------------

bronze_query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(                               
        availableNow=True                       ##process all the msgs which are already available
    )
    .option(
        "checkpointLocation",
        "/Volumes/live_traffic_kafka/bronze/checkpoint_volume/bronze/"
    )
    .toTable(
        "live_traffic_kafka.bronze.traffic_bronze"
    )
)


# -----------------------------
# Start streaming query
# -----------------------------

bronze_query.awaitTermination()