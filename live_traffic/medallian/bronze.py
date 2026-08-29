import logging

from datetime import datetime

import pytz

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# -----------------------------
# Initialize logging
# -----------------------------

# Define the log format and date format
log_format = '%(asctime)s - %(levelname)s - %(message)s'
date_format = '%d-%m-%Y %H:%M:%S'


class ISTFormatter(
    logging.Formatter
):

    def formatTime(
        self,
        record,
        datefmt=None
    ):

        ist = pytz.timezone(
            "Asia/Kolkata"
        )

        dt = datetime.fromtimestamp(
            record.created,
            ist
        )

        return dt.strftime(
            datefmt
        )


logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    datefmt=date_format
)


for handler in logging.getLogger().handlers:

    handler.setFormatter(
        ISTFormatter(
            log_format,
            date_format
        )
    )


logger = logging.getLogger(
    "live_traffic_bronze"
)


# -----------------------------
# Initialize Spark session
# -----------------------------

logger.info(
    "Starting Bronze streaming job."
)

spark = (
    SparkSession.builder
    .appName(
        "live_traffic_delta_lake"
    )
    .getOrCreate()
)

# spark.conf.set(
#     "spark.sql.session.timeZone",
#     "Asia/Kolkata"
# )

logger.info(
    "Spark session initialized."
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


logger.info(
    "Kafka configuration loaded."
)

logger.info(
    f"Kafka topic: {kafka_topic}"
)


# -----------------------------
# Kafka Raw Stream
# -----------------------------

logger.info(
    "Connecting to Kafka."
)

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
        "latest"
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

logger.info(
    "Kafka stream configured successfully."
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


logger.info(
    "Kafka stream transformed for Bronze."
)


# -----------------------------
# Process each micro-batch
# -----------------------------

def process_batch(
    batch_df,
    batch_id
):

    message_count = batch_df.count()


    logger.info(
        f"Batch {batch_id} received | "
        f"messages={message_count}"
    )


    if message_count == 0:

        logger.info(
            f"Batch {batch_id} contained no messages."
        )

        return


    try:

        logger.info(
            f"Writing batch {batch_id} to Bronze Delta."
        )

        batch_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(
                "live_traffic_kafka.bronze.traffic_bronze"
            )


        logger.info(
            f"Batch {batch_id} successfully written | "
            f"messages={message_count}"
        )


    except Exception as error:

        logger.error(
            f"Batch {batch_id} failed | "
            f"messages={message_count} | "
            f"error={error}"
        )

        raise


# -----------------------------
# Bronze Delta Write
# -----------------------------

logger.info(
    "Starting Bronze streaming query."
)

bronze_query = (
    bronze_stream.writeStream
    .foreachBatch(
        process_batch
    )
    .outputMode(
        "append"
    )
    .option(
        "checkpointLocation",
        "/Volumes/live_traffic_kafka/bronze/checkpoint_volume/bronze/"
    )
    .start()
)


# -----------------------------
# Start streaming query
# -----------------------------

logger.info(
    "Bronze streaming query started."
)

bronze_query.awaitTermination()