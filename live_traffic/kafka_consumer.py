import json
import logging

from kafka import KafkaConsumer


# -----------------------------
# Initialize logging
# -----------------------------

# Define the log format and date format

log_format = '%(asctime)s - %(levelname)s - %(message)s'
date_format = '%d-%m-%Y %H:%M:%S'

logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    datefmt=date_format
)

logger = logging.getLogger(
    "live_traffic_consumer"
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


# -----------------------------
# Kafka topic
# -----------------------------

kafka_topic = "traffic-topic"


logger.info(
    "Kafka configuration loaded."
)

logger.info(
    f"Kafka topic: {kafka_topic}"
)


# -----------------------------
# Establish Kafka connection
# -----------------------------

consumer = KafkaConsumer(

    kafka_topic,

    bootstrap_servers=kafka_server,

    security_protocol="SASL_SSL",

    sasl_mechanism="PLAIN",

    sasl_plain_username=kafka_key,

    sasl_plain_password=kafka_secret,

    group_id="live-traffic-consumer",

    auto_offset_reset="earliest",

    enable_auto_commit=True,

    value_deserializer=lambda v: json.loads(
        v.decode("utf-8")
    )
)


logger.info(
    "Kafka consumer initialized successfully."
)

logger.info(
    "Waiting for traffic events..."
)


# -----------------------------
# Consume traffic events
# -----------------------------

consumed_count = 0

failed_count = 0


try:

    for message in consumer:

        try:

            consumed_count += 1

            event = message.value

            logger.info(
                f"Traffic event consumed | "
                f"topic={message.topic} | "
                f"partition={message.partition} | "
                f"offset={message.offset}"
            )
      
            logger.info(
                f"Consumer statistics | "
                f"consumed={consumed_count} | "
                f"failed={failed_count}"
            )


        except Exception as error:

            failed_count += 1

            logger.error(
                f"Failed to process Kafka message | "
                f"consumed={consumed_count} | "
                f"failed={failed_count} | "
                f"error={error}"
            )


except KeyboardInterrupt:

    logger.info(
        "Consumer interrupted by user."
    )


finally:

    consumer.close()

    logger.info(
        f"Consumer stopped | "
        f"consumed={consumed_count} | "
        f"failed={failed_count}"
    )