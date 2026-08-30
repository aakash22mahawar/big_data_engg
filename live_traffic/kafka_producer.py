# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/aakash22mahawar@gmail.com/big_data_engg/requirements.txt",
# ]
# ///
import json
import logging
import random
import time

from datetime import datetime, timedelta

import pytz

from faker import Faker
from kafka import KafkaProducer
from utils.utils import configure_logging


configure_logging()


logger = logging.getLogger(
    "live_traffic_producer"
)


# -----------------------------
# Define timezone
# -----------------------------

ist = pytz.timezone(
    "Asia/Kolkata"
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
# Initialize Faker
# -----------------------------

fake = Faker()


# -----------------------------
# Establish Kafka connection
# -----------------------------

producer = KafkaProducer(

    bootstrap_servers=kafka_server,

    security_protocol="SASL_SSL",

    sasl_mechanism="PLAIN",

    sasl_plain_username=kafka_key,

    sasl_plain_password=kafka_secret,

    acks="all",

    retries=5,

    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


logger.info(
    "Kafka producer initialized successfully."
)


roads = [
    "R100",
    "R200",
    "R300",
    "R400"
]

zones = [
    "CBD",
    "AIRPORT",
    "TECHPARK",
    "SUBURB",
    "TRAINSTATION"
]

weather = [
    "CLEAR",
    "RAIN",
    "FOG",
    "STORM"
]


vehicle_cache = []


# -----------------------------
# Generate clean traffic event
# -----------------------------

def generate_clean_event():

    vid = fake.uuid4()

    vehicle_cache.append(
        vid
    )

    return {

        "vehicle_id": vid,

        "road_id": random.choice(
            roads
        ),

        "city_zone": random.choice(
            zones
        ),

        "speed": random.randint(
            20,
            100
        ),

        "congestion_level": random.randint(
            1,
            5
        ),

        "weather": random.choice(
            weather
        ),

        "event_time": datetime.now(
            ist
        ).isoformat()
    }


# -----------------------------
# Generate dirty traffic event
# -----------------------------

def generate_dirty_event():

    dirty_type = random.choice(
        [
            "null_speed",
            "negative_speed",
            "extreme_speed",
            "duplicate_vehicle",
            "late_event",
            "future_event",
            "wrong_datatype",
            "schema_drift",
            "corrupt_json"
        ]
    )

    base = generate_clean_event()


    if dirty_type == "null_speed":

        base["speed"] = None


    elif dirty_type == "negative_speed":

        base["speed"] = -40


    elif dirty_type == "extreme_speed":

        base["speed"] = 420


    elif dirty_type == "duplicate_vehicle" and vehicle_cache:

        base["vehicle_id"] = random.choice(
            vehicle_cache
        )


    elif dirty_type == "late_event":

        base["event_time"] = (

            datetime.now(
                ist
            )

            - timedelta(
                minutes=random.randint(
                    10,
                    120
                )
            )

        ).isoformat()


    elif dirty_type == "future_event":

        base["event_time"] = (

            datetime.now(
                ist
            )

            + timedelta(
                minutes=random.randint(
                    5,
                    60
                )
            )

        ).isoformat()


    elif dirty_type == "wrong_datatype":

        base["speed"] = "FAST"


    elif dirty_type == "schema_drift":

        base["road_condition"] = random.choice(
            [
                "GOOD",
                "BAD",
                "UNDER_CONSTRUCTION"
            ]
        )


    elif dirty_type == "corrupt_json":

        return "###CORRUPTED_EVENT###"


    return base


# -----------------------------
# Publish traffic events
# -----------------------------

def start_streaming():

    generated_count = 0

    successful_count = 0

    failed_count = 0


    logger.info(
        "Traffic producer started."
    )


    try:

        while True:

            generated_count += 1


            if random.random() < 0.7:

                event = generate_clean_event()

            else:

                event = generate_dirty_event()


            try:

                if isinstance(
                    event,
                    str
                ):

                    producer.send(
                        kafka_topic,
                        value={
                            "raw": event
                        }
                    ).get(
                        timeout=10
                    )

                    logger.warning(
                        "Corrupt event produced."
                    )

                else:

                    producer.send(
                        kafka_topic,
                        value=event
                    ).get(
                        timeout=10
                    )

                    logger.info(
                        "Traffic event produced successfully."
                    )


                successful_count += 1


                logger.info(
                    f"Producer statistics | "
                    f"generated={generated_count} | "
                    f"successful={successful_count} | "
                    f"failed={failed_count}"
                )


            except Exception as error:

                failed_count += 1

                logger.error(
                    f"Kafka message delivery failed | "
                    f"generated={generated_count} | "
                    f"successful={successful_count} | "
                    f"failed={failed_count} | "
                    f"error={error}"
                )


            time.sleep(
                random.uniform(
                    0.5,
                    1.5
                )
            )


    except KeyboardInterrupt:

        logger.info(
            "Producer interrupted by user."
        )


    finally:

        logger.info(
            "Flushing pending Kafka messages..."
        )

        producer.flush()


        producer.close()


        logger.info(
            f"Producer stopped | "
            f"generated={generated_count} | "
            f"successful={successful_count} | "
            f"failed={failed_count}"
        )


# -----------------------------
# Start traffic event producer
# -----------------------------

if __name__ == "__main__":

    start_streaming()