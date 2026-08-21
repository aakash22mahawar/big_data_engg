import configparser
import json
import os
import random
import time

from datetime import datetime, timedelta

import pytz

from faker import Faker
from kafka import KafkaProducer


# Initialize configuration

config = configparser.ConfigParser()

config_path = os.path.join(os.path.dirname(__file__),"..","config.ini")

config.read(os.path.abspath(config_path))


# Read Kafka configuration

kafka_key = config["KAFKA"]["ACCESS_KEY"]
kafka_secret = config["KAFKA"]["SECRET_KEY"]
kafka_server = config["KAFKA"]["BOOTSTRAP_SERVER"]


# Initialize Faker

fake = Faker()


# Establish Kafka connection

producer = KafkaProducer(

    bootstrap_servers=kafka_server,

    security_protocol="SASL_SSL",

    sasl_mechanism="PLAIN",

    sasl_plain_username=kafka_key,

    sasl_plain_password=kafka_secret,

    value_serializer=lambda v: json.dumps(v).encode("utf-8")
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
            pytz.utc
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
                pytz.utc
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
                pytz.utc
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

    message_count = 0

    try:

        while True:

            if random.random() < 0.7:

                event = generate_clean_event()

            else:

                event = generate_dirty_event()


            if isinstance(
                event,
                str
            ):

                producer.send(
                    "traffic-topic",
                    value={
                        "raw": event
                    }
                )

                print(
                    "CORRUPT EVENT SENT"
                )


            else:

                producer.send(
                    "traffic-topic",
                    value=event
                )

                print(event)

            message_count +=1  

            time.sleep(random.uniform(0.5,1.5))  

            print(f"Message successfully produced: {message_count}") 

    except KeyboardInterrupt:

         print(f"Message successfully produced: {message_count}") 


# -----------------------------
# Start traffic event producer
# -----------------------------

if __name__ == "__main__":

    start_streaming()