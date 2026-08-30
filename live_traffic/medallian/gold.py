import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import os
import sys
sys.path.append("/Workspace/Users/aakash22mahawar@gmail.com/big_data_engg/live_traffic")

from utils.utils import configure_logging


configure_logging()


logger = logging.getLogger(
    "live_traffic_gold"
)


# -----------------------------
# Initialize Spark session
# -----------------------------

logger.info(
    "Starting Gold streaming job."
)

spark = (
    SparkSession.builder
    .appName(
        "live_traffic_gold"
    )
    .getOrCreate()
)

logger.info(
    "Spark session initialized."
)


# -----------------------------
# Read Silver Stream
# -----------------------------

logger.info(
    "Reading Silver Delta table."
)

silver_stream = (
    spark.readStream
    .table(
        "live_traffic_kafka.silver.traffic_silver"
    )
)

logger.info(
    "Silver Delta stream configured."
)


# -----------------------------
# Dimension Zone
# -----------------------------

logger.info(
    "Building Zone dimension."
)

dim_zone = silver_stream.select(
    "city_zone"
).dropDuplicates() \
.withColumn(
    "zone_type",
    when(
        col("city_zone") == "CBD",
        "Commercial"
    )
    .when(
        col("city_zone") == "TECHPARK",
        "IT HUB"
    )
    .when(
        col("city_zone").isin(
            "AIRPORT",
            "TRAINSTATION"
        ),
        "Transit Hub"
    )
    .otherwise(
        "Residential"
    )
) \
.withColumn(
    "traffic_risk",
    when(
        col("city_zone").isin(
            "CBD",
            "AIRPORT",
            "TRAINSTATION"
        ),
        "HIGH"
    )
    .when(
        col("city_zone") == "TECHPARK",
        "MEDIUM"
    )
    .otherwise(
        "LOW"
    )
)


# -----------------------------
# Write Zone Dimension
# -----------------------------

logger.info(
    "Starting Zone dimension query."
)

zone_query = (
    dim_zone.writeStream
    .format("delta")
    .outputMode(
        "append"
    )
    .trigger(
        availableNow=True
    )
    .option(
        "checkpointLocation",
        "/Volumes/live_traffic_kafka/bronze/checkpoint_volume/gold/dim_zone/"
    )
    .toTable(
        "live_traffic_kafka.gold.dim_zone"
    )
)


# -----------------------------
# Dimension Road
# -----------------------------

logger.info(
    "Building Road dimension."
)

dim_road = silver_stream.select(
    "road_id"
).dropDuplicates() \
.withColumn(
    "road_type",
    when(
        col("road_id").isin(
            "R100",
            "R200"
        ),
        "Highway"
    )
    .otherwise(
        "City Road"
    )
) \
.withColumn(
    "speed_limit",
    when(
        col("road_id").isin(
            "R100",
            "R200"
        ),
        100
    )
    .otherwise(
        60
    )
)


# -----------------------------
# Write Road Dimension
# -----------------------------

logger.info(
    "Starting Road dimension query."
)

road_query = (
    dim_road.writeStream
    .format("delta")
    .outputMode(
        "append"
    )
    .trigger(
        availableNow=True
    )
    .option(
        "checkpointLocation",
        "/Volumes/live_traffic_kafka/bronze/checkpoint_volume/gold/dim_road/"
    )
    .toTable(
        "live_traffic_kafka.gold.dim_road"
    )
)


# -----------------------------
# Fact Table
# -----------------------------

logger.info(
    "Building Traffic fact table."
)

fact_stream = silver_stream.select(

    "vehicle_id",

    "road_id",

    "city_zone",

    "speed_int",

    "congestion_level",

    "event_ts",

    "peak_flag",

    "speed_band",

    "hour",

    "weather"
)


fact_enriched = fact_stream.withColumn(
    "date",
    to_date(
        "event_ts"
    )
)


# -----------------------------
# Write Fact Table
# -----------------------------

logger.info(
    "Starting Traffic fact query."
)

fact_query = (
    fact_enriched.writeStream
    .format("delta")
    .outputMode(
        "append"
    )
    .trigger(
        availableNow=True
    )
    .option(
        "checkpointLocation",
        "/Volumes/live_traffic_kafka/bronze/checkpoint_volume/gold/fact_traffic/"
    )
    .toTable(
        "live_traffic_kafka.gold.fact_traffic"
    )
)


# -----------------------------
# Start streaming queries
# -----------------------------

logger.info(
    "Gold streaming queries started."
)

spark.streams.awaitAnyTermination()

logger.info(
    "Gold streaming queries completed."
)