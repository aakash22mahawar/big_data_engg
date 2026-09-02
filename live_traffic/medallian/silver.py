import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys
sys.path.append("/Workspace/Users/aakash22mahawar@gmail.com/big_data_engg/live_traffic")


from utils.utils import configure_logging

configure_logging()

logger = logging.getLogger(
    "live_traffic_silver"
)


# -----------------------------
# Initialize Spark session
# -----------------------------

logger.info(
    "Starting Silver streaming job."
)

spark = (
    SparkSession.builder
    .appName(
        "live_traffic_silver"
    )
    .getOrCreate()
)

logger.info(
    "Spark session initialized."
)


# -----------------------------
# Read Bronze Stream
# -----------------------------

logger.info(
    "Reading Bronze Delta table."
)

bronze_df = (
    spark.readStream
    .table(
        "live_traffic_kafka.bronze.traffic_bronze"
    )
)

logger.info(
    "Bronze Delta stream configured."
)


# -----------------------------
# Traffic Schema
# -----------------------------

traffic_schema = StructType([

    StructField(
        "vehicle_id",
        StringType()
    ),

    StructField(
        "road_id",
        StringType()
    ),

    StructField(
        "city_zone",
        StringType()
    ),

    StructField(
        "speed",
        StringType()
    ),

    StructField(
        "congestion_level",
        IntegerType()
    ),

    StructField(
        "weather",
        StringType()
    ),

    StructField(
        "event_time",
        StringType()
    )
])


# -----------------------------
# Parse Raw JSON
# -----------------------------

parsed = bronze_df.withColumn(
    "data",
    from_json(
        col("raw_json"),
        traffic_schema
    )
)


flattened = parsed.select(
    "kafka_key",
    "raw_json",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "data.*"
)


# -----------------------------
# Data Quality Flag
# -----------------------------

dq_df = flattened.withColumn(
    "dq_flag",
    when(
        col("vehicle_id").isNull(),
        "MISSING_VEHICLE"
    )
    .when(
        col("event_time").isNull(),
        "MISSING_TIME"
    )
    .when(
        col("raw_json").contains("CORRUPTED"),
        "CORRUPT_JSON"
    )
    .otherwise(
        "OK"
    )
)


# -----------------------------
# Safe Type Casting
# -----------------------------

typed = dq_df.withColumn(
    "speed_int",
    expr("try_cast(speed AS INT)")
).withColumn(
    "event_ts",
    to_timestamp(
        "event_time"
    )
)


# -----------------------------
# Business Validation Rules
# -----------------------------

validated = typed.withColumn(
    "speed_valid",
    when(
        (col("speed_int") >= 0) &
        (col("speed_int") <= 160),
        1
    ).otherwise(
        0
    )
).withColumn(
    "time_valid",
    when(
        col("event_ts") <=
        current_timestamp() +
        expr("INTERVAL 10 MINUTES"),
        1
    ).otherwise(
        0
    )
)


# -----------------------------
# Filter Good Records
# -----------------------------

clean_stream = validated.filter(

    (col("dq_flag") == "OK") &
    (col("speed_valid") == 1) &
    (col("time_valid") == 1)

)


# -----------------------------
# Handle Late Data
# -----------------------------

watermarked = clean_stream.withWatermark(
    "event_ts",
    "15 minutes"
)


# -----------------------------
# Deduplication
# -----------------------------

deduped = watermarked.dropDuplicates(
    [
        "vehicle_id",
        "event_ts"
    ]
)


# -----------------------------
# Feature Engineering
# -----------------------------

silver_final = (
    deduped

    .withColumn(
        "hour",
        hour("event_ts")
    )

    .withColumn(
        "peak_flag",
        when(
            (
                col("hour").between(8, 11)
            ) |
            (
                col("hour").between(17, 20)
            ),
            1
        ).otherwise(
            0
        )
    )

    .withColumn(
        "speed_band",
        when(
            col("speed_int") < 30,
            "LOW"
        )
        .when(
            col("speed_int") < 70,
            "MEDIUM"
        )
        .otherwise(
            "HIGH"
        )
    )
)


# -----------------------------
# Write Silver Table
# -----------------------------

logger.info(
    "Starting Silver streaming query."
)

silver_query = (
    silver_final.writeStream
    .format("delta")
    .outputMode(
        "append"
    )
    .trigger(
    processingTime="10 seconds")
   
    .option(
        "checkpointLocation",
        "/Volumes/live_traffic_kafka/bronze/checkpoint_volume/silver/"
    )
    .toTable(
        "live_traffic_kafka.silver.traffic_silver"
    )
)


# -----------------------------
# Start streaming query
# -----------------------------

logger.info(
    "Silver streaming query started."
)

silver_query.awaitTermination()

logger.info(
    "Silver streaming query completed."
)