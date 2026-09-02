from pyspark import pipelines as dp

from pyspark.sql.functions import *
from pyspark.sql.types import *


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
# Silver streaming table
# -----------------------------

@dp.table(
    name="live_traffic_kafka.silver.traffic_silver_lakeflow",
    comment="Cleaned and validated traffic events"
)

def traffic_silver_lakeflow():


    # -----------------------------
    # Read Bronze Stream
    # -----------------------------

    bronze_df = (
        spark.readStream
        .table(
            "live_traffic_kafka.bronze.traffic_bronze_lakeflow"
        )
    )


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

        expr(
            "try_cast(speed AS INT)"
        )

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
                )

                |

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


    return silver_final