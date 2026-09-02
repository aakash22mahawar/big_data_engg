from pyspark import pipelines as dp

from pyspark.sql.functions import *


# -----------------------------
# Dimension Zone
# -----------------------------

@dp.table(
    name="live_traffic_kafka.gold.dim_zone_lakeflow",
    comment="Traffic zone dimension"
)

def dim_zone_lakeflow():

    silver_stream = (
        spark.readStream
        .table(
            "live_traffic_kafka.silver.traffic_silver_lakeflow"
        )
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

    return dim_zone


# -----------------------------
# Dimension Road
# -----------------------------

@dp.table(
    name="live_traffic_kafka.gold.dim_road_lakeflow",
    comment="Traffic road dimension"
)

def dim_road_lakeflow():

    silver_stream = (
        spark.readStream
        .table(
            "live_traffic_kafka.silver.traffic_silver_lakeflow"
        )
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

    return dim_road


# -----------------------------
# Fact Table
# -----------------------------

@dp.table(
    name="live_traffic_kafka.gold.fact_traffic_lakeflow",
    comment="Traffic fact table"
)

def fact_traffic_lakeflow():

    silver_stream = (
        spark.readStream
        .table(
            "live_traffic_kafka.silver.traffic_silver_lakeflow"
        )
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

    return fact_enriched