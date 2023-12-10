import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


transit_directions_raw = spark.read.json(
    "homehuntr/data/directions/*_transit.json", multiLine=True
)

transit_directions = (
    transit_directions_raw.select(
        F.col("geocoded_waypoints").getItem(0)["place_id"].alias("origin_id"),
        F.col("geocoded_waypoints").getItem(1)["place_id"].alias("destination_id"),
        F.explode("routes.legs").alias("legs"),
    )
    .select(
        F.col("origin_id"),
        F.col("destination_id"),
        F.col("legs.start_address").getItem(0).alias("origin_name"),
        F.col("legs.end_address").getItem(0).alias("destination_name"),
        F.col("legs.duration.value").getItem(0).alias("duration"),
        F.col("legs.distance.value").getItem(0).alias("distance"),
        F.col("legs.steps"),
    )
    .withColumn("steps", F.col("steps").getItem(0))
    .withColumn("distance_mi", F.round(F.col("distance") / 1609.34, 2))
    .withColumn("duration_min", F.ceiling(F.col("duration") / 60))
    .withColumn(
        "step_instructions",
        F.expr("transform(steps, x -> x.html_instructions)").getItem(0),
    )
    .withColumn(
        "transit_lines",
        F.array_compact(
            F.transform(
                F.col("steps"), lambda x: x["transit_details"]["line"]["short_name"]
            )
        ),
    )
    .withColumn(
        "departure_stop",
        F.array_compact(
            F.transform(
                F.col("steps"), lambda x: x["transit_details"]["departure_stop"]["name"]
            )
        ),
    )
    .withColumn(
        "arrival_stop",
        F.array_compact(
            F.transform(
                F.col("steps"), lambda x: x["transit_details"]["arrival_stop"]["name"]
            )
        ),
    )
    .withColumn(
        "transit_stops",
        F.transform(
            F.arrays_zip("transit_lines", "departure_stop", "arrival_stop"),
            lambda x: F.concat(
                x["transit_lines"],
                F.lit(":"),
                x["departure_stop"],
                F.lit("->"),
                x["arrival_stop"],
            ),
        ),
    )
    .withColumn(
        "num_transfers",
        F.size(F.col("transit_stops")) - 1,
    )
    .withColumn(
        "walking_min_array",
        F.array_compact(
            F.transform(
                F.col("steps"),
                lambda x: F.when(
                    x["travel_mode"] == "WALKING", x["duration"]["value"].cast("int")
                ).otherwise(F.lit(None)),
            ),
        ),
    )
    .withColumn(
        "transit_min_array",
        F.array_compact(
            F.transform(
                F.col("steps"),
                lambda x: F.when(
                    x["travel_mode"] == "TRANSIT", x["duration"]["value"].cast("int")
                ).otherwise(F.lit(None)),
            ),
        ),
    )
    .withColumn(
        "walking_min",
        F.aggregate(F.col("walking_min_array"), F.lit(0), lambda acc, x: acc + x) / 60,
    )
    .withColumn(
        "transit_min",
        F.aggregate(F.col("transit_min_array"), F.lit(0), lambda acc, x: acc + x) / 60,
    )
    .withColumn(
        "waiting_min",
        F.col("duration_min") - F.col("walking_min") - F.col("transit_min"),
    )
    .withColumn("transit_stops", F.concat_ws("; ", F.col("transit_stops")))
)


transit_directions_final = transit_directions.select(
    "origin_id",
    "origin_name",
    "destination_id",
    "destination_name",
    "duration_min",
    "distance_mi",
    "transit_stops",
    "num_transfers",
    "walking_min",
    "transit_min",
    "waiting_min",
)


transit_directions_final.write.format("delta").mode("append").save(
    "homehuntr/data/delta/transit_directions"
)
