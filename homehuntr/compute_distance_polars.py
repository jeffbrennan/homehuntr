import polars as pl
import gcsfs


def get_initial_transit_cols(df: pl.DataFrame) -> pl.DataFrame:
    parsed_df = (
        df.with_columns(
            pl.col("geocoded_waypoints")
            .map_elements(lambda x: x[0]["place_id"])
            .alias("origin_id")
        )
        .with_columns(
            pl.col("geocoded_waypoints")
            .map_elements(lambda x: x[1]["place_id"])
            .alias("destination_id")
        )
        .with_columns(
            routes_struct=pl.col("routes").map_elements(
                lambda x: x[0] if x is not None else None
            ),
        )
        .unnest("routes_struct")
        .with_columns(
            legs_struct=pl.col("legs").map_elements(
                lambda x: x[0] if x is not None else None
            )
        )
        .unnest("legs_struct")
        .unnest("duration")
        .with_columns(duration=pl.col("value"))
        .drop("text")
        .drop("value")
        .unnest("distance")
        .with_columns(distance=pl.col("value"))
        .with_columns(steps=pl.col("steps").map_elements(lambda x: x[0]))
        .select(
            pl.col("origin_id"),
            pl.col("destination_id"),
            pl.col("start_address").alias("origin_name"),
            pl.col("end_address").alias("destination_name"),
            pl.col("duration"),
            pl.col("distance"),
            pl.col("steps"),
        )
    )

    return parsed_df


def parse_stops(df: DataFrame) -> DataFrame:
    out_df = (
        df.withColumn(
            "transit_lines",
            F.array_compact(
                F.transform(
                    F.col("steps"),
                    lambda x: x["transit_details"]["line"]["short_name"],
                )
            ),
        )
        .withColumn(
            "departure_stop",
            F.array_compact(
                F.transform(
                    F.col("steps"),
                    lambda x: x["transit_details"]["departure_stop"]["name"],
                )
            ),
        )
        .withColumn(
            "arrival_stop",
            F.array_compact(
                F.transform(
                    F.col("steps"),
                    lambda x: x["transit_details"]["arrival_stop"]["name"],
                )
            ),
        )
        .withColumn(
            "vehicle_type",
            F.array_compact(
                F.transform(
                    F.col("steps"),
                    lambda x: x["transit_details"]["line"]["vehicle"]["type"],
                )
            ),
        )
        .withColumn(
            "transit_stops",
            F.transform(
                F.arrays_zip(
                    "vehicle_type",
                    "transit_lines",
                    "departure_stop",
                    "arrival_stop",
                ),
                lambda x: F.concat(
                    F.lit("["),
                    x["vehicle_type"],
                    x["transit_lines"],
                    F.lit("]"),
                    F.lit(" "),
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
            "num_transfers",
            F.when(F.col("num_transfers") < 0, 0).otherwise(F.col("num_transfers")),
        )
        .withColumn("transit_stops", F.concat_ws("; ", F.col("transit_stops")))
        .withColumn("transit_stops", F.regexp_replace("transit_stops", "BUS", "🚌"))
        .withColumn("transit_stops", F.regexp_replace("transit_stops", "SUBWAY", "🚂")),
    )
    out_df = out_df[0]
    return out_df


def parse_mode_duration(dur_col: str, mode: str, second_divisor: int) -> Column:
    duration_array_seconds: Column = F.array_compact(
        F.transform(
            F.col("steps"),
            lambda x: F.when(
                x["travel_mode"] == mode,
                x[dur_col]["value"].cast("int"),
            ).otherwise(F.lit(None)),
        ),
    )
    return (
        F.aggregate(duration_array_seconds, F.lit(0), lambda acc, x: acc + x)
        / second_divisor
    )


def drop_bad_directions(df: pl.DataFrame):
    paths = (
        df.select("origin_id", "destination_id")
        .distinct()
        .withColumn(
            "path", F.concat_ws("_", F.col("origin_id"), F.col("destination_id"))
        )
        .select("path")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    fs = gcsfs.GCSFileSystem(project="homehuntr")
    for path in paths:
        fs.rm(f"gs://homehuntr-storage/directions/{path}_transit.json")


def parse_transit_result(df: pl.DataFrame) -> pl.DataFrame:
    transit_initial_selection = get_initial_transit_cols(df)
    parsed_stops = parse_stops(transit_initial_selection)

    transit_directions = (
        parsed_stops.distinct()
        .withColumn("distance_mi", F.round(F.col("distance") / 1609.34, 2))
        .withColumn("duration_min", F.ceiling(F.col("duration") / 60))
        .withColumn("walking_min", parse_mode_duration("duration", "WALKING", 60))
        .withColumn("transit_min", parse_mode_duration("duration", "TRANSIT", 60))
        .withColumn(
            "waiting_min",
            F.col("duration_min") - F.col("walking_min") - F.col("transit_min"),
        )
    )

    bad_directions = transit_directions.filter(F.col("duration_min") > 120)
    if bad_directions.count() > 0:
        drop_bad_directions(bad_directions)

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

    return transit_directions_final


def parse_distance(run_type: str = "overwrite"):
    if run_type not in ["append", "overwrite"]:
        raise ValueError(f"run_type must be 'append' or 'overwrite', got {run_type}")

    fs = gcsfs.GCSFileSystem(project="homehuntr")
    transit_directions = []
    transit_directions_paths = fs.ls("homehuntr/directions")
    for path in transit_directions_paths:
        if not path.endswith("_transit.json"):
            continue
        with fs.open(path) as f:
            temp_df = pl.read_json(f.read())
            transit_directions.append(temp_df)

    transit_directions_raw = pl.concat(transit_directions, how="diagonal")
    transit_directions_final = parse_transit_result(transit_directions_raw)

    transit_directions_final.head()

    # with fs.open("homehuntr/data/delta/transit_directions/", "wb") as f:
    #     transit_directions_final.write_delta(f, mode="overwrite")  # type: ignore


if __name__ == "__main__":
    parse_distance("overwrite")
