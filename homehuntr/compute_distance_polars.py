from concurrent.futures import ThreadPoolExecutor
import polars as pl
import gcsfs
import os
from itertools import repeat


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


def parse_list_of_structs(
    col: pl.Series, keys: tuple[str], keep_nulls: bool = True
) -> pl.Series:
    """
    Takes in a column which is a list[struct[n]] and returns values from the struct based on a tuple of provided keys
    """
    if keep_nulls:
        return col.map_elements(lambda x: [reduce(dict.get, keys, i) for i in x])

    return col.map_elements(
        lambda x: [
            reduce(dict.get, keys, i)
            for i in x
            if reduce(dict.get, keys, i) is not None
        ]
    )


def parse_stops(df: pl.DataFrame) -> pl.DataFrame:
    print("Parsing stops...")
    out_df = (
        df.with_columns(
            transit_lines=parse_list_of_structs(
                col=pl.col("steps"),
                keys=("transit_details", "line", "short_name"),
                keep_nulls=False,
            )
        )
        .with_columns(
            departure_stop=parse_list_of_structs(
                col=pl.col("steps"),
                keys=("transit_details", "departure_stop", "name"),
                keep_nulls=False,
            )
        )
        .with_columns(
            arrival_stop=parse_list_of_structs(
                col=pl.col("steps"),
                keys=("transit_details", "arrival_stop", "name"),
                keep_nulls=False,
            )
        )
        .with_columns(
            vehicle_type=parse_list_of_structs(
                col=pl.col("steps"),
                keys=("transit_details", "line", "vehicle", "type"),
                keep_nulls=False,
            )
        )
        .explode(["vehicle_type", "transit_lines", "departure_stop", "arrival_stop"])
        .select(
            "origin_id",
            "destination_id",
            "origin_name",
            "destination_name",
            pl.struct(
                ["vehicle_type", "transit_lines", "departure_stop", "arrival_stop"]
            ).alias("combined_stops"),
        )
        .group_by(
            "origin_id",
            "destination_id",
            "origin_name",
            "destination_name",
        )
        .agg(pl.col("combined_stops"))
        .with_columns(
            transit_stops=pl.col("combined_stops").map_elements(
                lambda x: [
                    "["
                    + i["vehicle_type"]
                    + i["transit_lines"]
                    + "] "
                    + i["departure_stop"]
                    + "->"
                    + i["arrival_stop"]
                    for i in x
                ]
            )
        )
        .with_columns(num_transfers=pl.col("combined_stops").list.len() - 1)
        .with_columns(
            num_transfers=pl.when(pl.col("num_transfers") < 0)
            .then(0)
            .otherwise(pl.col("num_transfers"))
        )
        .with_columns(
            transit_stops=pl.col("transit_stops").map_elements(lambda x: "; ".join(x))
        )
        .with_columns(transit_stops=pl.col("transit_stops").str.replace_all("BUS", "🚌"))
        .with_columns(
            transit_stops=pl.col("transit_stops").str.replace_all("SUBWAY", "🚂")
        )
    )

    return out_df


# def parse_mode_duration(dur_col: str, mode: str, second_divisor: int) -> Column:
#     duration_array_seconds: Column = F.array_compact(
#         F.transform(
#             F.col("steps"),
#             lambda x: F.when(
#                 x["travel_mode"] == mode,
#                 x[dur_col]["value"].cast("int"),
#             ).otherwise(F.lit(None)),
#         ),
#     )
#     return (
#         F.aggregate(duration_array_seconds, F.lit(0), lambda acc, x: acc + x)
#         / second_divisor
#     )


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
        # .withColumn("walking_min", parse_mode_duration("duration", "WALKING", 60))
        # .withColumn("transit_min", parse_mode_duration("duration", "TRANSIT", 60))
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


def json_to_df(fs: gcsfs.GCSFileSystem, path: str) -> pl.DataFrame:
    with fs.open(path) as f:
        temp_df = pl.read_json(f.read())
    return temp_df


def parse_distance(run_type: str = "overwrite"):
    if run_type not in ["append", "overwrite"]:
        raise ValueError(f"run_type must be 'append' or 'overwrite', got {run_type}")

    fs = gcsfs.GCSFileSystem(project="homehuntr", token=os.getenv("GCP_AUTH_PATH"))
    transit_directions = []
    transit_directions_paths = [
        i for i in fs.ls("homehuntr-storage/directions") if i.endswith(" transit.json")
    ]

    with ThreadPoolExecutor(max_workers=64) as executor:
        transit_directions = list(
            executor.map(json_to_df, repeat(fs), transit_directions_paths)
        )
    transit_directions_raw = pl.concat(transit_directions, how="diagonal_relaxed")
    transit_directions_final = parse_transit_result(transit_directions_raw)

    transit_directions_final.head()

    # with fs.open("homehuntr/data/delta/transit_directions/", "wb") as f:
    #     transit_directions_final.write_delta(f, mode="overwrite")  # type: ignore


if __name__ == "__main__":
    parse_distance("overwrite")
