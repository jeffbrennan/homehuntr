from concurrent.futures import ThreadPoolExecutor
import json
from typing import Literal
import polars as pl
import gcsfs
import os
from itertools import repeat
from functools import reduce


VALID_STRUCT_KEYS = (
    tuple[
        Literal["transit_details"],
        Literal["line"] | Literal["departure_stop"] | Literal["arrival_stop"],
        Literal["short_name"] | Literal["name"],
    ]
    | tuple[
        Literal["transit_details"],
        Literal["line"],
        Literal["vehicle"],
        Literal["type"],
    ]
)


def get_initial_transit_cols(df: pl.DataFrame) -> pl.DataFrame:
    print("getting initial transit cols...")
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
            routes_struct=pl.col("routes").map_elements(lambda x: x[0]),
        )
        .unnest("routes_struct")
        .with_columns(legs_struct=pl.col("legs").map_elements(lambda x: x[0]))
        .unnest("legs_struct")
        .unnest("duration")
        .with_columns(duration=pl.col("value"))
        .drop("text")
        .drop("value")
        .unnest("distance")
        .with_columns(distance=pl.col("value"))
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
    col: pl.Expr,
    keys: VALID_STRUCT_KEYS,
    keep_nulls: bool = True,
) -> pl.Expr:
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
            pl.struct(
                ["vehicle_type", "transit_lines", "departure_stop", "arrival_stop"]
            ).alias("combined_stops"),
        )
        .group_by("origin_id", "destination_id")
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
        .drop("combined_stops")
    )

    return out_df


def parse_mode_duration(
    col: pl.Expr, dur_col: str, mode: str, second_divisor: int
) -> pl.Expr:
    """
    Parses all instances of a given mode from a list of structs and returns the sum of the durations
    Values are provided in seconds, second divisor provides option to convert to minutes, hours
    """
    return col.map_elements(
        lambda x: [
            i[dur_col]["value"] / second_divisor for i in x if i["travel_mode"] == mode
        ]
    ).list.sum()


def drop_bad_directions(fs: gcsfs.GCSFileSystem, df: pl.DataFrame) -> None:
    paths = (
        df.select("origin_id", "destination_id")
        .unique()
        .with_columns(path=pl.col("origin_id") + pl.lit(" ") + pl.col("destination_id"))
        .select("path")
        .to_dict()["path"]
        .to_list()
    )

    if isinstance(paths, str):
        paths = [paths]

    for path in paths:
        path_to_remove = f"gs://homehuntr-storage/directions/{path} transit.json"
        print(f"Removing {path_to_remove}")
        fs.rm(path_to_remove)


def parse_transit_result(fs: gcsfs.GCSFileSystem, df: pl.DataFrame) -> pl.DataFrame:
    max_duration_min = 60 * 2
    transit_initial_selection = get_initial_transit_cols(df)
    parsed_stops = parse_stops(transit_initial_selection)

    transit_stops_combined = transit_initial_selection.join(
        parsed_stops, on=["origin_id", "destination_id"]
    )

    print("Parsing transit directions...")
    transit_directions = (
        transit_stops_combined.with_columns(distance_mi=pl.col("distance") / 1609.34)
        .with_columns(distance_mi=pl.col("distance_mi").round(2))
        .with_columns(duration_min=pl.col("duration") / 60)
        .with_columns(duration_min=pl.col("duration_min").ceil())
        .with_columns(
            walking_min=parse_mode_duration(pl.col("steps"), "duration", "WALKING", 60)
        )
        .with_columns(
            transit_min=parse_mode_duration(pl.col("steps"), "duration", "TRANSIT", 60)
        )
        .with_columns(
            waiting_min=pl.col("duration_min")
            - pl.col("walking_min")
            - pl.col("transit_min")
        )
    )

    bad_directions = transit_directions.filter(
        pl.col("duration_min") > max_duration_min
    )
    n_bad_directions = bad_directions.select(pl.count()).item()
    if n_bad_directions > 0:
        raise ValueError(f"Expecting no bad directions, found {n_bad_directions}")
        #  TODO: remove error after more testing
        drop_bad_directions(fs, bad_directions)

    transit_directions_final = transit_directions.filter(
        pl.col("duration_min") < max_duration_min
    ).select(
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


def json_to_dict(fs: gcsfs.GCSFileSystem, path: str) -> pl.DataFrame:
    with fs.open(path) as f:
        d = json.loads(f.read())
    return d


def parse_distance(run_type: str = "overwrite"):
    if run_type not in ["append", "overwrite"]:
        raise ValueError(f"run_type must be 'append' or 'overwrite', got {run_type}")

    token = os.getenv("GCP_AUTH_PATH")
    if token is None:
        raise ValueError("GCP_AUTH_PATH environment variable must be set")

    fs = gcsfs.GCSFileSystem(project="homehuntr", token=token)
    transit_directions = []
    transit_directions_paths = [
        i for i in fs.ls("homehuntr-storage/directions") if i.endswith(" transit.json")
    ]

    with ThreadPoolExecutor(max_workers=128) as executor:
        transit_directions = list(
            executor.map(json_to_dict, repeat(fs), transit_directions_paths)
        )

    transit_directions_raw = pl.DataFrame(transit_directions)
    transit_directions_final = parse_transit_result(fs, transit_directions_raw)

    # TODO: run pyspark on same dataset and compare results in separate test script
    transit_directions_final.write_delta(
        "gs://homehuntr-storage/delta/transit_directions_polars",
        mode="overwrite",
        storage_options={"SERVICE_ACCOUNT": token},
    )


if __name__ == "__main__":
    parse_distance("overwrite")
