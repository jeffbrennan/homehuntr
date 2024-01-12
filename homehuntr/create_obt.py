import json
import polars as pl
import os
from dotenv import load_dotenv
import gcsfs

from homehuntr import common


def normalize_data(col: pl.Expr) -> pl.Expr:
    col_max = col.max()
    col_min = col.min()
    if col_max is None:
        raise Exception("col_max is None")
    if col_min is None:
        raise Exception("col_min is None")
    return (col - col_min) / (col_max - col_min)  # type: ignore


def col_has_value(col, substr):
    return col.str.contains(substr).cast(int)


def clean_address(df: pl.DataFrame) -> pl.DataFrame:
    amenity_weights = {
        "OUTDOOR": 0.8,
        "DISHWASHER": 1,
        "LAUNDRY": 0.9,
        "PETS": 0.3,
        "ELEVATOR": 0.5,
        "CENTRAL AIR": 0.2,
        "ROOF": 0.5,
    }

    apartment_score = {"price": 5, "amenities": 2}

    load_dotenv()
    MAX_PRICE = os.getenv("MAX_PRICE")
    MIN_PRICE = os.getenv("MIN_PRICE")

    if MAX_PRICE is None or MIN_PRICE is None:
        raise Exception("MAX_PRICE and MIN_PRICE must be set in .env file")

    MAX_PRICE = int(MAX_PRICE)
    MIN_PRICE = int(MIN_PRICE)

    address_cleaned = (
        df.unnest("price_details", "building_details", "vitals")
        .with_columns(
            price_with_fee=pl.when(pl.col("is_fee"))
            .then(pl.col("price") + (pl.col("price") * (1 / 12)))
            .otherwise(pl.col("price"))
        )
        .with_columns(amenity_string=pl.col("amenities").list.join("|"))
        .with_columns(
            has_outdoors=col_has_value(pl.col("amenity_string"), "GARDEN|OUTDOOR")
        )
        .with_columns(
            has_dishwasher=col_has_value(pl.col("amenity_string"), "DISHWASHER")
        )
        .with_columns(
            has_laundry=col_has_value(pl.col("amenity_string"), "LAUNDRY|DRYER")
        )
        .with_columns(has_pets=col_has_value(pl.col("amenity_string"), "PETS"))
        .with_columns(has_elevator=col_has_value(pl.col("amenity_string"), "ELEVATOR"))
        .with_columns(
            has_central_air=col_has_value(pl.col("amenity_string"), "CENTRAL AIR")
        )
        .with_columns(has_roof=col_has_value(pl.col("amenity_string"), "ROOF"))
        .with_columns(
            amenity_score_sum=pl.col("has_outdoors") * amenity_weights["OUTDOOR"]
            + pl.col("has_dishwasher") * amenity_weights["DISHWASHER"]
            + pl.col("has_laundry") * amenity_weights["LAUNDRY"]
            + pl.col("has_pets") * amenity_weights["PETS"]
            + pl.col("has_elevator") * amenity_weights["ELEVATOR"]
            + pl.col("has_central_air") * amenity_weights["CENTRAL AIR"]
            + pl.col("has_roof") * amenity_weights["ROOF"]
        )
        .with_columns(amenity_score=pl.col("amenity_score_sum") / len(amenity_weights))
        .with_columns(
            price_score=1 - (pl.col("price_with_fee") - MIN_PRICE) / (MAX_PRICE - MIN_PRICE)  # type: ignore
        )
        .with_columns(
            apartment_score_raw=pl.col("price_score") * apartment_score["price"]
            + pl.col("amenity_score") * apartment_score["amenities"]
        )
        .with_columns(apartment_score=normalize_data(pl.col("apartment_score_raw")))
        .drop("amenitites", "amenity_string", "amenity_score_sum")
    )
    return address_cleaned


def get_clean_address(fs: gcsfs.GCSFileSystem):
    address_base_path = "gs://homehuntr-storage/address"
    all_addresses = fs.ls(address_base_path)
    address_data = []
    for i in all_addresses:
        with fs.open(f"gs://{i}", "r") as f:
            address_data.append(json.loads(f.read()))

    address_info = pl.DataFrame(address_data)
    address_cleaned = clean_address(address_info)
    return address_cleaned


def get_direction_df(token: str):
    direction_path = "gs://homehuntr-storage/delta/transit_directions"

    direction_info = pl.read_delta(
        direction_path, storage_options={"SERVICE_ACCOUNT": token}
    ).sort("origin_id")

    return direction_info


def get_destination_df(fs: gcsfs.GCSFileSystem):
    destination_path = "gs://homehuntr-storage/destinations"
    destination_files = fs.ls(destination_path)
    with fs.open(destination_files[0], "r") as f:
        destination_data = json.loads(f.read())
    destination_info_raw = pl.DataFrame(destination_data)

    destination_info = (
        destination_info_raw.drop("address")
        .explode("weights")
        .with_columns(person=pl.col("weights").struct.field("person"))
        .with_columns(weight=pl.col("weights").struct.field("weight"))
        .drop("weights")
        .filter(pl.col("weight") > 0)
        .sort("address_name")
    )
    return destination_info


def get_transit_score(direction_info, destination_info):
    transit_weights = {
        "duration_min": -0.5,
        "walking_min": -0.2,
        "num_transfers": -0.4,
        "waiting_min": -0.3,
    }
    transit_score_df = (
        direction_info.join(
            destination_info,
            left_on="destination_id",
            right_on="place_id",
            how="inner",
            validate="m:m",
        )
        .select(
            [
                "origin_id",
                "origin_name",
                "destination_id",
                "destination_name",
                "duration_min",
                "num_transfers",
                "walking_min",
                "transit_min",
                "waiting_min",
                "weight",
                "person",
            ]
        )
        .sort("origin_id")
    )

    scaled_df = (
        transit_score_df.select(
            "person",
            "destination_id",
            "origin_id",
            "weight",
            "num_transfers",
            "duration_min",
            "waiting_min",
            "walking_min",
        )
        .sort("person", "destination_id")
        .with_columns(
            travel_duration_weighted=pl.col("duration_min") * pl.col("weight")
        )
        .with_columns(
            walking_duration_weighted=pl.col("walking_min") * pl.col("weight")
        )
        .with_columns(
            waiting_duration_weighted=pl.col("waiting_min") * pl.col("weight")
        )
        .with_columns(
            travel_duration_scaled=normalize_data(
                pl.col("travel_duration_weighted")
            ).over(["person", "destination_id"])
        )
        .with_columns(
            travel_score=pl.col("travel_duration_scaled")
            * transit_weights["duration_min"]
        )
        .with_columns(
            walking_duration_scaled=normalize_data(pl.col("walking_min")).over(
                ["person", "destination_id"]
            )
        )
        .with_columns(
            walking_score=pl.col("walking_duration_scaled")
            * transit_weights["walking_min"]
        )
        .with_columns(
            waiting_duration_scaled=normalize_data(pl.col("waiting_min")).over(
                ["person", "destination_id"]
            )
        )
        .with_columns(
            waiting_score=pl.col("waiting_duration_scaled")
            * transit_weights["waiting_min"]
        )
        .with_columns(
            num_transfers_scaled=normalize_data(pl.col("num_transfers")).over(
                ["person", "destination_id"]
            )
        )
        .with_columns(
            num_transfers_scaled=pl.when(pl.col("num_transfers_scaled").is_nan())
            .then(0)
            .otherwise(pl.col("num_transfers_scaled"))
        )
        .with_columns(
            num_transfers_score=pl.col("num_transfers_scaled")
            * transit_weights["num_transfers"]
        )
        .with_columns(
            transit_score_raw=pl.col("travel_score")
            + pl.col("walking_score")
            + pl.col("waiting_score")
            + pl.col("num_transfers_score")
        )
        .with_columns(
            transit_score=normalize_data(pl.col("transit_score_raw")).over(
                ["person", "destination_id"]
            )
        )
        .with_columns(
            transit_score=pl.when(pl.col("transit_score").is_nan())
            .then(1)
            .otherwise(pl.col("transit_score"))
        )
    )

    return scaled_df


def create_obt(address_df, direction_df, destination_df, transit_score_df):
    obt = (
        address_df.join(
            direction_df,
            left_on="place_id",
            right_on="origin_id",
            how="left",
            validate="1:m",
        )
        .join(
            destination_df,
            left_on="destination_id",
            right_on="place_id",
            how="left",
            validate="m:1",
        )
        .join(
            transit_score_df,
            left_on=["place_id", "person"],
            right_on=["origin_id", "person"],
            how="left",
        )
        .filter(pl.col("place_id").is_not_null())
    )
    return obt


def summarize_scores(df: pl.DataFrame) -> pl.DataFrame:
    score_summary = (
        df.select(
            [
                "place_id",
                "building_address",
                "person",
                "apartment_score",
                "transit_score",
            ]
        )
        .unique()
        .with_columns(
            overall_score_raw=pl.col("apartment_score") * 0.6
            + pl.col("transit_score") * 0.4
        )
        .with_columns(
            overall_score=normalize_data(pl.col("overall_score_raw")).over(["person"])
        )
        .with_columns(rank=pl.col("overall_score").rank(descending=True).over("person"))
        .sort("place_id", "person", "rank")
        .select("place_id", "person", "apartment_score", "transit_score", "rank")
    )

    summary_df = (
        df.join(
            score_summary,
            left_on=["place_id", "person"],
            right_on=["place_id", "person"],
            how="left",
        )
        .select(
            [
                "place_id",
                "building_address",
                "url",
                "neighborhood",
                "price",
                "is_fee",
                "has_dishwasher",
                "has_laundry",
                "person",
                "apartment_score",
                "transit_score",
                "rank",
            ]
        )
        .unique()
        .sort("person", "rank")
    )
    return summary_df


def main() -> None:
    out_base_path = "gs://homehuntr-storage/delta/gold"
    fs, token = common.get_gcp_fs()

    address_cleaned = get_clean_address(fs)
    direction_df = get_direction_df()
    destination_df = get_destination_df(fs)
    transit_score_df = get_transit_score(
        direction_info=direction_df, destination_info=destination_df
    )
    obt = create_obt(
        address_df=address_cleaned,
        direction_df=direction_df,
        destination_df=destination_df,
        transit_score_df=transit_score_df,
    )
    summary_df = summarize_scores(obt)

    address_cleaned.write_delta(
        f"{out_base_path}/apartment_details",
        mode="overwrite",
        overwrite_schema=True,
        storage_options={"SERVICE_ACCOUNT": token},
    )

    transit_score_df.write_delta(
        f"{out_base_path}/transit_score",
        mode="overwrite",
        overwrite_schema=True,
        storage_options={"SERVICE_ACCOUNT": token},
    )

    summary_df.write_delta(
        f"{out_base_path}/summary",
        mode="overwrite",
        storage_options={"SERVICE_ACCOUNT": token},
    )

    obt.write_delta(
        f"{out_base_path}/obt",
        mode="overwrite",
        overwrite_schema=True,
        storage_options={"SERVICE_ACCOUNT": token},
    )


if __name__ == "__main__":
    main()
