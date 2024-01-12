import argparse
from homehuntr import common
from scrape_streeteasy import scrape_apartment_url
from travel import get_directions, request_directions, Place
from compute_distance import parse_distance
from clean_delta import dedupe_directions
from datetime import datetime as dt
import polars as pl
import os
from dotenv import load_dotenv

import gcsfs
from gcsfs.core import GCSFileSystem

# from clean_address import clean_address
# from create_obt import build_obt


def handle_missing_directions(fs: GCSFileSystem) -> None:
    """
    Can occur when the direction api produces an unexpected result or a new destination is added to the list
    Need to compare direction routing against actual and run the missing ones
    """
    destination_path = "gs://homehuntr-storage/destinations/destinations.json"
    with fs.open(destination_path, "rb") as f:
        destination_place_df = pl.read_json(f.read())
        destination_place_ids = destination_place_df["place_id"].to_list()

    origin_path = "gs://homehuntr-storage/address"
    all_origin_dfs = []
    origin_files = fs.ls(origin_path)
    for origin_file in origin_files:
        with fs.open(origin_file, "rb") as f:
            all_origin_dfs.append(pl.read_json(f.read()))  # type: ignore

    origin_data = pl.concat(all_origin_dfs, how="diagonal_relaxed")

    origin_place_ids = (
        origin_data.select("place_id")
        .filter(pl.col("place_id").is_not_null())
        .unique()["place_id"]
        .to_list()
    )

    direction_types = ["bicycling", "transit"]

    expected_directions: list[str] = []
    for origin in origin_place_ids:
        for destination in destination_place_ids:
            for direction_type in direction_types:
                expected_directions.append(
                    f"{origin} {destination} {direction_type}.json"
                )

    direction_path = "homehuntr-storage/directions/"
    actual_directions: list[str] = fs.ls(direction_path)
    actual_directions = [i.replace(direction_path, "") for i in actual_directions]
    missing_directions = list(set(expected_directions) - set(actual_directions))
    if len(missing_directions) == 0:
        return

    for missing_direction in missing_directions:
        missing_direction = missing_direction.replace(".json", "")
        origin_id, destination_id, direction_type = missing_direction.split(" ")
        origin_address = origin_data.filter(pl.col("place_id") == origin_id)[
            "building_address"
        ].to_list()[0]

        with fs.open(destination_path, "rb") as f:
            destination_address = pl.read_json(f.read()).filter(
                pl.col("place_id") == destination_id
            )["address"][0]

        origin: Place = {
            "place_id": origin_id,
            "address": origin_address,
        }

        destination: Place = {
            "place_id": destination_id,
            "address": destination_address,
        }

        print(f"Requesting {origin_address}->{destination_address} [{direction_type}]")
        request_directions(
            origin=origin, destination=destination, mode=direction_type, fs=fs
        )


def update_last_modified(
    url_df: pl.DataFrame, url: str, destination: str, fs: GCSFileSystem
) -> pl.DataFrame:
    url_df = url_df.with_columns(
        date_modified=pl.when(pl.col("link") == url)
        .then(pl.lit(dt.now()))
        .otherwise(pl.col("date_modified"))
    )

    with fs.open(destination, "wb") as f:
        url_df.write_csv(f)  # type: ignore
    return url_df


def main():
    recheck_interval_days = 5
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", help="url to scrape", nargs="*", required=False)
    parser.add_argument(
        "--refresh-directions",
        help="refresh directions",
        action="store_true",
        required=False,
    )
    DT_FORMAT = "%Y-%m-%dT%H:%M:%S%.f"

    args = parser.parse_args()

    refresh_directions = False
    if args.refresh_directions:
        refresh_directions = True

    if args.url is not None:
        new_url_data = [
            {
                "link": url,
                "date_added": f"{dt.now().isoformat()}",
                "date_modified": None,
            }
            for url in args.url
        ]
        new_url_df = pl.DataFrame(new_url_data)
    else:
        new_url_df = pl.DataFrame([])

    link_path = "gs://homehuntr-storage/links/links.csv"
    fs, _ = common.get_gcp_fs()

    with fs.open(link_path, "rb") as f:
        current_df = pl.read_csv(f)

    combined_df = (
        pl.concat([current_df, new_url_df], how="diagonal")
        .with_columns(rn=pl.col("date_added").cum_count().over("link"))
        .filter(pl.col("rn") == 0)
        .drop("rn")
        .with_columns(
            date_added=pl.when(pl.col("date_added").is_null())
            .then(pl.lit(dt.now().isoformat()))
            .otherwise(pl.col("date_added"))
        )
    )

    with fs.open(link_path, "wb") as f:
        combined_df.write_csv(f)  # type: ignore

    url_df = (
        combined_df.with_columns(
            date_added=pl.col("date_added").str.strptime(pl.Datetime, format=DT_FORMAT)
        )
        .with_columns(
            date_modified=pl.col("date_modified").str.strptime(
                pl.Datetime, format=DT_FORMAT
            )
        )
        .with_columns(today=pl.lit(dt.now()))
        .with_columns(days_since_modified=pl.col("date_modified") - pl.lit(dt.now()))
    )

    handle_missing_directions(fs)
    new_urls = url_df.filter(pl.col("date_modified").is_null())["link"].to_list()
    stale_urls = url_df.filter(pl.col("days_since_modified") > recheck_interval_days)[
        "link"
    ].to_list()
    url_df = url_df.drop("today", "days_since_modified")

    # new urls need to be scraped and have directions requested
    if len(new_urls) > 0:
        for url in new_urls:
            scraping_result = scrape_apartment_url(url)
            get_directions(uid=scraping_result["uid"], refresh_directions=True)
            url_df = update_last_modified(url_df, url, link_path, fs)

    # stale urls need to have directions requested only if refresh_directions is True
    if len(stale_urls) > 0:
        for url in stale_urls:
            scraping_result = scrape_apartment_url(url)
            get_directions(
                uid=scraping_result["uid"], refresh_directions=refresh_directions
            )
            url_df = update_last_modified(url_df, url, link_path, fs)

    parse_distance(run_type="overwrite")
    dedupe_directions()


if __name__ == "__main__":
    main()
