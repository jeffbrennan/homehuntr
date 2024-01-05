import argparse
from scrape_streeteasy import scrape_apartment_url
from travel import get_directions, request_directions, Place
from compute_distance import parse_distance
from clean_delta import dedupe_directions
from datetime import datetime as dt
import polars as pl
import os

# from clean_address import clean_address
# from create_obt import build_obt


def handle_missing_directions() -> None:
    """
    Can occur when the direction api produces an unexpected result or a new destination is added to the list
    Need to compare direction routing against actual and run the missing ones
    """
    destination_path = "homehuntr/data/destinations/destinations.json"
    destination_place_ids = pl.read_json(destination_path)["place_id"].to_list()

    origin_path = "homehuntr/data/address"
    origin_data = pl.concat(
        [pl.read_json(f"{origin_path}/{i}") for i in os.listdir(origin_path)],
        how="diagonal",
    )

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
                    f"{origin}_{destination}_{direction_type}.json"
                )

    actual_directions: list[str] = os.listdir("homehuntr/data/directions")

    missing_directions = list(set(expected_directions) - set(actual_directions))
    if len(missing_directions) == 0:
        return

    for missing_direction in missing_directions:
        origin_id, destination_id, direction_type = missing_direction.split("_")
        origin_address = origin_data.filter(pl.col("place_id") == origin_id)[
            "building_address"
        ].to_list()[0]

        destination_address = pl.read_json(destination_path).filter(
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
        request_directions(origin=origin, destination=destination, mode=direction_type)


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

    args = parser.parse_args()
    if args.url is not None:
        new_url_data = [
            {"link": url, "date_added": f"{dt.now()}", "date_modified": None}
            for url in args.url
        ]
        new_url_df = pl.DataFrame(new_url_data)
    else:
        new_url_df = pl.DataFrame([])

    current_df = pl.read_csv("homehuntr/data/links/links.csv")
    combined_df = (
        pl.concat([current_df, new_url_df], how="diagonal")
        .with_columns(rn=pl.col("date_added").cum_count().over("link"))
        .filter(pl.col("rn") == 0)
        .drop("rn")
        .with_columns(
            date_added=pl.when(pl.col("date_added").is_null())
            .then(pl.lit(dt.now()))
            .otherwise(pl.col("date_added"))
        )
    )

    combined_df.write_csv("homehuntr/data/links/links.csv")

    url_df = (
        combined_df.with_columns(
            date_added=pl.col("date_added").str.strptime(
                pl.Datetime, format="%Y-%m-%d %H:%M:%S%.f"
            )
        )
        .with_columns(
            date_modified=pl.col("date_modified").str.strptime(
                pl.Datetime, format="%Y-%m-%d %H:%M:%S%.f"
            )
        )
        .with_columns(today=pl.lit(dt.now()))
        .with_columns(days_since_modified=pl.col("date_modified") - pl.lit(dt.now()))
    )

    handle_missing_directions()
    new_urls = url_df.filter(pl.col("date_modified").is_null())["link"].to_list()
    stale_urls = url_df.filter(pl.col("days_since_modified") > recheck_interval_days)[
        "link"
    ].to_list()

    if len(new_urls) > 0:
        for url in new_urls:
            scraping_result = scrape_apartment_url(url)
            get_directions(uid=scraping_result["uid"])

    if len(stale_urls) > 0:
        for url in stale_urls:
            scraping_result = scrape_apartment_url(url)
            if args.refresh_directions:
                get_directions(uid=scraping_result["uid"])
    parse_distance(run_type="overwrite")
    dedupe_directions()


if __name__ == "__main__":
    main()
