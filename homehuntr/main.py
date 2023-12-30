import argparse
from scrape_streeteasy import scrape_apartment_url
from travel import get_directions
from compute_distance import parse_distance
from clean_delta import dedupe_directions
from datetime import datetime as dt
import polars as pl
import os

# from clean_address import clean_address
# from create_obt import build_obt


def handle_missing_directions():
    """
    Can occur when the direction api produces an unexpected result or a new destination is added to the list
    Need to compare direction routing against actual and run the missing ones
    """
    destination_path = "homehuntr/data/destinations/destinations.json"
    destination_place_ids = pl.read_json(destination_path)["place_id"].to_list()

    origin_path = "homehuntr/data/address"
    origin_place_ids = (
        pl.concat(
            [pl.read_json(f"{origin_path}/{i}") for i in os.listdir(origin_path)],
            how="diagonal",
        )
        .select("place_id")
        .filter(pl.col("place_id").is_not_null())
        .unique()["place_id"]
        .to_list()
    )

    direction_types = ["bicycling", "transit"]

    expected_directions = []
    for origin in origin_place_ids:
        for destination in destination_place_ids:
            for direction_type in direction_types:
                expected_directions.append(
                    f"{origin}_{destination}_{direction_type}.json"
                )

    actual_directions = os.listdir("homehuntr/data/directions")

    missing_directions = list(set(expected_directions) - set(actual_directions))


def main():
    recheck_interval_days = 5
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", help="url to scrape", nargs="*", required=False)

    args = parser.parse_args()
    new_urls = args.url

    if new_urls is not None:
        new_url_data = [
            {"link": url, "date_added": f"{dt.now()}", "date_modified": None}
            for url in new_urls
        ]
        new_url_df = pl.DataFrame(new_url_data)
    else:
        new_url_df = pl.DataFrame([])
    current_df = pl.read_csv("homehuntr/data/links/links.csv")
    combined_df = (
        pl.concat([current_df, new_url_df], how="diagonal")
        .with_columns(rn=pl.col("date_added").cumcount().over("link"))
        .filter(pl.col("rn") == 0)
        .drop("rn")
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

    all_urls = url_df["link"].to_list()
    handle_missing_directions(all_urls)

    urls_to_scrape = url_df.filter(
        pl.col("date_modified").is_null()
        | (pl.col("days_since_modified") > recheck_interval_days)
    )["link"].to_list()

    for url in urls_to_scrape:
        scraping_result = scrape_apartment_url(url)
        get_directions(uid=scraping_result["uid"])

    parse_distance(run_type="overwrite")
    dedupe_directions()


if __name__ == "__main__":
    main()
