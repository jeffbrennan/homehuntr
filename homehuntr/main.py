import argparse
from scrape_streeteasy import scrape_apartment_url
from travel import get_directions
from compute_distance import parse_distance
from clean_delta import dedupe_directions
from datetime import datetime as dt
import polars as pl

# from clean_address import clean_address
# from create_obt import build_obt


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
