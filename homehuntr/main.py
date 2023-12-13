import argparse
from scrape_streeteasy import scrape_apartment_url
from travel import get_directions
from compute_distance import parse_distance
from clean_delta import dedupe_directions

# from clean_address import clean_address
# from create_obt import build_obt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", help="url to scrape", required=True)
    args = parser.parse_args()

    scraping_result = scrape_apartment_url(args.url)
    get_directions(uid=scraping_result["uid"])

    parse_distance(run_type="overwrite")
    dedupe_directions()

    # parse_address(address_uid)

    # build_obt()


if __name__ == "__main__":
    main()
