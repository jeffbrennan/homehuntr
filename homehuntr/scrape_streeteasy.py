import json
import os
from dotenv import load_dotenv
import requests
from lxml import html
from lxml.html import HtmlElement
from typing import TypedDict, Optional
from datetime import datetime as dt
import argparse
import uuid
from pathlib import Path
import re
import gcsfs


class PriceElement(TypedDict):
    price_change: str
    price: int
    availability: str
    is_fee: bool
    rented: bool
    rented_days_ago: Optional[str]


class Vitals(TypedDict):
    date_available: Optional[str]
    days_on_market: Optional[int]


class BuildingDetails(TypedDict):
    units: int
    stories: int
    year_built: int


class Home(TypedDict):
    uid: str
    url: str
    building_address: str
    neighborhood: str
    price_details: PriceElement
    vitals: Vitals
    building_details: BuildingDetails
    amenities: list
    times_saved: int


class CheckResult(TypedDict):
    url_exists: bool
    uid: str


class ScrapeResult(TypedDict):
    uid: str


def get_page_tree(url: str) -> HtmlElement:
    header = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
        "referer": "https://www.google.com/",
    }
    parsed_url = url.split("?")[0]
    page = requests.get(parsed_url, headers=header)
    tree = html.fromstring(page.content)

    page = requests.get(url, headers=header)
    tree = html.fromstring(page.content)
    return tree


def get_amenities(tree: HtmlElement) -> list:
    amenity_elements = tree.xpath("//div[@class='AmenitiesBlock']")
    amenities_raw = []
    for element in amenity_elements[0].getchildren():
        try:
            amenities_raw.append(
                element.text_content().strip().replace("\n", "").replace("\t", "")
            )
        except IndexError:
            pass
    amenities_clean = [
        i.split(" " * 166) for i in amenities_raw if "googletag" not in i
    ]
    amenities = [
        item.strip().upper() for sublist in amenities_clean for item in sublist
    ]
    return amenities


def get_price_elements(tree: HtmlElement) -> PriceElement:
    price_element = tree.xpath("//div[@class='details_info_price']")[0]
    price_info = [i.strip() for i in price_element.text_content().split("\n")]
    price_clean = [i.upper() for i in price_info if i != ""]

    rented = False
    rented_days_ago = None
    if len(price_clean) == 3:
        price_index = [i for i, s in enumerate(price_clean) if "$" in s][0]
        availability_index = [i for i, s in enumerate(price_clean) if "RENT" in s][0]
        fee_index = [i for i, s in enumerate(price_clean) if "FEE" in s]
        change_index = [
            i for i, s in enumerate(price_clean) if "\u2193" or "\u2191" in s
        ]

        price = price_clean[price_index]
        availability = price_clean[availability_index]

        if len(fee_index) == 0:
            fee_str = "FEE"
        else:
            fee_str = price_clean[fee_index[0]]

        if len(change_index) == 0:
            price_change = "unknown"
        else:
            price_change = price_clean[change_index[0]]
    elif len(price_clean) == 2:
        price = price_clean[0]
        availability = price_clean[1]
        fee_str = "FEE"
        price_change = "unknown"
    elif len(price_clean) == 5:
        fee_str = "FEE"
        price_change, price, availability, rented_str, rented_days_ago = price_clean
        rented = rented_str == "RENTED"

    else:
        price_change, price, availability, fee_str = price_clean

    if price_change == "\u2193":
        price_change = "decrease"
    elif price_change == "\u2191":
        price_change = "increase"
    else:
        price_change = "unknown"

    price = int(price.replace("$", "").replace(",", ""))
    fee = fee_str != "NO FEE"

    return {
        "price_change": price_change,
        "price": price,
        "availability": availability,
        "is_fee": fee,
        "rented": rented,
        "rented_days_ago": rented_days_ago,
    }


def get_building_address(tree: HtmlElement) -> str:
    building_address_raw = tree.xpath("//div[@class='backend_data BuildingInfo-item']")
    if len(building_address_raw) != 1:
        raise ValueError("Expecting exactly one building address")

    building_address_text = building_address_raw[0].text_content()
    building_address = re.sub(r"\s+", " ", building_address_text).strip()
    return building_address


def get_room_elements(tree: HtmlElement) -> list:
    room_elements = [
        i.text_content().upper()
        for i in tree.xpath("//div[@class='details_info']")[0]
        .getchildren()[0]
        .getchildren()
    ]
    return room_elements


def get_neighborhood(tree: HtmlElement) -> str:
    neighborhood = (
        tree.xpath("//div[@class='details_info']")[1]
        .getchildren()[0]
        .text_content()
        .strip()
        .split("\n")[-1]
        .replace("in", "")
        .strip()
    )
    return neighborhood


def get_building_details(tree: HtmlElement) -> BuildingDetails:
    building_details_raw = tree.xpath(
        "//li[@class='BuildingInfo-detail horizontal_list']"
    )

    if len(building_details_raw) != 3:
        raise ValueError("Expecting exactly three building details")

    units, stories, year_built = [
        i.text_content().strip() for i in building_details_raw
    ]
    return {
        "units": int(units.split(" ")[0]),
        "stories": int(stories.split(" ")[0]),
        "year_built": int(year_built.split(" ")[0]),
    }


def get_vitals(tree: HtmlElement) -> Vitals:
    vitals = tree.xpath("//div[@class='Vitals-detailsInfo']")
    date_available_raw, days_on_market = [
        i.text_content().strip().split("\n")[-1].strip() for i in vitals
    ]
    if date_available_raw.upper() == "AVAILABLE NOW":
        date_available = dt.today().strftime("%Y-%m-%d")
    elif "No Longer Available" in date_available_raw:
        date_available = None
        days_on_market = None
    else:
        date_available = dt.strptime(date_available_raw, "%m/%d/%Y").strftime(
            "%Y-%m-%d"
        )
    return {"date_available": date_available, "days_on_market": days_on_market}


def get_num_times_saved(tree: HtmlElement) -> int:
    times_saved = tree.xpath("//div[@class='popularity']")

    if len(times_saved) != 1:
        raise ValueError("Expecting exactly one element")

    times_saved_text = times_saved[0].text_content()
    saved_listings = re.findall(r"\d+", times_saved_text)[0]

    return int(saved_listings)


def scrape_apartment_url(url) -> ScrapeResult:
    check_result = check_if_url_exists(url)
    if check_result["url_exists"]:
        return check_result

    print("Parsing address: ", url)

    tree = get_page_tree(url)

    address_uid = str(uuid.uuid4())
    building_address = get_building_address(tree)

    address_parsed: Home = {
        "uid": address_uid,
        "url": url,
        "building_address": building_address,
        "neighborhood": get_neighborhood(tree),
        "price_details": get_price_elements(tree),
        "vitals": get_vitals(tree),
        "building_details": get_building_details(tree),
        "amenities": get_amenities(tree),
        "times_saved": get_num_times_saved(tree),
    }

    address_path = f"gs://homehuntr-storage/address/{address_uid}.json"
    load_dotenv()
    fs = gcsfs.GCSFileSystem(project="homehuntr", token=os.getenv("GCP_AUTH_PATH"))
    with fs.open(address_path, "w") as f:
        json.dump(address_parsed, f, indent=4, ensure_ascii=False)

    print("Scraped address: ", building_address)
    return {"uid": address_uid}


def check_if_url_exists(url: str) -> CheckResult:
    def load_json(path):
        with open(path) as f:
            return json.load(f)

    # TODO: replace this with lookup against final parquet
    base_path = Path(__file__).parent / "data" / "address"
    all_addresses = [load_json(i) for i in base_path.glob("*.json")]

    url_exists = any([i["url"] == url for i in all_addresses])
    if url_exists:
        uid = [i["uid"] for i in all_addresses if i["url"] == url][0]
    else:
        uid = str(uuid.uuid4())

    output: CheckResult = {"url_exists": url_exists, "uid": uid}
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", help="url to scrape", required=True)
    args = parser.parse_args()

    result = scrape_apartment_url(args.url)
    print(result)
