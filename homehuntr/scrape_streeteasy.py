import json
import requests
from lxml import html
from lxml.html import HtmlElement
from typing import TypedDict
from datetime import datetime as dt
import argparse
import uuid


class PriceElement(TypedDict):
    price_change: str
    price: int
    availability: str
    is_fee: bool


class Vitals(TypedDict):
    date_available: str
    days_on_market: int


class Home(TypedDict):
    uid: str
    url: str
    building_address: str
    neighborhood: str
    price_details: PriceElement
    vitals: Vitals
    amenities: list


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
    }


def get_building_address(tree: HtmlElement) -> str:
    return tree.xpath("//h1[@class='building-title']")[0].text_content().strip()


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


def get_vitals(tree: HtmlElement) -> Vitals:
    vitals = tree.xpath("//div[@class='Vitals-detailsInfo']")
    date_available_raw, days_on_market = [
        i.text_content().strip().split("\n")[-1].strip() for i in vitals
    ]
    date_available = dt.strptime(date_available_raw, "%m/%d/%Y").strftime("%Y-%m-%d")
    return {"date_available": date_available, "days_on_market": days_on_market}


def parse_address(url) -> None:
    tree = get_page_tree(url)

    address_uid = str(uuid.uuid4())

    address_parsed: Home = {
        "uid": address_uid,
        "url": url,
        "building_address": get_building_address(tree),
        "neighborhood": get_neighborhood(tree),
        "price_details": get_price_elements(tree),
        "vitals": get_vitals(tree),
        "amenities": get_amenities(tree),
    }

    with open(f"homehuntr/data/address/{address_uid}.json", "w") as f:
        json.dump(address_parsed, f, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", help="url to scrape", required=True)
    args = parser.parse_args()
    parse_address(args.url)
