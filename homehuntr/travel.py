import requests
import os
from dotenv import load_dotenv
import json
from pathlib import Path
import argparse
from typing import Optional, TypedDict


class Place(TypedDict):
    place_id: str
    address: str


def get_place_id(address: str) -> Optional[str]:
    load_dotenv()
    MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
    request_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"

    params = {
        "input": address,
        "inputtype": "textquery",
        "fields": "place_id",
        "key": MAPS_API_KEY,
    }
    print(f"Requesting place id for {address}")
    response = requests.get(request_url, params=params)

    if response.json()["status"] != "OK":
        raise ValueError(response.json()["status"])

    if response.status_code != 200:
        raise ValueError(
            f"Place ID request failed for {address}: {response.status_code}"
        )

    return response.json()["candidates"][0]["place_id"]


def request_directions(origin: Place, destination: Place, mode: str) -> None:
    load_dotenv()
    MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
    request_url = "https://maps.googleapis.com/maps/api/directions/json"

    params = {
        "destination": destination["place_id"],
        "mode": mode,
        "origin": origin["place_id"],
        "key": MAPS_API_KEY,
    }
    print(
        f"[{origin['address']}] [{mode}] - Requesting directions to {destination['address']}"
    )
    response = requests.get(request_url, params=params)

    if response.status_code == 200 and response.json()["status"] == "OK":
        dump_directions(response.json(), origin, destination, mode)


def dump_directions(
    directions: dict, origin: Place, destination: Place, mode: str
) -> None:
    file_name = f"{origin['place_id']}_{destination['place_id']}_{mode}"
    output_path = Path(__file__).parent / "data" / "directions" / f"{file_name}.json"
    with open(output_path, "w") as f:
        json.dump(directions, f)


def get_directions(address: Optional[str] = None, uid: Optional[str] = None) -> None:
    if address is None and uid is None:
        raise ValueError("Must provide either origin or uid")

    if address is None:
        with open(Path(__file__).parent / "data" / "address" / f"{uid}.json") as f:
            address_data = json.load(f)
        address = address_data["building_address"]

    if address is None:
        raise ValueError("Could not resolve address")

    if uid is None:
        uid = get_place_id(address)

    if uid is None:
        raise ValueError("Could not resolve uid")

    origin: Place = {"place_id": uid, "address": address}

    with open(
        Path(__file__).parent / "data" / "destinations" / "destinations.json"
    ) as f:
        destination_data = json.load(f)

    destinations: list[Place] = [
        {"place_id": x["place_id"], "address": x["address"]} for x in destination_data
    ]

    for destination in destinations:
        request_directions(origin, destination, "transit")
        request_directions(origin, destination, "bicycling")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("origin", help="The origin address")
    args = parser.parse_args()
    get_directions(address=args.origin)


if __name__ == "__main__":
    main()
