import requests
import os
from dotenv import load_dotenv
import json
from pathlib import Path
import argparse
from typing import Optional


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


    load_dotenv()
    MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
    request_url = "https://maps.googleapis.com/maps/api/directions/json"

    params = {
        "destination": destination,
        "mode": mode,
        "origin": origin,
        "key": MAPS_API_KEY,
    }
    response = requests.get(request_url, params=params)

    if response.status_code == 200:
        dump_directions(response.json(), mode)


def dump_directions(directions: dict, mode) -> None:
    origin_uid = directions["geocoded_waypoints"][0]["place_id"]
    destination_uid = directions["geocoded_waypoints"][1]["place_id"]
    file_name = f"{origin_uid}_{destination_uid}_{mode}"
    output_path = Path(__file__).parent / "data" / "directions" / f"{file_name}.json"
    with open(output_path, "w") as f:
        json.dump(directions, f)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("origin", help="The origin address")
    args = parser.parse_args()

    with open(Path(__file__).parent / "data" / "destinations.json") as f:
        destinations = json.load(f)

    for destination in destinations:
        get_directions(args.origin, destination, "transit")
        get_directions(args.origin, destination, "bicycling")


if __name__ == "__main__":
    main()
