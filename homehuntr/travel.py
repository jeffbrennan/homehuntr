import requests
import os
from dotenv import load_dotenv
import json
from pathlib import Path
import argparse


def get_directions(origin: str, destination: str, mode: str) -> dict:
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
