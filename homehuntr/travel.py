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


def validate_response(response: requests.Response) -> None:
    if response.json()["status"] != "OK":
        raise ValueError(response.json()["status"])

    if response.status_code != 200:
        raise ValueError(f"{response.status_code}")


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
    validate_response(response)
    return response.json()["candidates"][0]["place_id"]


def request_directions(origin: Place, destination: Place, mode: str) -> None:
    load_dotenv()
    MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
    request_url = "https://maps.googleapis.com/maps/api/directions/json"

    params = {
        "destination": f'place_id:{destination["place_id"]}',
        "mode": mode,
        "origin": f'place_id:{origin["place_id"]}',
        "key": MAPS_API_KEY,
    }
    print(
        f"[{origin['address']}] [{mode}] - Requesting directions to {destination['address']}"
    )
    response = requests.get(request_url, params=params)
    validate_response(response)

    dump_directions(response.json(), origin, destination, mode)


def dump_directions(
    directions: dict, origin: Place, destination: Place, mode: str
) -> None:
    file_name = f"{origin['place_id']}_{destination['place_id']}_{mode}"
    output_path = Path(__file__).parent / "data" / "directions" / f"{file_name}.json"
    with open(output_path, "w") as f:
        json.dump(directions, f)


def get_destinations() -> list[Place]:
    with open(
        Path(__file__).parent / "data" / "destinations" / "destinations.json"
    ) as f:
        destination_data = json.load(f)

    destinations: list[Place] = [
        {"place_id": x["place_id"], "address": x["address"]} for x in destination_data
    ]
    return destinations


def get_origin(address: Optional[str] = None, uid: Optional[str] = None) -> Place:
    place_id = None
    if address is None and uid is None:
        raise ValueError("Must provide either origin or uid")

    # TODO: fix spaghetti
    if uid is not None:
        # when get_directions is called as part of pipeline, we need to get the address, place_id
        home_path = Path(__file__).parent / "data" / "address" / f"{uid}.json"
        with open(home_path) as f:
            address_data = json.load(f)
        address = address_data["building_address"]
        if address is None:
            raise ValueError("Could not resolve address")

        if "place_id" in address_data:
            place_id = address_data["place_id"]
        else:
            place_id = get_place_id(address)
            if place_id is None:
                raise ValueError("Could not resolve place id")
            address_data["place_id"] = place_id
            with open(home_path, "w") as f:
                json.dump(address_data, f, indent=4, ensure_ascii=False)

    if uid is None and address is not None:
        # when get_directions is called independently, we need to get the place_id
        place_id = get_place_id(address)

    if place_id is None:
        raise ValueError("Could not resolve place id")

    if address is None:
        raise ValueError("Could not resolve address")

    origin: Place = {"place_id": place_id, "address": address}
    return origin


def get_directions(address: Optional[str] = None, uid: Optional[str] = None) -> None:
    """
    address argument: get_directions being called independently
    uid argment: get_directions being called as part of pipeline from main.py
    """
    modes = ["transit", "bicycling"]
    origin = get_origin(address, uid)
    destinations = get_destinations()

    for destination in destinations:
        for mode in modes:
            directions_exist = check_if_directions_exist(origin, destination, mode)
            if directions_exist:
                print("Skipping existing direction - ", origin, destination, mode)
                continue
            request_directions(origin, destination, mode)


def check_if_directions_exist(origin: Place, destination: Place, mode: str) -> bool:
    file_name = f"{origin['place_id']}_{destination['place_id']}_{mode}"
    output_path = Path(__file__).parent / "data" / "directions" / f"{file_name}.json"
    return output_path.exists()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("origin", help="The origin address")
    args = parser.parse_args()
    get_directions(address=args.origin)


if __name__ == "__main__":
    main()
