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


class AddressDetails(TypedDict):
    place_id: str
    place_lat: str
    place_lng: str


def validate_response(response: requests.Response) -> None:
    if response.json()["status"] != "OK":
        raise ValueError(response.json()["status"])

    if response.status_code != 200:
        raise ValueError(f"{response.status_code}")


def get_address_details(address: str) -> AddressDetails:
    load_dotenv()
    MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
    request_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"

    params = {
        "input": address,
        "inputtype": "textquery",
        "fields": "place_id,geometry",
        "key": MAPS_API_KEY,
    }
    print(f"Requesting place id for {address}")
    response = requests.get(request_url, params=params)
    validate_response(response)

    response_address = response.json()["candidates"][0]

    return {
        "place_id": response_address["place_id"],
        "place_lat": response_address["geometry"]["location"]["lat"],
        "place_lng": response_address["geometry"]["location"]["lng"],
    }


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
    """
    Obtains details about an address given either an address string or a uid (existing address)
    """
    place_id = None

    if uid is not None:
        # when get_directions is called as part of pipeline, we need to get the address, place_id
        home_path = Path(__file__).parent / "data" / "address" / f"{uid}.json"
        with open(home_path) as f:
            address_data = json.load(f)
        address = address_data["building_address"]
        if address is None:
            raise ValueError("Could not resolve address")

        data_elements = ["place_id", "address", "place_lat", "place_lng"]
        data_complete = all([x in address_data for x in data_elements])
        if data_complete:
            place_id = address_data["place_id"]
            return {
                "place_id": address_data["place_id"],
                "address": address,
            }

        # missing either place_id or place geolocation
        details = get_address_details(address)
        data_complete = all([x in details for x in data_elements])
        address_info = {
            "place_id": place_id,
            "place_lat": details["place_lat"],
            "place_lng": details["place_lng"],
        }
        address_data.update(address_info)

        with open(home_path, "w") as f:
            json.dump(address_data, f, indent=4, ensure_ascii=False)

        return {
            "place_id": address_data["place_id"],
            "address": address_data["building_address"],
        }

    if address is None:
        raise ValueError("Could not resolve address")

    address_details = get_address_details(address)
    place_id = address_details["place_id"]
    if place_id is None:
        raise ValueError("Could not resolve place id")

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
