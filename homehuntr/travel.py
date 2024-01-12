import requests
import os
from dotenv import load_dotenv
import json
import argparse
from typing import Optional, TypedDict
import gcsfs

from homehuntr import common


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


def request_directions(
    origin: Place, destination: Place, mode: str, fs: gcsfs.GCSFileSystem
) -> None:
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

    dump_directions(response.json(), origin, destination, mode, fs)


def dump_directions(
    directions: dict,
    origin: Place,
    destination: Place,
    mode: str,
    fs: gcsfs.GCSFileSystem,
) -> None:
    file_name = f"{origin['place_id']} {destination['place_id']} {mode}"
    output_path = f"gs://homehuntr-storage/directions/{file_name}.json"
    with fs.open(output_path, "w") as f:
        json.dump(directions, f)


def get_destinations(fs: gcsfs.GCSFileSystem) -> list[Place]:
    destination_path = "gs://homehuntr-storage/destinations/destinations.json"
    with fs.open(destination_path) as f:
        destination_data = json.load(f)

    destinations: list[Place] = [
        {"place_id": x["place_id"], "address": x["address"]} for x in destination_data
    ]
    return destinations


def get_origin(
    fs: gcsfs.GCSFileSystem, address: Optional[str] = None, uid: Optional[str] = None
) -> Place:
    """
    Obtains details about an address given either an address string or a uid (existing address)
    """
    place_id = None

    if uid is not None:
        # when get_directions is called as part of pipeline, we need to get the address, place_id
        home_path = f"gs://homehuntr-storage/address/{uid}.json"
        with fs.open(home_path) as f:
            address_data = json.load(f)

        address = address_data["building_address"]
        if address is None:
            raise ValueError("Could not resolve address")

        data_elements = ["place_id", "address", "place_lat", "place_lng"]
        data_complete = all([x in address_data for x in data_elements])
        if data_complete and address_data["place_id"] is not None:
            return {
                "place_id": address_data["place_id"],
                "address": address,
            }

        # missing either place_id or place geolocation
        details = get_address_details(address)
        address_info = {
            "place_id": details["place_id"],
            "place_lat": details["place_lat"],
            "place_lng": details["place_lng"],
        }
        address_data.update(address_info)

        with fs.open(home_path, "w") as f:
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


def get_directions(
    address: Optional[str] = None,
    uid: Optional[str] = None,
    refresh_directions: bool = False,
) -> None:
    """
    address argument: get_directions being called independently
    uid argment: get_directions being called as part of pipeline from main.py
    """
    modes = ["transit", "bicycling"]

    fs, _ = common.get_gcp_fs()
    origin = get_origin(fs, address, uid)
    if not refresh_directions:
        return
    destinations = get_destinations(fs)

    for destination in destinations:
        for mode in modes:
            directions_exist = check_if_directions_exist(origin, destination, mode, fs)
            if directions_exist:
                continue
            request_directions(origin, destination, mode, fs)


def check_if_directions_exist(origin: Place, destination: Place, mode: str, fs) -> bool:
    file_name = f"{origin['place_id']}_{destination['place_id']}_{mode}"
    output_path = f"gs://homehuntr-storage/directions/{file_name}.json"
    return output_path in fs.ls("homehuntr-storage/directions")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("origin", help="The origin address")
    args = parser.parse_args()
    get_directions(address=args.origin)


if __name__ == "__main__":
    main()
