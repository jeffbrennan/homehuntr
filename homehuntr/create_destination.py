import json
from pathlib import Path
import argparse
from typing import TypedDict
from travel import get_address_details


class Destination(TypedDict):
    place_id: str
    address: str
    person: str | list[str]
    address_type: str | list[str]
    address_name: str
    weight: float


def clean_input(input: str) -> str | list[str]:
    if "[" in input and "]" and "," in input:
        input_list = input.split(",")
        input_clean = [
            i.replace("[", "").replace("]", "").replace('"', "").strip()
            for i in input_list
        ]
        return input_clean
    return input.replace('"', "").strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", help="address to add to destinations", required=True
    )
    parser.add_argument("--person", help="person to add to destinations", required=True)
    parser.add_argument(
        "--address_type", help="address_type to add to destinations", required=True
    )
    parser.add_argument(
        "--address_name", help="address_name to add to destinations", required=True
    )
    parser.add_argument("--weight", help="weight to add to destinations", required=True)

    args = parser.parse_args()
    address = clean_input(args.address)
    address_name = clean_input(args.address_name)
    person = clean_input(args.person)
    address_type = clean_input(args.address_type)

    if isinstance(address, list):
        raise ValueError("Address must be a single address")

    if isinstance(address_name, list):
        raise ValueError("Address name must be a single address name")

    place_id = get_address_details(address)

    if place_id is None:
        raise ValueError(f"Place ID not found for {address}. Exiting.")

    new_destination: Destination = {
        "place_id": place_id,
        "address": address,
        "person": person,
        "address_type": address_type,
        "address_name": address_name,
        "weight": float(args.weight),
    }

    with open(
        Path(__file__).parent / "data" / "destinations" / "destinations.json"
    ) as f:
        destination_data = json.load(f)

    destination_data.append(new_destination)

    with open(
        Path(__file__).parent / "data" / "destinations" / "destinations.json", "w"
    ) as f:
        json.dump(destination_data, f, indent=4)

    print(f"Added {address} to destinations.json")


if __name__ == "__main__":
    main()
