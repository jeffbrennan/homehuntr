import gcsfs
import os
from dotenv import load_dotenv
import json

load_dotenv()
fs = gcsfs.GCSFileSystem(project="homehuntr", token=os.getenv("GCP_AUTH_PATH"))

directory_path = "homehuntr-storage/directions"

all_files = fs.ls(directory_path)
MIN_SPLIT_LEN = 12

for file_name in all_files:
    expected_separators = 2

    if file_name.count("_") == 0:
        continue

    new_file_name_test = file_name.replace("_", " ")
    new_file_name_test_split = new_file_name_test.split(" ")
    split_lens = [len(split) for split in new_file_name_test_split]
    incorrect_split = [i for i, _ in enumerate(split_lens) if i <= MIN_SPLIT_LEN]
    new_file_name = new_file_name_test_split[0]
    for i, split in enumerate(new_file_name_test_split[1:]):
        split_len = len(split)
        if split_len <= MIN_SPLIT_LEN and ".json" not in split:
            new_file_name += f"_{split}"
        else:
            new_file_name += f" {split}"

    with fs.open(file_name, "rb") as f:
        data = json.load(f)

    origin_place_id = data["geocoded_waypoints"][0]["place_id"]
    destination_place_id = data["geocoded_waypoints"][1]["place_id"]

    parsed_origin, parsed_destination, parsed_mode = new_file_name.replace(
        f"{directory_path}/", ""
    ).split(" ")

    assert parsed_origin == origin_place_id
    assert parsed_destination == destination_place_id
    assert new_file_name.count(" ") == expected_separators

    print(f"Renaming {file_name} to {new_file_name}")
    print("done")
    with fs.open(new_file_name, "w") as f:
        json.dump(data, f)

    fs.rm(file_name)
