from typing import TypedDict
from google.cloud import storage
from pathlib import Path
import os


class FolderResult(TypedDict):
    folder_name: str
    files: list[Path]


def upload_to_bucket(blob_name: str, path_to_file: str, bucket_name: str):
    """Upload data to a bucket"""
    credential_path = (
        str(Path(__file__).parent.parent.parent) + "/terraform/terraform.json"
    )
    print(credential_path)
    storage_client = storage.Client.from_service_account_json(credential_path)

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_filename(path_to_file)

    return blob.public_url


def get_folder_files(folder_name: str) -> list[Path]:
    substr_to_exclude = ".DS_Store"
    folder_path = str(Path(__file__).parent.parent) + "/data/" + folder_name
    all_files = list(Path(folder_path).rglob("*"))

    files = [
        i for i in all_files if os.path.isfile(i) and substr_to_exclude not in str(i)
    ]
    return files


def upload_folder(parent_folder_name: str, bucket_name: str) -> None:
    files = get_folder_files(parent_folder_name)

    if len(files) == 0:
        print(f"Folder {parent_folder_name} is empty")
        return

    for file_path in files:
        file_path_start = file_path.parts.index(parent_folder_name)
        file_name = "/".join(file_path.parts[file_path_start:])
        local_path = str(Path(__file__).parent.parent) + "/data/" + file_name

        print(f"Uploading {file_name} to {BUCKET_NAME}")
        upload_to_bucket(file_name, local_path, BUCKET_NAME)


BUCKET_NAME = "homehuntr-storage"

# upload_folder("address", BUCKET_NAME)

upload_folder("destinations", BUCKET_NAME)

upload_folder("delta", BUCKET_NAME)

upload_folder("directions", BUCKET_NAME)

upload_folder("links", BUCKET_NAME)
