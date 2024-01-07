from typing import TypedDict
from google.cloud import storage
from pathlib import Path
import os
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat


class FolderResult(TypedDict):
    folder_name: str
    files: list[Path]


def upload_to_bucket(blob_name: str, path_to_file: str, bucket_name: str) -> str:
    """Upload data to a bucket"""
    credential_path = (
        str(Path(__file__).parent.parent.parent) + "/terraform/terraform.json"
    )

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


def upload_file(file_path: Path, parent_folder_name: str, bucket_name: str) -> None:
    file_path_start = file_path.parts.index(parent_folder_name)
    file_name = "/".join(file_path.parts[file_path_start:])
    local_path = str(Path(__file__).parent.parent) + "/data/" + file_name

    print(f"Uploading {file_name} to {bucket_name}")
    upload_to_bucket(file_name, local_path, bucket_name)


def upload_folder(parent_folder_name: str, bucket_name: str) -> None:
    files = get_folder_files(parent_folder_name)

    if len(files) == 0:
        print(f"Folder {parent_folder_name} is empty")
        return

    with ThreadPoolExecutor(max_workers=32) as executor:
        executor.map(
            upload_file, files, repeat(parent_folder_name), repeat(bucket_name)
        )


def main():
    BUCKET_NAME = "homehuntr-storage"
    folders_to_upload = ["address", "destinations", "delta", "directions", "links"]
    for folder_name in folders_to_upload:
        upload_folder(folder_name, BUCKET_NAME)


if __name__ == "__main__":
    main()
