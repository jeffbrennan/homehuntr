from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
import json
from dotenv import load_dotenv
import gcsfs
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
import polars as pl


def get_spark() -> SparkSession:
    load_dotenv()

    builder = (
        SparkSession.builder.appName("homehuntr")  # type: ignore
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark._jsc.hadoopConfiguration().set(  # type: ignore
        "google.cloud.auth.service.account.json.keyfile", os.getenv("GCP_AUTH_PATH")
    )
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    spark.conf.set(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )
    spark.conf.set(
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    )

    return spark


def get_gcp_fs() -> tuple[gcsfs.GCSFileSystem, str]:
    load_dotenv()

    token = os.getenv("GCP_AUTH_PATH")
    if token is None:
        raise ValueError("GCP_AUTH_PATH environment variable must be set")

    fs = gcsfs.GCSFileSystem(project="homehuntr", token=token)
    return fs, token


def drop_link(fs: gcsfs.GCSFileSystem, url: str):
    link_path = "gs://homehuntr-storage/links/links.csv"
    with fs.open(link_path, "rb") as f:
        links = pl.read_csv(f.read())

    links_new = links.filter(pl.col("link") != url)
    n_original_links = links.shape[0]
    n_new_links = links_new.shape[0]

    if n_original_links - n_new_links > 1:
        raise ValueError(f"Expecting one or 0 fewer links. Got: {n_original_links}")

    print("dropping link...")
    with fs.open(link_path, "wb") as f:
        links_new.write_csv(f)  # type: ignore


def drop_directions(fs: gcsfs.GCSFileSystem, place_id: str) -> None:
    all_direction_paths = fs.ls("gs://homehuntr-storage/directions/")
    paths_to_delete = [i for i in all_direction_paths if place_id in i]
    if len(paths_to_delete) == 0:
        print("No directions found for place_id. Skipping.")
        return

    for path_to_delete in paths_to_delete:
        fs.rm(path_to_delete)

    print(f"deleting directions [n={len(paths_to_delete)}]")


def delete_from_delta(
    token: str, table_path: str, where_clause: dict[str, str]
) -> None:
    current_df = pl.read_delta(table_path, storage_options={"SERVICE_ACCOUNT": token})
    new_df = current_df.filter(
        pl.col(where_clause["col_name"]) != where_clause["col_value"]
    )

    print(f"deleting entry from {table_path}")
    new_df.write_delta(
        table_path, mode="overwrite", storage_options={"SERVICE_ACCOUNT": token}
    )


def drop_summaries(token: str, place_id: str) -> None:
    delta_tables_to_update = {
        "gs://homehuntr-storage/delta/gold/obt": {
            "col_name": "place_id",
            "col_value": place_id,
        },
        "gs://homehuntr-storage/delta/gold/summary": {
            "col_name": "place_id",
            "col_value": place_id,
        },
        "gs://homehuntr-storage/delta/gold/apartment_details": {
            "col_name": "place_id",
            "col_value": place_id,
        },
        "gs://homehuntr-storage/delta/gold/transit_score": {
            "col_name": "origin_id",
            "col_value": place_id,
        },
    }
    with ThreadPoolExecutor(max_workers=len(delta_tables_to_update)) as executor:
        executor.map(
            delete_from_delta,
            repeat(token),
            delta_tables_to_update.keys(),
            delta_tables_to_update.values(),
        )

    for table_path, where_clause in delta_tables_to_update.items():
        delete_from_delta(token=token, table_path=table_path, where_clause=where_clause)


def drop_home(uid: str, url: str, fs: gcsfs.GCSFileSystem, token: str) -> None:
    print("dropping home...")
    address_base_path = "homehuntr-storage/address"
    saved_addresses = fs.ls(f"gs://{address_base_path}")
    if f"{address_base_path}/{uid}.json" not in saved_addresses:
        drop_link(fs=fs, url=url)
        return

    with fs.open(f"gs://{address_base_path}/{uid}.json", "rb") as f:
        address_data = json.loads(f.read())

    url = address_data["url"]

    if "place_id" in address_data:
        place_id = address_data["place_id"]
        drop_directions(fs=fs, place_id=place_id)
        drop_summaries(token, place_id=place_id)

    if url is not None:
        drop_link(fs=fs, url=url)
        fs.rm(f"gs://homehuntr-storage/address/{uid}.json")
