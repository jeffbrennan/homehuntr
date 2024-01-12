from dotenv import load_dotenv
import gcsfs
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os


def get_spark() -> SparkSession:
    load_dotenv()

    builder = (
        SparkSession.builder.appName("homehuntr")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark._jsc.hadoopConfiguration().set(
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
