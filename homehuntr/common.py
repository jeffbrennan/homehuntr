from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("MyApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    return spark
