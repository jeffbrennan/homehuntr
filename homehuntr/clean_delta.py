import pyspark.sql.functions as F

from common import get_spark
from delta import DeltaTable


def dedupe_delta(delta_path: str, partition_cols: list, order_cols: list):
    """
    Utility to remove duplicate rows from a delta table
    """
    df = spark.read.format("delta").load(delta_path)
    df.createOrReplaceTempView("df")

    partition_str = ", ".join(partition_cols)
    order_str = ", ".join(order_cols)

    df_no_dupes = (
        spark.sql(
            f"""
                SELECT *,
                ROW_NUMBER() OVER (PARTITION BY {partition_str} ORDER BY {order_str}) rn
                FROM df
                """
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    df_no_dupes.write.format("delta").mode("overwrite").save(delta_path)

    delta_table = DeltaTable.forPath(spark, delta_path)
    delta_table.optimize()
    delta_table.vacuum(0)


spark = get_spark()
dedupe_delta(
    delta_path="homehuntr/data/delta/transit_directions",
    partition_cols=["origin_id", "destination_id"],
    order_cols=["transit_min"],
)
