from homehuntr import common
from deltalake import DeltaTable
import polars as pl


def dedupe_delta(delta_path: str, partition_cols: list, order_cols: list):
    """
    Utility to remove duplicate rows from a delta table
    """

    _, token = common.get_gcp_fs()
    df = pl.read_delta(delta_path, storage_options={"SERVICE_ACCOUNT": token})

    if not isinstance(df, pl.DataFrame):
        raise ValueError("Delta table not found")

    if len(order_cols) > 1:
        raise ValueError("Only one order column is currently supported")

    df_no_dupes = (
        df.with_columns(
            rn=pl.col(order_cols).rank("ordinal", descending=True).over(partition_cols)
        )
        .filter(pl.col("rn") == 1)
        .drop("rn")
    )

    df_no_dupes.write_delta(
        delta_path, mode="overwrite", storage_options={"SERVICE_ACCOUNT": token}
    )

    delta_table = DeltaTable(delta_path, storage_options={"SERVICE_ACCOUNT": token})
    delta_table.optimize()
    delta_table.vacuum(retention_hours=1, enforce_retention_duration=False)


def dedupe_directions():
    dedupe_delta(
        delta_path="gs://homehuntr-storage/delta/transit_directions",
        partition_cols=["origin_id", "destination_id"],
        order_cols=["transit_min"],
    )
