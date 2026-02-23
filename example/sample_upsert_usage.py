"""
Example usage of DeltaUpsertEngine.

Note: Replace 'source_path' and 'target_path' with real locations.
"""

from framework.hash_utils import add_row_hash
from framework.logging_utils import log_info
from framework.upsert_engine import DeltaUpsertEngine


def run_example(spark) -> None:
    log_info("Loading source data...")
    source_df = spark.read.parquet("source_path")

    log_info("Adding row hash for change detection...")
    source_df = add_row_hash(source_df, columns=["id", "name", "amount"])

    log_info("Initialising upsert engine...")
    engine = DeltaUpsertEngine(
        spark=spark,
        target_table_path="target_path",
        key_columns=["id"],
        hash_column="row_hash",
    )

    log_info("Executing upsert...")
    engine.execute_upsert(source_df)

    log_info("Upsert finished.")
