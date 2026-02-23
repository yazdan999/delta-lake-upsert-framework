"""
Example usage of DeltaUpsertEngine.
"""

from framework.upsert_engine import DeltaUpsertEngine
from framework.hash_utils import add_row_hash


def run_example(spark):
    source_df = spark.read.parquet("source_path")

    source_df = add_row_hash(
        source_df,
        columns=["id", "name", "amount"]
    )

    engine = DeltaUpsertEngine(
        spark=spark,
        target_table_path="target_path",
        key_columns=["id"]
    )

    engine.execute_upsert(source_df)
