"""
Delta Lake Upsert Engine

Reusable PySpark-based Delta Lake merge framework with:
- Null-safe merge conditions
- Hash-based update detection
- Insert/update tracking
- Basic structured logging
"""

from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql.functions import col


class DeltaUpsertEngine:

    def __init__(self, spark, target_table_path: str, key_columns: list, hash_column: str = "row_hash"):
        self.spark = spark
        self.target_table_path = target_table_path
        self.key_columns = key_columns
        self.hash_column = hash_column

    def _build_merge_condition(self):
        conditions = [
            f"target.{c} <=> source.{c}"
            for c in self.key_columns
        ]
        return " AND ".join(conditions)

    def execute_upsert(self, source_df: DataFrame):
        print("Starting Delta upsert process...")

        delta_table = DeltaTable.forPath(
            self.spark,
            self.target_table_path
        )

        merge_condition = self._build_merge_condition()

        (
            delta_table.alias("target")
            .merge(
                source_df.alias("source"),
                merge_condition
            )
            .whenMatchedUpdate(
                condition=f"target.{self.hash_column} <> source.{self.hash_column}",
                set={col_name: f"source.{col_name}" for col_name in source_df.columns}
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("Delta upsert completed successfully.")
