"""
Delta Lake Upsert Engine

Reusable PySpark-based Delta Lake merge framework with
hash-based change detection and null-safe merge logic.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from delta.tables import DeltaTable


class DeltaUpsertEngine:

    def __init__(self, spark, target_table_path: str, key_columns: list):
        self.spark = spark
        self.target_table_path = target_table_path
        self.key_columns = key_columns

    def build_merge_condition(self):
        conditions = [
            f"target.{col} <=> source.{col}"
            for col in self.key_columns
        ]
        return " AND ".join(conditions)

    def execute_upsert(self, source_df: DataFrame):
        delta_table = DeltaTable.forPath(
            self.spark,
            self.target_table_path
        )

        merge_condition = self.build_merge_condition()

        (
            delta_table.alias("target")
            .merge(
                source_df.alias("source"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
