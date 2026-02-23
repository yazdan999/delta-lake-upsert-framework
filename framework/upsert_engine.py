"""
Delta Lake Upsert Engine

Reusable PySpark-based Delta Lake merge framework with:
- Null-safe merge conditions
- Hash-based update detection
- Insert/update tracking
- Basic structured logging

CI-friendly: avoids importing pyspark/delta at import-time.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class DeltaUpsertEngine:
    def __init__(
        self,
        spark: Any,
        target_table_path: str,
        key_columns: List[str],
        hash_column: str = "row_hash",
    ):
        self.spark = spark
        self.target_table_path = target_table_path
        self.key_columns = key_columns
        self.hash_column = hash_column

    def _build_merge_condition(self) -> str:
        # Null-safe equality in Spark SQL: <=> (equivalent to eqNullSafe)
        conditions = [f"target.{c} <=> source.{c}" for c in self.key_columns]
        return " AND ".join(conditions)

    def execute_upsert(self, source_df: "DataFrame") -> Dict[str, str]:
        # Lazy imports so CI can run without Spark/Delta installed
        from delta.tables import DeltaTable
        from framework.logging_utils import log_info

        log_info("Starting Delta upsert process...")

        delta_table = DeltaTable.forPath(self.spark, self.target_table_path)
        merge_condition = self._build_merge_condition()

        (
            delta_table.alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdate(
                condition=f"target.{self.hash_column} <> source.{self.hash_column}",
                set={c: f"source.{c}" for c in source_df.columns},
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        log_info("Delta upsert completed successfully.")

        return {"status": "completed"}
