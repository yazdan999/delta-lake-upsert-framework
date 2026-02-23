"""
Hash utilities for change detection.

This module provides helpers to generate a deterministic row-level hash
from a list of columns for Delta upsert change detection.
"""

from __future__ import annotations

from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from pyspark.sql.functions import col, concat_ws, sha2


def add_row_hash(df: "DataFrame", columns: List[str], hash_col: str = "row_hash") -> "DataFrame":
    """
    Adds a SHA-256 row hash column computed from the given list of columns.

    Args:
        df: Input DataFrame
        columns: Column names used to generate the hash
        hash_col: Output hash column name

    Returns:
        DataFrame with an additional hash column
    """
    expr = concat_ws("||", *[col(c).cast("string") for c in columns])
    return df.withColumn(hash_col, sha2(expr, 256))
