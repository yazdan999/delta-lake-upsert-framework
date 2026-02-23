"""
Row-level hashing utility for change detection.
"""

from pyspark.sql.functions import sha2, concat_ws


def add_row_hash(df, columns: list, hash_column: str = "row_hash"):
    return df.withColumn(
        hash_column,
        sha2(concat_ws("||", *columns), 256)
    )
