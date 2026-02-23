"""
Basic validation tests for DeltaUpsertEngine.
"""

from framework.upsert_engine import DeltaUpsertEngine


def test_engine_initialisation():
    engine = DeltaUpsertEngine(
        spark=None,
        target_table_path="dummy_path",
        key_columns=["id"],
    )

    assert engine.key_columns == ["id"]
    assert engine.hash_column == "row_hash"
