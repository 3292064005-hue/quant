from a_share_quant.core.schema_loader import load_schema_sql


def test_schema_loader_reads_packaged_schema() -> None:
    schema_sql = load_schema_sql()
    assert "CREATE TABLE IF NOT EXISTS securities" in schema_sql
    assert "schema_version" in schema_sql
