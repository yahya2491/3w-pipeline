from datetime import datetime
from hashlib import sha256

from threew.silver.input import _silver_rows_from_manifest_records


def test_manifest_rows_keep_current_schema_approved_files_and_calculate_missing_hash(tmp_path):
    content = b"small source file"
    (tmp_path / "landing").mkdir()
    (tmp_path / "landing" / "source.parquet").write_bytes(content)
    records = [
        {
            "schema_ok": True,
            "path": "data/1/WELL-00001.parquet",
            "sha256": None,
            "landing_path": "landing/source.parquet",
            "source_type": "REAL",
            "label": 1,
            "instance_id": "WELL-00001",
            "well_id": 1,
            "instance_start": datetime.fromisoformat("2020-01-01T00:00:00"),
        },
        {
            "schema_ok": False,
            "path": "data/2/ignored.parquet",
            "sha256": "ignored",
            "landing_path": "landing/source.parquet",
            "source_type": "REAL",
            "label": 2,
            "instance_id": "ignored",
            "well_id": 1,
            "instance_start": None,
        },
    ]

    rows = _silver_rows_from_manifest_records(records, tmp_path)

    assert rows == [
        {
            "_source_path": "data/1/WELL-00001.parquet",
            "_file_sha256": sha256(content).hexdigest(),
            "_manifest_source_type": "REAL",
            "_manifest_label": 1,
            "instance_id": "WELL-00001",
            "well_id": 1,
            "instance_start": datetime.fromisoformat("2020-01-01T00:00:00"),
        }
    ]
