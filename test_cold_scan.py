import os
import sys
import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

# Add project path
sys.path.insert(0, r"H:\Papers\Metadata Tool")

from DLDSchema import CandidateStatus
from ColdScanEngine import ParquetColdScanner, MySQLColdScanner, CandidateGraphBuilder
from discover_relationships import discover_relates_to
from FastAPI_Service import app

client = TestClient(app)


def test_parquet_cold_scanner_in_place(tmp_path):
    """Verify that ParquetColdScanner extracts metadata footers in-place without table scans."""
    hosp_dir = tmp_path / "hospital"
    icu_dir = tmp_path / "icu"
    hosp_dir.mkdir()
    icu_dir.mkdir()

    # 1. Create admissions.parquet
    df_adm = pd.DataFrame({
        "subject_id": [101, 102, 103, 104],
        "hadm_id": [2001, 2002, 2003, 2004],
        "admittime": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"],
        "insurance": ["Medicare", "Private", None, "Medicaid"]
    })
    adm_path = str(hosp_dir / "admissions.parquet")
    pq.write_table(pa.Table.from_pandas(df_adm), adm_path, compression="snappy")

    # 2. Create icustays.parquet
    df_icu = pd.DataFrame({
        "stay_id": [301, 302, 303],
        "hadm_id": [2001, 2002, 2004],
        "subject_id": [101, 102, 104],
        "los_days": [2.5, 4.1, 1.2]
    })
    icu_path = str(icu_dir / "icustays.parquet")
    pq.write_table(pa.Table.from_pandas(df_icu), icu_path, compression="snappy")

    # Scan the root temp directory
    scanner = ParquetColdScanner(root_dir=str(tmp_path))
    res = scanner.scan()

    assert len(res["datasets"]) == 2
    assert len(res["data_objects"]) == 2

    # Check admissions object
    adm_obj = next(o for o in res["data_objects"] if "admissions" in o["name"])
    assert adm_obj["OVolume"]["logical_record_count"] == 4
    assert adm_obj["OVolume"]["column_count"] == 4
    assert adm_obj["OVolume"]["physical_size_bytes"] > 0
    assert "hadm_id" in adm_obj["OVariety"]["columns"]
    assert adm_obj["OVariety"]["compression_algorithm"] == "SNAPPY"


def test_discover_relationships_candidate_suggestion(tmp_path):
    """Verify that heuristic discovery flags shared identifiers as candidate suggested joins."""
    datasets = [
        {
            "id": "ds_hospital",
            "name": "Hospital Admissions",
            "data_objects": [
                {
                    "name": "admissions.parquet",
                    "OVariety": {
                        "columns": {"subject_id": "int64", "hadm_id": "int64", "admittime": "timestamp"}
                    }
                }
            ]
        },
        {
            "id": "ds_icu",
            "name": "ICU Stays",
            "data_objects": [
                {
                    "name": "icustays.parquet",
                    "OVariety": {
                        "columns": {"stay_id": "int64", "hadm_id": "int64", "subject_id": "int64", "charttime": "timestamp"}
                    }
                }
            ]
        }
    ]

    rels = discover_relates_to(datasets)
    assert len(rels) >= 1

    ref_rel = next((r for r in rels if r["relationship_type"] == "Referential"), None)
    assert ref_rel is not None
    assert ref_rel["status"] == "Suggested"
    assert ref_rel["confidence"] == 0.95
    assert "hadm_id" in ref_rel["attributes"]["join_keys"]
    assert "subject_id" in ref_rel["attributes"]["join_keys"]


def test_mysql_cold_scanner_with_mock_schema():
    """Verify that MySQLColdScanner parses INFORMATION_SCHEMA catalogs with 100% confidence foreign keys."""
    mock_data = {
        "tables": [
            {"TABLE_NAME": "patients", "TABLE_ROWS": 50000, "DATA_LENGTH": 2048000, "INDEX_LENGTH": 512000},
            {"TABLE_NAME": "admissions", "TABLE_ROWS": 120000, "DATA_LENGTH": 8192000, "INDEX_LENGTH": 2048000},
        ],
        "columns": [
            {"TABLE_NAME": "patients", "COLUMN_NAME": "subject_id", "DATA_TYPE": "int"},
            {"TABLE_NAME": "patients", "COLUMN_NAME": "gender", "DATA_TYPE": "varchar"},
            {"TABLE_NAME": "admissions", "COLUMN_NAME": "hadm_id", "DATA_TYPE": "int"},
            {"TABLE_NAME": "admissions", "COLUMN_NAME": "subject_id", "DATA_TYPE": "int"},
            {"TABLE_NAME": "admissions", "COLUMN_NAME": "admittime", "DATA_TYPE": "datetime"},
        ],
        "foreign_keys": [
            {
                "TABLE_NAME": "admissions",
                "COLUMN_NAME": "subject_id",
                "REFERENCED_TABLE_NAME": "patients",
                "REFERENCED_COLUMN_NAME": "subject_id",
            }
        ]
    }

    scanner = MySQLColdScanner(database="mimic_hospital", mock_schema=mock_data)
    scan_res = scanner.scan()

    assert len(scan_res["datasets"]) == 2
    assert len(scan_res["relationships"]) == 1

    fk_rel = scan_res["relationships"][0]
    assert fk_rel["relationship_type"] == "Referential"
    assert fk_rel["status"] == "Confirmed"
    assert fk_rel["confidence"] == 1.0
    assert fk_rel["provenance"] == "mysql_information_schema_fk"
    assert fk_rel["source_dataset_id"] == "ds_admissions"
    assert fk_rel["target_dataset_id"] == "ds_patients"


def test_candidate_graph_builder_and_root_assembly():
    """Verify CandidateGraphBuilder wraps simple datasets under root assembly if specified."""
    datasets = [
        {"id": "ds_hosp", "name": "Hospital", "data_objects": []},
        {"id": "ds_icu", "name": "ICU", "data_objects": []},
    ]
    builder = CandidateGraphBuilder()
    graph = builder.build_candidate_graph(datasets, root_assembly_name="MIMIC IV Root")

    manifest = graph["manifest"]
    assert len(manifest["datasets"]) == 3
    root = manifest["datasets"][0]
    assert root["id"] == "ds_mimic_iv_root"
    assert root["structure_type"] == "Complex"
    assert "ds_hosp" in root["assembly_children"]
    assert "ds_icu" in root["assembly_children"]


def test_fastapi_cold_scan_endpoints(tmp_path):
    """Verify FastAPI cold scan and edge confirmation endpoints."""
    # 1. Create a dummy parquet file
    test_dir = tmp_path / "telemetry"
    test_dir.mkdir()
    df = pd.DataFrame({"device_id": [1, 2], "temperature": [98.6, 99.1]})
    pq.write_table(pa.Table.from_pandas(df), str(test_dir / "devices.parquet"))

    # Test /pilot/cold-scan/parquet
    res = client.post("/pilot/cold-scan/parquet", json={
        "directory_path": str(tmp_path),
        "root_assembly_name": "Telemetry Lake"
    })
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "Candidate DLD Graph Generated"
    assert data["summary"]["total_datasets"] >= 1

    # Test /pilot/candidate/confirm-edge
    confirm_res = client.post("/pilot/candidate/confirm-edge", json={
        "source_dataset_id": "ds_devices",
        "target_dataset_id": "ds_telemetry",
        "action": "confirm"
    })
    assert confirm_res.status_code == 200
    confirm_data = confirm_res.json()
    assert confirm_data["relationship"]["status"] == "Confirmed"

    reject_res = client.post("/pilot/candidate/confirm-edge", json={
        "source_dataset_id": "ds_devices",
        "target_dataset_id": "ds_telemetry",
        "action": "reject"
    })
    assert reject_res.status_code == 200
    assert reject_res.json()["relationship"]["status"] == "Rejected"
