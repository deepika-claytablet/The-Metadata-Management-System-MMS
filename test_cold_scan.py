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


def test_cloud_storage_uri_cold_scan():
    """Verify that ParquetColdScanner handles cloud object store URIs (s3://, gs://, hdfs://)."""
    s3_scanner = ParquetColdScanner("s3://mimic4-lakehouse/hosp/")
    res = s3_scanner.scan()
    assert len(res["datasets"]) == 3
    assert len(res["data_objects"]) == 3
    adm_obj = next(o for o in res["data_objects"] if "admissions" in o["name"])
    assert adm_obj["OVolume"]["logical_record_count"] == 124500
    assert "hadm_id" in adm_obj["OVariety"]["columns"]


def test_mysql_uri_and_simulated_scan():
    """Verify MySQL URI parsing and simulated hospital schema cold scan."""
    from ColdScanEngine import parse_mysql_uri
    parsed = parse_mysql_uri("mysql://admin:secret@lakehouse-db:3306/mimic_clinical")
    assert parsed["host"] == "lakehouse-db"
    assert parsed["port"] == 3306
    assert parsed["user"] == "admin"
    assert parsed["password"] == "secret"
    assert parsed["database"] == "mimic_clinical"

    # Test via FastAPI endpoint with simulated mock
    res = client.post("/pilot/cold-scan/mysql", json={
        "connection_uri": "mysql://root@localhost:3306/mimic_clinical",
        "use_mock": True,
        "root_assembly_name": "Hospital MySQL Lakehouse"
    })
    assert res.status_code == 200
    data = res.json()
    assert data["summary"]["total_datasets"] == 5
    assert len(data["manifest"]["relationships"]) >= 3


def test_mysql_test_connection_endpoint():
    """Verify the /pilot/cold-scan/test-connection endpoint with mock and failure cases."""
    # 1. With mock enabled
    res = client.post("/pilot/cold-scan/test-connection", json={
        "database": "banking",
        "use_mock": True
    })
    assert res.status_code == 200
    assert res.json()["status"] == "success"
    assert res.json()["mock"] is True

    # 2. Live connection with invalid password generates clear Error 1045 or connection error
    fail_res = client.post("/pilot/cold-scan/test-connection", json={
        "host": "localhost",
        "port": 3306,
        "user": "invalid_user_xyz",
        "password": "wrong_password_123",
        "database": "banking",
        "use_mock": False
    })
    assert fail_res.status_code == 400
    assert "detail" in fail_res.json()


def test_multi_database_cold_scan_and_lineage():
    """Verify multi-database scanning creates database assemblies and cross-database lineage."""
    from ColdScanEngine import DEFAULT_MOCK_MULTI_DB_SCHEMA

    scanner = MySQLColdScanner(mock_schema=DEFAULT_MOCK_MULTI_DB_SCHEMA)
    res = scanner.scan()

    # 6 base tables + 2 database complex containers = 8 datasets
    assert len(res["datasets"]) == 8

    # Verify database containers exist
    db_containers = [d for d in res["datasets"] if d["structure_type"] == "Complex"]
    assert len(db_containers) == 2
    assert any(d["id"] == "ds_db_banking" for d in db_containers)
    assert any(d["id"] == "ds_db_sale" for d in db_containers)

    # Verify namespace isolation on table IDs
    assert any(d["id"] == "ds_banking_customers" for d in res["datasets"])
    assert any(d["id"] == "ds_sale_dim_product" for d in res["datasets"])

    # Build candidate graph
    builder = CandidateGraphBuilder()
    graph = builder.build_candidate_graph(res["datasets"], res["relationships"])

    # Verify root lakehouse assembles the two database containers
    manifest = graph["manifest"]
    root = manifest["datasets"][0]
    assert "ds_db_banking" in root["assembly_children"]
    assert "ds_db_sale" in root["assembly_children"]

    # Verify explicit cross-database foreign key from sale to banking
    cross_fk = next(
        (r for r in manifest["relationships"] if r["source_dataset_id"] == "ds_sale_sale" and r["target_dataset_id"] == "ds_banking_customers"),
        None
    )
    assert cross_fk is not None
    assert cross_fk["confidence"] == 1.0


def test_get_mysql_databases_endpoint():
    """Verify /pilot/cold-scan/mysql/databases endpoint returns list of schemas."""
    res = client.get("/pilot/cold-scan/mysql/databases")
    assert res.status_code == 200
    data = res.json()
    assert "databases" in data
    assert isinstance(data["databases"], list)
    assert len(data["databases"]) > 0


