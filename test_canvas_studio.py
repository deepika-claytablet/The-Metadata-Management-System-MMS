import os
import sys
import pytest
from fastapi.testclient import TestClient
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Add project root to sys.path
sys.path.insert(0, r"H:\Papers\Metadata Tool")

from FastAPI_Service import app
from DLDSchema import DLDManifest

client = TestClient(app)


def test_get_canvas_studio_html():
    """Verify that GET /canvas serves the interactive studio HTML."""
    response = client.get("/canvas")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "MeDOM Studio" in response.text
    assert "cytoscape" in response.text
    assert "Entity Palette" in response.text


def test_get_mimic_sample_manifest():
    """Verify that GET /pilot/sample/mimic returns the complete multimodal lakehouse manifest."""
    response = client.get("/pilot/sample/mimic")
    assert response.status_code == 200
    data = response.json()
    assert "stakeholders" in data
    assert "datasets" in data
    assert "relationships" in data
    assert len(data["datasets"]) >= 10
    # Check that root complex lakehouse is present
    root_ds = next((ds for ds in data["datasets"] if ds["id"] == "ds_mimic_root"), None)
    assert root_ds is not None
    assert root_ds["structure_type"] == "Complex"


def test_profile_file_endpoints(tmp_path):
    """Verify that POST /pilot/profile-file extracts metadata without table scan."""
    # 1. Create a dummy parquet file
    df = pd.DataFrame({
        "subject_id": [1001, 1002, 1003],
        "hadm_id": [2001, 2002, 2003],
        "icd_code": ["I10", "E11", None],
    })
    parquet_path = str(tmp_path / "test_profiler.parquet")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_path, compression="snappy")

    # Call endpoint
    res = client.post("/pilot/profile-file", json={"filepath": parquet_path})
    assert res.status_code == 200
    res_data = res.json()
    assert res_data["status"] == "success"
    stats = res_data["stats"]
    assert stats["record_count"] == 3
    assert stats["column_count"] == 3
    assert stats["file_format"] == "Parquet"
    assert stats["compression_algorithm"] == "SNAPPY"

    # 2. Test non-existent file
    bad_res = client.post("/pilot/profile-file", json={"filepath": "C:\\non_existent_file.parquet"})
    assert bad_res.status_code == 404


def test_canvas_validate_and_commit():
    """Verify that POST /pilot/canvas/validate detects DAG integrity and cycles."""
    # 1. Valid Minimal Manifest
    valid_payload = {
        "stakeholders": [
            {
                "id": "stk_steward",
                "name": "Data Steward",
                "role": "Steward",
                "email": "steward@org.com",
                "department": "Governance"
            }
        ],
        "datasets": [
            {
                "id": "ds_raw",
                "name": "Raw Ingestion",
                "structure_type": "Simple",
                "lifecycle_stage": "Raw",
                "interacts_with": ["stk_steward"],
                "data_objects": []
            },
            {
                "id": "ds_clean",
                "name": "Cleaned Table",
                "structure_type": "Simple",
                "lifecycle_stage": "Processed",
                "interacts_with": ["stk_steward"],
                "processed_from": ["ds_raw"],
                "data_objects": []
            }
        ],
        "relationships": []
    }

    val_res = client.post("/pilot/canvas/validate", json=valid_payload)
    assert val_res.status_code == 200
    val_data = val_res.json()
    assert val_data["valid"] is True
    assert "cycle-free" in val_data["status"]

    # 2. Commit valid graph to catalog
    commit_res = client.post("/pilot/canvas/commit", json=valid_payload)
    assert commit_res.status_code == 200
    commit_data = commit_res.json()
    assert commit_data["status"] == "Committed and Registered"

    # 3. Cyclic Manifest (ProcessedFrom cycle: A -> B -> A)
    cyclic_payload = {
        "stakeholders": [
            {
                "id": "stk_steward",
                "name": "Data Steward",
                "role": "Steward",
                "email": "steward@org.com",
                "department": "Governance"
            }
        ],
        "datasets": [
            {
                "id": "ds_cycle_1",
                "name": "Cycle Node 1",
                "structure_type": "Simple",
                "lifecycle_stage": "Processed",
                "interacts_with": ["stk_steward"],
                "processed_from": ["ds_cycle_2"],
                "data_objects": []
            },
            {
                "id": "ds_cycle_2",
                "name": "Cycle Node 2",
                "structure_type": "Simple",
                "lifecycle_stage": "Processed",
                "interacts_with": ["stk_steward"],
                "processed_from": ["ds_cycle_1"],
                "data_objects": []
            }
        ],
        "relationships": []
    }

    cyc_res = client.post("/pilot/canvas/validate", json=cyclic_payload)
    assert cyc_res.status_code == 200
    cyc_data = cyc_res.json()
    assert cyc_data["valid"] is False
    assert "Cycle detected" in cyc_data["error"]


def test_medom_3tier_manifest_endpoint():
    """
    Verifies that the /pilot/medom/3tier-manifest endpoint returns the exact 3-tier MeDOM
    representation: TMD Object (Dataset TV-words), OMD Objects (OV-words), and RMD Objects (RV-words).
    """
    res = client.get("/pilot/medom/3tier-manifest")
    assert res.status_code == 200
    data = res.json()

    # Tier 1: TMD Object
    assert "tmd_object" in data
    tmd = data["tmd_object"]
    assert "target_dataset" in tmd
    assert "attributes" in tmd
    attrs = tmd["attributes"]
    assert "TVolume_physical_bytes" in attrs
    assert "TVolume_logical_records" in attrs
    assert "TVelocity_speed" in attrs
    assert "TVariety_nature" in attrs
    assert "TVariety_ttl" in attrs
    assert "TVeracity_source" in attrs
    assert "timestamp" in attrs

    # Tier 2: OMD Objects
    assert "omd_objects" in data
    assert isinstance(data["omd_objects"], list)
    if data["omd_objects"]:
        omd = data["omd_objects"][0]
        assert "target_data_object" in omd
        assert "attributes" in omd
        omd_attrs = omd["attributes"]
        assert "OVolume_byte_size" in omd_attrs
        assert "OVolume_row_count" in omd_attrs
        assert "OVariety_physical_rep" in omd_attrs
        assert "OVariety_schema" in omd_attrs
        assert "OVariability_last_modified" in omd_attrs

    # Tier 3: RMD Objects
    assert "rmd_objects" in data
    assert isinstance(data["rmd_objects"], list)
    if data["rmd_objects"]:
        rmd = data["rmd_objects"][0]
        assert "edge_type" in rmd
        assert "attributes" in rmd


def test_flattened_catalog_records_endpoint():
    """
    Verifies that the /catalog/flattened-records endpoint returns catalog entities
    flattened into the properties dictionary format for persistent database/index storage.
    """
    res = client.get("/catalog/flattened-records")
    assert res.status_code == 200
    data = res.json()
    assert "datasets" in data
    assert "data_objects" in data
    assert "relationships" in data

    if data["datasets"]:
        ds = data["datasets"][0]
        assert "properties" in ds
        assert isinstance(ds["properties"], dict)

    if data["data_objects"]:
        obj = data["data_objects"][0]
        assert "properties" in obj
        assert isinstance(obj["properties"], dict)

    if data["relationships"]:
        rel = data["relationships"][0]
        assert "properties" in rel
        assert isinstance(rel["properties"], dict)

