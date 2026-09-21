import os
import shutil
from datetime import datetime, timezone, timedelta
import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from DLDSchema import (
    StructureType,
    LifecycleStage,
    RelationKind,
    VulnerabilityClassification,
    BusinessCriticality,
    DataSetInput,
    DataObjectInput,
    StakeholderInput,
    DLDManifest,
    VolumeIndicator,
    VelocityIndicator,
    VarietyIndicator,
    VeracityIndicator,
    VariabilityIndicator,
    ValueIndicator,
    VulnerabilityIndicator,
    TMDObject,
    OMDObject,
)
from IngestionController import DLDSetupEngine, MetadataRepository, PilotStage
from ExtractionHelpers import profile_csv_file, profile_parquet_file, auto_bind_data_object
from MetadataServices import (
    ConstraintEngine,
    LineageAndRecEngine,
    StalenessAndEvolutionMonitor,
    ValidationSeverity,
)


@pytest.fixture
def sample_files(tmp_path):
    # CSV file
    csv_path = str(tmp_path / "test_data.csv")
    df = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "name": ["Alpha", "Beta", "Gamma", None],
        "val": [10.5, 20.0, None, 40.2],
    })
    df.to_csv(csv_path, index=False)

    # Parquet file
    pq_path = str(tmp_path / "test_data.parquet")
    table = pa.Table.from_pandas(df.dropna())
    pq.write_table(table, pq_path, compression="SNAPPY")

    return {"csv": csv_path, "parquet": pq_path}


# =====================================================================
# STEP 1 TESTS: Data Models & Pydantic Validation Invariants
# =====================================================================

def test_dld_schema_simple_cannot_have_assembly():
    with pytest.raises(ValueError, match="Simple dataset ds_1 cannot have assembly children"):
        DataSetInput(
            id="ds_1",
            name="Simple Test",
            structure_type=StructureType.SIMPLE,
            lifecycle_stage=LifecycleStage.RAW,
            assembly_children=["ds_2"],
        )


def test_dld_schema_raw_cannot_have_processed_from():
    with pytest.raises(ValueError, match="Raw dataset ds_raw cannot have 'processed_from'"):
        DataSetInput(
            id="ds_raw",
            name="Raw Test",
            structure_type=StructureType.SIMPLE,
            lifecycle_stage=LifecycleStage.RAW,
            processed_from=["ds_source"],
        )


def test_dld_schema_processed_must_have_processed_from():
    with pytest.raises(ValueError, match="must define at least one 'processed_from' parent"):
        DataSetInput(
            id="ds_proc",
            name="Proc Test",
            structure_type=StructureType.SIMPLE,
            lifecycle_stage=LifecycleStage.PROCESSED,
            processed_from=[],
        )


def test_tmd_object_enforces_value_on_processed():
    vol = VolumeIndicator(byte_size=100)
    vel = VelocityIndicator(expected_refresh_interval_sec=300)
    var = VarietyIndicator()
    ver = VeracityIndicator(source_origin="Source A")
    vbl = VariabilityIndicator()

    # Should raise error when value is None on PROCESSED dataset
    with pytest.raises(ValueError, match="must have a ValueIndicator"):
        TMDObject(
            dataset_id="ds_p1",
            dataset_name="P1",
            structure_type=StructureType.SIMPLE,
            lifecycle_stage=LifecycleStage.PROCESSED,
            volume=vol,
            velocity=vel,
            variety=var,
            veracity=ver,
            variability=vbl,
            value=None,
        )


# =====================================================================
# STEP 2 TESTS: Metadata Pilot (Workflow Engine Stages A, B, C)
# =====================================================================

def test_pilot_stage_a_cycle_detection():
    engine = DLDSetupEngine()
    # Cyclic manifest: A -> B -> A
    cyclic_manifest = {
        "stakeholders": [{"id": "s1", "name": "Steward", "role": "Steward"}],
        "datasets": [
            {
                "id": "ds_a",
                "name": "Dataset A",
                "structure_type": "Simple",
                "lifecycle_stage": "Processed",
                "interacts_with": ["s1"],
                "processed_from": ["ds_b"],
            },
            {
                "id": "ds_b",
                "name": "Dataset B",
                "structure_type": "Simple",
                "lifecycle_stage": "Processed",
                "interacts_with": ["s1"],
                "processed_from": ["ds_a"],
            },
        ],
        "relationships": [],
    }
    with pytest.raises(ValueError, match="Lineage Derivation Cycle detected in ProcessedFrom graph"):
        engine.ingest_manifest(cyclic_manifest)


def test_pilot_stage_b_and_c_execution():
    repo = MetadataRepository()
    engine = DLDSetupEngine(repository=repo)

    manifest = {
        "stakeholders": [{"id": "s1", "name": "Alice", "role": "Curator"}],
        "datasets": [
            {
                "id": "ds_sub1",
                "name": "Sub Dataset 1",
                "structure_type": "Simple",
                "lifecycle_stage": "Raw",
                "interacts_with": ["s1"],
                "data_objects": [{"id": "obj1", "name": "f1.csv", "physical_type": "CSV"}],
            },
            {
                "id": "ds_sub2",
                "name": "Sub Dataset 2",
                "structure_type": "Simple",
                "lifecycle_stage": "Raw",
                "interacts_with": ["s1"],
                "data_objects": [{"id": "obj2", "name": "f2.csv", "physical_type": "CSV"}],
            },
            {
                "id": "ds_comp",
                "name": "Complex Asset",
                "structure_type": "Complex",
                "lifecycle_stage": "Raw",
                "interacts_with": ["s1"],
                "assembly_children": ["ds_sub1", "ds_sub2"],
            },
        ],
        "relationships": [],
    }

    res_a = engine.ingest_manifest(manifest)
    assert res_a["ready_for_stage_b"] is True

    # Bind objects
    engine.bind_data_object_ov_words(
        "obj1", "ds_sub1",
        VolumeIndicator(byte_size=1000, record_count=50),
        VarietyIndicator(column_count=4, schema_definition={"c1": "int"}),
        VariabilityIndicator(),
    )
    engine.bind_data_object_ov_words(
        "obj2", "ds_sub2",
        VolumeIndicator(byte_size=2500, record_count=150),
        VarietyIndicator(column_count=5, schema_definition={"c2": "str"}),
        VariabilityIndicator(),
    )

    # Bind datasets
    for ds_id in ["ds_sub1", "ds_sub2", "ds_comp"]:
        engine.bind_dataset_tv_words(
            dataset_id=ds_id,
            volume=VolumeIndicator(byte_size=0, record_count=0),
            velocity=VelocityIndicator(expected_refresh_interval_sec=3600),
            variety=VarietyIndicator(),
            veracity=VeracityIndicator(source_origin="System Source"),
            variability=VariabilityIndicator(),
        )

    # Bind interactions
    for ds_id in ["ds_sub1", "ds_sub2", "ds_comp"]:
        engine.bind_interacts_with_vulnerability(
            "s1", ds_id,
            VulnerabilityIndicator(access_control_policy="Policy: Standard Curator Read")
        )

    res_b = engine.validate_stage_b_completeness()
    assert res_b["ready_for_stage_c"] is True

    res_c = engine.register_catalog()
    assert res_c["current_stage"] == PilotStage.REGISTERED.value

    # Check that Complex dataset aggregated child sizes
    tmd_comp = repo.tmd_objects["ds_comp"]
    assert tmd_comp.volume.byte_size == 3500  # 1000 + 2500
    assert tmd_comp.volume.record_count == 200  # 50 + 150
    assert "c1" in tmd_comp.variety.schema_definition
    assert "c2" in tmd_comp.variety.schema_definition


# =====================================================================
# STEP 3 TESTS: Extraction & Profiling Helpers
# =====================================================================

def test_csv_and_parquet_extractors(sample_files):
    csv_profile = profile_csv_file(sample_files["csv"])
    assert csv_profile["column_count"] == 3
    assert csv_profile["null_counts"]["name"] == 1
    assert csv_profile["null_counts"]["val"] == 1

    pq_profile = profile_parquet_file(sample_files["parquet"])
    assert pq_profile["column_count"] == 3
    assert pq_profile["compression_algorithm"] == "SNAPPY"

    omd = auto_bind_data_object("obj_test", "ds_test", sample_files["parquet"])
    assert isinstance(omd, OMDObject)
    assert omd.physical_type == "Parquet"
    assert omd.variety.compression_algorithm == "SNAPPY"


# =====================================================================
# STEP 4 TESTS: Metadata Services (Constraint, Lineage, Staleness)
# =====================================================================

def test_constraint_engine_flags_missing_source():
    repo = MetadataRepository()
    engine = DLDSetupEngine(repository=repo)
    manifest = {
        "stakeholders": [{"id": "s1", "name": "Alice", "role": "Curator"}],
        "datasets": [{
            "id": "ds_1", "name": "DS 1", "structure_type": "Simple",
            "lifecycle_stage": "Raw", "interacts_with": ["s1"]
        }],
        "relationships": [],
    }
    engine.ingest_manifest(manifest)
    engine.bind_dataset_tv_words(
        "ds_1",
        VolumeIndicator(byte_size=10),
        VelocityIndicator(expected_refresh_interval_sec=60),
        VarietyIndicator(),
        VeracityIndicator(source_origin="   "),  # Empty source origin!
        VariabilityIndicator(),
    )
    engine.bind_interacts_with_vulnerability("s1", "ds_1", VulnerabilityIndicator(access_control_policy="policy"))
    engine.register_catalog()

    checker = ConstraintEngine(repo)
    alerts = checker.run_all_checks()
    assert any(a.rule_name == "MissingVeracitySourceOrigin" for a in alerts)


def test_staleness_and_drift_monitor():
    repo = MetadataRepository()
    engine = DLDSetupEngine(repository=repo)
    manifest = {
        "stakeholders": [{"id": "s1", "name": "Alice", "role": "Curator"}],
        "datasets": [{
            "id": "ds_fresh", "name": "Fresh DS", "structure_type": "Simple",
            "lifecycle_stage": "Raw", "interacts_with": ["s1"]
        }],
        "relationships": [],
    }
    engine.ingest_manifest(manifest)

    # Set last refreshed 1 hour ago with TTL of 300 seconds (stale!)
    old_time = datetime.now(timezone.utc) - timedelta(seconds=3600)
    engine.bind_dataset_tv_words(
        "ds_fresh",
        VolumeIndicator(byte_size=50),
        VelocityIndicator(expected_refresh_interval_sec=300, last_refreshed_at=old_time),
        VarietyIndicator(schema_definition={"col_a": "int", "col_b": "str"}),
        VeracityIndicator(source_origin="ETL"),
        VariabilityIndicator(),
    )
    engine.bind_interacts_with_vulnerability("s1", "ds_fresh", VulnerabilityIndicator(access_control_policy="policy"))
    engine.register_catalog()

    monitor = StalenessAndEvolutionMonitor(repo)
    stale_list = monitor.check_staleness()
    assert len(stale_list) == 1
    assert stale_list[0]["dataset_id"] == "ds_fresh"
    assert stale_list[0]["overdue_sec"] > 3000

    # Test schema drift
    drift = monitor.detect_schema_drift("ds_fresh", {"col_a": "int", "col_b": "str", "col_c": "float"})
    assert drift["drift_detected"] is True
    assert "col_c" in drift["added_columns"]
