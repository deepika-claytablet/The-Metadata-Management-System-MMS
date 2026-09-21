import sys
import os
import shutil
from datetime import datetime, timezone, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console
from rich.panel import Panel

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

console = Console(legacy_windows=False)

from DLDSchema import (
    StructureType,
    LifecycleStage,
    RelationKind,
    VulnerabilityClassification,
    BusinessCriticality,
    StakeholderInput,
    DataObjectInput,
    DataSetInput,
    DatasetRelationshipInput,
    DLDManifest,
    VolumeIndicator,
    VelocityIndicator,
    VarietyIndicator,
    VeracityIndicator,
    VariabilityIndicator,
    ValueIndicator,
    VulnerabilityIndicator,
)
from IngestionController import DLDSetupEngine, MetadataRepository
from ExtractionHelpers import auto_bind_data_object
from MetadataServices import (
    ConstraintEngine,
    LineageAndRecEngine,
    StalenessAndEvolutionMonitor,
)
from CLI_Dashboard import render_full_dashboard


def create_sample_physical_files(output_dir: str):
    """
    Generates realistic CSV and Parquet files for automated profiling (Step 3).
    """
    os.makedirs(output_dir, exist_ok=True)

    # 1. Raw Sensors CSV (5,000 rows)
    sensors_csv = os.path.join(output_dir, "raw_sensors.csv")
    df_sensors = pd.DataFrame({
        "sensor_id": [f"SENS_{i:04d}" for i in range(5000)],
        "timestamp": pd.date_range("2026-09-01", periods=5000, freq="min"),
        "temperature": [20.0 + (i % 30) * 0.5 for i in range(5000)],
        "humidity": [40.0 + (i % 40) * 0.4 for i in range(5000)],
        "status_code": [0 if i % 100 != 0 else None for i in range(5000)],  # 50 nulls
    })
    df_sensors.to_csv(sensors_csv, index=False)

    # 2. Raw Device Registry Parquet (1,000 rows with SNAPPY compression)
    devices_parquet = os.path.join(output_dir, "raw_devices.parquet")
    df_devices = pd.DataFrame({
        "device_id": [f"DEV_{i:04d}" for i in range(1000)],
        "model": ["Model-X" if i % 2 == 0 else "Model-Y" for i in range(1000)],
        "firmware": ["v2.4.1" if i % 3 == 0 else "v2.5.0" for i in range(1000)],
        "region": [["US-East", "EU-West", "AP-South", "US-West"][i % 4] for i in range(1000)],
        "is_active": [True if i % 10 != 0 else False for i in range(1000)],
    })
    table_devices = pa.Table.from_pandas(df_devices)
    pq.write_table(table_devices, devices_parquet, compression="SNAPPY")

    # 3. Cleansed Sensors Parquet
    cleansed_parquet = os.path.join(output_dir, "cleansed_sensors.parquet")
    df_cleansed = df_sensors.dropna().copy()
    table_cleansed = pa.Table.from_pandas(df_cleansed)
    pq.write_table(table_cleansed, cleansed_parquet, compression="GZIP")

    return {
        "sensors_csv": sensors_csv,
        "devices_parquet": devices_parquet,
        "cleansed_parquet": cleansed_parquet,
    }


def run_complete_end_to_end_demonstration():
    console.print(Panel("[bold green]Starting MeDOM & DLD End-to-End Pipeline Demonstration[/bold green]", expand=False))

    work_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sample_data")
    files = create_sample_physical_files(work_dir)
    console.print(f"[dim]Generated test artifacts in {work_dir}[/dim]\n")

    # =====================================================================
    # STEP 1 & STEP 2: STAGE A (DLD Setup & Graph Ingestion)
    # =====================================================================
    console.print("[bold cyan]>>> Stage A: DLD Setup & Ingestion Engine[/bold cyan]")

    manifest_dict = {
        "stakeholders": [
            {"id": "stk_alice", "name": "Alice Curator", "role": "Data Curator", "email": "alice@lake.io"},
            {"id": "stk_bob", "name": "Bob Analyst", "role": "Data Scientist", "email": "bob@lake.io"},
            {"id": "stk_charlie", "name": "Charlie Engineer", "role": "Data Engineer", "email": "charlie@lake.io"},
        ],
        "datasets": [
            # 1. Raw Sensors (Simple, Raw)
            {
                "id": "ds_sensors_raw",
                "name": "Raw IoT Sensor Telemetry",
                "structure_type": "Simple",
                "lifecycle_stage": "Raw",
                "tags": ["iot", "telemetry", "raw", "sensors"],
                "interacts_with": ["stk_alice", "stk_charlie"],
                "data_objects": [
                    {"id": "obj_sensors_csv", "name": "sensors_stream.csv", "physical_type": "CSV", "storage_path": files["sensors_csv"]}
                ],
            },
            # 2. Raw Devices (Simple, Raw)
            {
                "id": "ds_devices_raw",
                "name": "Device Registry Metadata",
                "structure_type": "Simple",
                "lifecycle_stage": "Raw",
                "tags": ["hardware", "inventory", "devices"],
                "interacts_with": ["stk_charlie"],
                "data_objects": [
                    {"id": "obj_devices_parquet", "name": "device_inventory.parquet", "physical_type": "Parquet", "storage_path": files["devices_parquet"]}
                ],
            },
            # 3. Cleansed Sensors (Simple, Processed - ProcessedFrom ds_sensors_raw)
            {
                "id": "ds_sensors_clean",
                "name": "Cleansed Sensor Records",
                "structure_type": "Simple",
                "lifecycle_stage": "Processed",
                "tags": ["iot", "telemetry", "cleansed", "sensors"],
                "interacts_with": ["stk_alice", "stk_bob"],
                "processed_from": ["ds_sensors_raw"],
                "data_objects": [
                    {"id": "obj_sensors_parquet", "name": "sensors_cleansed.parquet", "physical_type": "Parquet", "storage_path": files["cleansed_parquet"]}
                ],
            },
            # 4. Aggregated Telemetry (Simple, Processed - ProcessedFrom ds_sensors_clean)
            {
                "id": "ds_sensors_daily_agg",
                "name": "Daily Sensor Analytics",
                "structure_type": "Simple",
                "lifecycle_stage": "Processed",
                "tags": ["iot", "daily_rollup", "gold"],
                "interacts_with": ["stk_bob"],
                "processed_from": ["ds_sensors_clean"],
                "data_objects": [
                    {"id": "obj_agg_parquet", "name": "daily_rollup.parquet", "physical_type": "Parquet", "storage_path": files["cleansed_parquet"]}
                ],
            },
            # 5. Complex IoT Lakehouse Asset (Complex, Processed - AssemblyOf ds_sensors_clean & ds_devices_raw)
            {
                "id": "ds_iot_lakehouse_asset",
                "name": "IoT Asset Intelligence Complex",
                "structure_type": "Complex",
                "lifecycle_stage": "Processed",
                "tags": ["lakehouse", "complex", "telemetry", "inventory"],
                "interacts_with": ["stk_bob", "stk_charlie"],
                "processed_from": ["ds_sensors_clean", "ds_devices_raw"],
                "assembly_children": ["ds_sensors_clean", "ds_devices_raw"],
                "data_objects": [],
            },
            # 6. Canonical Telemetry Abstract Model (Abstract, Raw - Generalization parent)
            {
                "id": "ds_abstract_telemetry",
                "name": "Canonical Telemetry Schema",
                "structure_type": "Abstract",
                "lifecycle_stage": "Raw",
                "tags": ["canonical", "abstract", "model"],
                "interacts_with": ["stk_alice"],
                "generalization_children": ["ds_sensors_raw"],
                "data_objects": [],
            },
        ],
        "relationships": [
            # Referential / Join relationship between sensors and devices
            {
                "relationship_type": "Referential",
                "source_dataset_id": "ds_sensors_clean",
                "target_dataset_id": "ds_devices_raw",
                "attributes": {"join_key": "device_id", "cardinality": "N:1"},
            },
            # Semantic correlation between raw and cleansed telemetry
            {
                "relationship_type": "Semantic",
                "source_dataset_id": "ds_sensors_raw",
                "target_dataset_id": "ds_sensors_clean",
                "attributes": {"semantic_distance": 0.05, "mapping": "identity"},
            },
        ],
    }

    repository = MetadataRepository()
    pilot = DLDSetupEngine(repository=repository)
    stage_a_res = pilot.ingest_manifest(manifest_dict)
    console.print(f"[green][OK] Stage A Complete:[/green] Registered {stage_a_res['registered_datasets']} datasets, {stage_a_res['graph_edges']} graph edges. DAG cycle check PASSED.\n")

    # =====================================================================
    # STEP 3 & STEP 2: STAGE B (Automated Profiling & Indicator Binding)
    # =====================================================================
    console.print("[bold cyan]>>> Stage B: Automated Extraction & V-Word Indicator Binding[/bold cyan]")

    # 1. Auto-extract and bind physical DataObjects (Step 3 helpers)
    omd_sensors_raw = auto_bind_data_object(
        data_object_id="obj_sensors_csv",
        dataset_id="ds_sensors_raw",
        filepath=files["sensors_csv"],
    )
    pilot.omd_objects[omd_sensors_raw.data_object_id] = omd_sensors_raw

    omd_devices_raw = auto_bind_data_object(
        data_object_id="obj_devices_parquet",
        dataset_id="ds_devices_raw",
        filepath=files["devices_parquet"],
    )
    pilot.omd_objects[omd_devices_raw.data_object_id] = omd_devices_raw

    omd_sensors_clean = auto_bind_data_object(
        data_object_id="obj_sensors_parquet",
        dataset_id="ds_sensors_clean",
        filepath=files["cleansed_parquet"],
    )
    pilot.omd_objects[omd_sensors_clean.data_object_id] = omd_sensors_clean

    omd_agg_parquet = auto_bind_data_object(
        data_object_id="obj_agg_parquet",
        dataset_id="ds_sensors_daily_agg",
        filepath=files["cleansed_parquet"],
    )
    pilot.omd_objects[omd_agg_parquet.data_object_id] = omd_agg_parquet

    console.print(f"  * Profiled & bound {len(pilot.omd_objects)} physical DataObjects (OVolume, OVariety, OVariability).")

    # 2. Bind TV-words to DataSets
    # Dataset 1: ds_sensors_raw
    pilot.bind_dataset_tv_words(
        dataset_id="ds_sensors_raw",
        volume=VolumeIndicator(byte_size=omd_sensors_raw.volume.byte_size, record_count=omd_sensors_raw.volume.record_count),
        velocity=VelocityIndicator(ingestion_mode="Stream", expected_refresh_interval_sec=300),
        variety=VarietyIndicator(schema_definition=omd_sensors_raw.variety.schema_definition, column_count=5, file_format="CSV"),
        veracity=VeracityIndicator(source_origin="IoT Gateway Sensor Hub v4", quality_score=0.98, null_rate=0.01),
        variability=VariabilityIndicator(schema_version="v1.0"),
        vulnerability=VulnerabilityIndicator(
            classification=VulnerabilityClassification.INTERNAL,
            access_control_policy="Policy: IoT Infrastructure Ingestion Role required",
            authorized_roles=["iot_curator", "data_engineer"],
        ),
    )

    # Dataset 2: ds_devices_raw
    pilot.bind_dataset_tv_words(
        dataset_id="ds_devices_raw",
        volume=VolumeIndicator(byte_size=omd_devices_raw.volume.byte_size, record_count=omd_devices_raw.volume.record_count),
        velocity=VelocityIndicator(ingestion_mode="Batch", expected_refresh_interval_sec=86400),
        variety=VarietyIndicator(schema_definition=omd_devices_raw.variety.schema_definition, column_count=5, file_format="Parquet", compression_algorithm="SNAPPY"),
        veracity=VeracityIndicator(source_origin="Enterprise Asset Management (SAP EAM)", quality_score=1.0),
        variability=VariabilityIndicator(schema_version="v1.0"),
        vulnerability=VulnerabilityIndicator(
            classification=VulnerabilityClassification.INTERNAL,
            access_control_policy="Policy: Hardware Operations Read Access",
            authorized_roles=["hardware_ops", "data_engineer"],
        ),
    )

    # Dataset 3: ds_sensors_clean (Processed -> requires TV-Value!)
    pilot.bind_dataset_tv_words(
        dataset_id="ds_sensors_clean",
        volume=VolumeIndicator(byte_size=omd_sensors_clean.volume.byte_size, record_count=omd_sensors_clean.volume.record_count),
        velocity=VelocityIndicator(ingestion_mode="Micro-batch", expected_refresh_interval_sec=900),
        variety=VarietyIndicator(schema_definition=omd_sensors_clean.variety.schema_definition, column_count=5, file_format="Parquet", compression_algorithm="GZIP"),
        veracity=VeracityIndicator(source_origin="Cleansing Pipeline Spark Job #104", quality_score=1.0),
        variability=VariabilityIndicator(schema_version="v1.0"),
        value=ValueIndicator(
            business_criticality=BusinessCriticality.TIER_2,
            cost_per_query=0.015,
            roi_score=8.5,
            sla_tier="Silver (99.5% uptime)",
        ),
        vulnerability=VulnerabilityIndicator(
            classification=VulnerabilityClassification.INTERNAL,
            access_control_policy="Policy: Analytics Read-Only",
            authorized_roles=["analyst", "data_scientist"],
        ),
    )

    # Dataset 4: ds_sensors_daily_agg (Processed -> requires TV-Value!)
    # Intentionally set last_refreshed_at to 3 hours ago with a 60-second TTL to trigger Staleness Monitor!
    stale_time = datetime.now(timezone.utc) - timedelta(hours=3)
    pilot.bind_dataset_tv_words(
        dataset_id="ds_sensors_daily_agg",
        volume=VolumeIndicator(byte_size=omd_agg_parquet.volume.byte_size, record_count=omd_agg_parquet.volume.record_count),
        velocity=VelocityIndicator(ingestion_mode="Batch Daily", expected_refresh_interval_sec=3600, last_refreshed_at=stale_time),
        variety=VarietyIndicator(schema_definition=omd_agg_parquet.variety.schema_definition, column_count=5, file_format="Parquet"),
        veracity=VeracityIndicator(source_origin="dbt Mart Aggregation Model", quality_score=0.99),
        variability=VariabilityIndicator(schema_version="v1.0"),
        value=ValueIndicator(
            business_criticality=BusinessCriticality.TIER_1,
            cost_per_query=0.04,
            roi_score=9.2,
            sla_tier="Gold (99.9% uptime)",
        ),
        vulnerability=VulnerabilityIndicator(
            classification=VulnerabilityClassification.INTERNAL,
            access_control_policy="Policy: Executive KPI Dashboards",
            authorized_roles=["executives", "analysts"],
        ),
    )

    # Dataset 5: ds_iot_lakehouse_asset (Complex, Processed)
    pilot.bind_dataset_tv_words(
        dataset_id="ds_iot_lakehouse_asset",
        volume=VolumeIndicator(byte_size=0, record_count=0),  # Will be recursively computed in Stage C!
        velocity=VelocityIndicator(ingestion_mode="Composite", expected_refresh_interval_sec=86400),
        variety=VarietyIndicator(schema_definition={}, column_count=0, file_format="Composite"),
        veracity=VeracityIndicator(source_origin="Assembled Lakehouse View", quality_score=0.99),
        variability=VariabilityIndicator(schema_version="v1.0"),
        value=ValueIndicator(
            business_criticality=BusinessCriticality.TIER_1,
            cost_per_query=0.10,
            roi_score=9.5,
            sla_tier="Gold",
        ),
        vulnerability=VulnerabilityIndicator(
            classification=VulnerabilityClassification.CONFIDENTIAL,
            access_control_policy="Policy: Multi-Department Lakehouse Authorization",
            authorized_roles=["admin", "lead_analyst"],
        ),
    )

    # Dataset 6: ds_abstract_telemetry (Abstract, Raw)
    pilot.bind_dataset_tv_words(
        dataset_id="ds_abstract_telemetry",
        volume=VolumeIndicator(byte_size=0, record_count=0),
        velocity=VelocityIndicator(ingestion_mode="Template", expected_refresh_interval_sec=86400 * 30),
        variety=VarietyIndicator(schema_definition={"timestamp": "timestamp", "metric": "float"}, column_count=2, file_format="Canonical"),
        veracity=VeracityIndicator(source_origin="Enterprise Architecture Governance Council", quality_score=1.0),
        variability=VariabilityIndicator(schema_version="v2.0"),
        vulnerability=VulnerabilityIndicator(
            classification=VulnerabilityClassification.PUBLIC,
            access_control_policy="Policy: Open Read-Only Architecture Standard",
            authorized_roles=["all"],
        ),
    )

    # 3. Bind InteractsWith Vulnerability Indicators
    for u, v, data in pilot.graph.edges(data=True):
        if data.get("relation_type") == "InteractsWith":
            pilot.bind_interacts_with_vulnerability(
                stakeholder_id=u,
                dataset_id=v,
                vulnerability=VulnerabilityIndicator(
                    classification=VulnerabilityClassification.INTERNAL,
                    access_control_policy=f"RBAC Grant for stakeholder '{u}' interacting with dataset '{v}'",
                    authorized_roles=[pilot.stakeholders[u].role],
                ),
            )

    stage_b_res = pilot.validate_stage_b_completeness()
    console.print(f"[green][OK] Stage B Complete:[/green] Bound {stage_b_res['bound_tmd_objects']} TMDObjects, {stage_b_res['bound_omd_objects']} OMDObjects, {stage_b_res['bound_rmd_objects']} RMDObjects.\n")

    # =====================================================================
    # STEP 2: STAGE C (Flattening & Registration)
    # =====================================================================
    console.print("[bold cyan]>>> Stage C: Flattening & Catalog Persistence[/bold cyan]")
    stage_c_res = pilot.register_catalog()
    tmd_complex = repository.tmd_objects["ds_iot_lakehouse_asset"]
    console.print(f"[green][OK] Stage C Complete:[/green] Complex dataset 'ds_iot_lakehouse_asset' recursively flattened:")
    console.print(f"  * Total Aggregated Byte Size: [bold cyan]{tmd_complex.volume.byte_size:,} bytes[/bold cyan]")
    console.print(f"  * Total Aggregated Records: [bold cyan]{tmd_complex.volume.record_count:,} rows[/bold cyan]")
    console.print(f"  * Combined Schema Column Count: [bold cyan]{tmd_complex.variety.column_count} columns[/bold cyan]\n")

    # =====================================================================
    # STEP 4: METADATA SERVICES (MSM - Constraints, Lineage, Staleness)
    # =====================================================================
    console.print("[bold cyan]>>> Step 4: Metadata Services (MSM) Execution[/bold cyan]")

    # 1. Constraint Engine Audit
    constraint_eng = ConstraintEngine(repository)
    alerts = constraint_eng.run_all_checks()
    console.print(f"  * Constraint Engine executed. Total rule alerts flagged: [bold]{len(alerts)}[/bold].")

    # 2. Lineage & Recommendation Engine
    lineage_eng = LineageAndRecEngine(repository)
    upstream_agg = lineage_eng.get_upstream_lineage("ds_sensors_daily_agg")
    console.print(f"  * Upstream Lineage for 'ds_sensors_daily_agg': {' -> '.join([u['name'] for u in upstream_agg['upstream_datasets']])}")

    impact_raw = lineage_eng.get_downstream_impact("ds_sensors_raw")
    console.print(f"  * Downstream Blast Radius for 'ds_sensors_raw': {impact_raw['blast_radius_count']} derived datasets affected.")

    sim_datasets = lineage_eng.get_similar_datasets("ds_sensors_raw")
    if sim_datasets:
        console.print(f"  * Recommendation Engine: top similar dataset to 'ds_sensors_raw' is '{sim_datasets[0]['name']}' (score: {sim_datasets[0]['similarity_score']}).")

    alt_datasets = lineage_eng.get_alternative_datasets("ds_sensors_raw")
    console.print(f"  * Recommendation Engine: alternative sibling models found: {len(alt_datasets)}.")

    # 3. Staleness & Drift Monitor
    staleness_mon = StalenessAndEvolutionMonitor(repository)
    stale_reports = staleness_mon.check_staleness()
    console.print(f"  * Staleness Monitor: [bold red]{len(stale_reports)} stale dataset(s) detected[/bold red] exceeding expected refresh TTL.")

    # Simulated schema drift test
    drift_res = staleness_mon.detect_schema_drift(
        dataset_id="ds_sensors_clean",
        current_schema={
            "sensor_id": "string",
            "timestamp": "timestamp",
            "temperature": "float",
            "humidity": "float",
            "battery_voltage": "float",  # NEW COLUMN!
        },
    )
    console.print(f"  * Schema Evolution Monitor: Drift test on 'ds_sensors_clean' -> Drift Detected = [bold yellow]{drift_res['drift_detected']}[/bold yellow] (Added: {list(drift_res['added_columns'].keys())})\n")

    # =====================================================================
    # STEP 5: VISUAL DASHBOARD PRESENTATION
    # =====================================================================
    console.print("[bold cyan]>>> Step 5: Terminal Dashboard Rendering[/bold cyan]")
    render_full_dashboard(repository, target_lineage_id="ds_sensors_daily_agg")

    # Cleanup sample directory
    try:
        shutil.rmtree(work_dir)
    except Exception:
        pass


if __name__ == "__main__":
    run_complete_end_to_end_demonstration()
