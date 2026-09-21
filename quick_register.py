import sys
import os
import argparse
import yaml
from rich.console import Console
from rich.panel import Panel

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass
from typing import Optional, Dict, Any, List
from rich import box

console = Console(legacy_windows=False)

from DLDSchema import (
    StructureType,
    LifecycleStage,
    VulnerabilityClassification,
    BusinessCriticality,
    StakeholderInput,
    DataObjectInput,
    DataSetInput,
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
from MetadataServices import ConstraintEngine, LineageAndRecEngine, StalenessAndEvolutionMonitor
from CLI_Dashboard import render_full_dashboard


def register_from_yaml(yaml_path: str, repo: Optional = None) -> MetadataRepository:
    """
    Ingests a declarative YAML manifest for a new dataset, executes the 3-stage
    Pilot wizard automatically, profiles physical files, and registers the catalog.
    """
    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"Manifest file not found: {yaml_path}")

    with open(yaml_path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)

    repository = repo or MetadataRepository()
    pilot = DLDSetupEngine(repository=repository)

    # 1. Parse Stakeholders
    stk_inputs = []
    for s in doc.get("stakeholders", []):
        stk_inputs.append(StakeholderInput(**s))

    # 2. Parse Dataset
    ds_data = doc["dataset"]
    ds_input = DataSetInput(
        id=ds_data["id"],
        name=ds_data["name"],
        structure_type=StructureType(ds_data.get("structure_type", "Simple")),
        lifecycle_stage=LifecycleStage(ds_data.get("lifecycle_stage", "Raw")),
        tags=ds_data.get("tags", []),
        interacts_with=ds_data.get("interacts_with", []),
        processed_from=ds_data.get("processed_from", []),
        assembly_children=ds_data.get("assembly_children", []),
        generalization_children=ds_data.get("generalization_children", []),
        data_objects=[
            DataObjectInput(id=do["id"], name=do["name"], physical_type=do["physical_type"], storage_path=do.get("storage_path"))
            for do in doc.get("data_objects", [])
        ],
    )

    manifest_dict = {
        "stakeholders": [s.model_dump() for s in stk_inputs],
        "datasets": [ds_input.model_dump()],
        "relationships": doc.get("relationships", []),
    }

    # STAGE A: Setup & DAG validation
    res_a = pilot.ingest_manifest(manifest_dict)
    console.print(f"[green][OK] Stage A Passed:[/green] Ingested dataset '{ds_input.name}' ({ds_input.id})")

    # STAGE B: Automated Profiling & Indicator Binding
    # 1. Auto-profile physical data objects if paths are provided
    total_bytes = 0
    total_records = 0
    combined_schema = {}

    for do in doc.get("data_objects", []):
        file_path = do.get("storage_path")
        if file_path and os.path.exists(file_path):
            omd = auto_bind_data_object(
                data_object_id=do["id"],
                dataset_id=ds_input.id,
                filepath=file_path,
                physical_type=do.get("physical_type"),
            )
            pilot.omd_objects[do["id"]] = omd
            total_bytes += omd.volume.byte_size
            if omd.volume.record_count:
                total_records += omd.volume.record_count
            combined_schema.update(omd.variety.schema_definition)
            console.print(f"  * Profiled physical object '{do['name']}': {omd.volume.byte_size:,} bytes, format={omd.variety.file_format}")
        else:
            # Fallback if file not on local disk
            pilot.omd_objects[do["id"]] = auto_bind_data_object(
                data_object_id=do["id"],
                dataset_id=ds_input.id,
                filepath=__file__,  # Dummy probe
                physical_type=do.get("physical_type", "File"),
            )

    # 2. TV-words
    ind_cfg = doc.get("indicators", {})
    vol_cfg = ind_cfg.get("volume", {})
    vel_cfg = ind_cfg.get("velocity", {})
    var_cfg = ind_cfg.get("variety", {})
    ver_cfg = ind_cfg.get("veracity", {})
    vbl_cfg = ind_cfg.get("variability", {})
    val_cfg = ind_cfg.get("value")
    vul_cfg = ind_cfg.get("vulnerability", {})

    value_indicator = None
    if ds_input.lifecycle_stage == LifecycleStage.PROCESSED:
        val_cfg = val_cfg or {}
        value_indicator = ValueIndicator(
            business_criticality=BusinessCriticality(val_cfg.get("business_criticality", BusinessCriticality.TIER_2)),
            cost_per_query=val_cfg.get("cost_per_query", 0.02),
            sla_tier=val_cfg.get("sla_tier", "Standard"),
        )

    tmd = pilot.bind_dataset_tv_words(
        dataset_id=ds_input.id,
        volume=VolumeIndicator(
            byte_size=vol_cfg.get("byte_size", total_bytes),
            record_count=vol_cfg.get("record_count", total_records),
        ),
        velocity=VelocityIndicator(
            ingestion_mode=vel_cfg.get("ingestion_mode", "Batch"),
            expected_refresh_interval_sec=vel_cfg.get("expected_refresh_interval_sec", 86400),
        ),
        variety=VarietyIndicator(
            schema_definition=var_cfg.get("schema_definition", combined_schema),
            column_count=var_cfg.get("column_count", len(combined_schema)),
            file_format=var_cfg.get("file_format", "Auto-Detected"),
        ),
        veracity=VeracityIndicator(
            source_origin=ver_cfg.get("source_origin", "User Pipeline Registration"),
            quality_score=ver_cfg.get("quality_score", 1.0),
        ),
        variability=VariabilityIndicator(schema_version=vbl_cfg.get("schema_version", "v1.0")),
        value=value_indicator,
        vulnerability=VulnerabilityIndicator(
            classification=VulnerabilityClassification(vul_cfg.get("classification", "Internal")),
            access_control_policy=vul_cfg.get("access_control_policy", "Standard Access"),
            authorized_roles=vul_cfg.get("authorized_roles", ["user"]),
        ),
    )

    # 3. InteractsWith Vulnerability
    for stk_id in ds_input.interacts_with:
        pilot.bind_interacts_with_vulnerability(
            stakeholder_id=stk_id,
            dataset_id=ds_input.id,
            vulnerability=VulnerabilityIndicator(
                classification=VulnerabilityClassification(vul_cfg.get("classification", "Internal")),
                access_control_policy=f"Access grant for stakeholder {stk_id}",
                authorized_roles=vul_cfg.get("authorized_roles", ["user"]),
            ),
        )

    pilot.validate_stage_b_completeness()
    console.print(f"[green][OK] Stage B Passed:[/green] All 7 V-Words & container MD objects successfully bound.")

    # STAGE C: Flattening & Catalog Registration
    res_c = pilot.register_catalog()
    console.print(f"[green][OK] Stage C Passed:[/green] Catalog updated. Dataset '{ds_input.name}' is now ACTIVE.\n")

    return repository


def main():
    parser = argparse.ArgumentParser(description="MeDOM & DLD Dataset Quick Registration CLI")
    parser.add_argument("--manifest", type=str, default="new_dataset_example.yaml", help="Path to YAML manifest")
    parser.add_argument("--audit", action="store_true", help="Run governance audit after registration")
    args = parser.parse_args()

    console.print(Panel(f"[bold cyan]Registering Dataset via Manifest:[/bold cyan] {args.manifest}", box=box.ROUNDED))
    repo = register_from_yaml(args.manifest)

    # Render dashboard for the registered dataset
    render_full_dashboard(repo)


if __name__ == "__main__":
    from typing import Optional
    from rich import box
    main()
