import os
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel

from DLDSchema import (
    DLDManifest,
    VolumeIndicator,
    VelocityIndicator,
    VarietyIndicator,
    VeracityIndicator,
    VariabilityIndicator,
    ValueIndicator,
    VulnerabilityIndicator,
    BusinessCriticality,
    VulnerabilityClassification,
)
from IngestionController import DLDSetupEngine, MetadataRepository
from MetadataServices import (
    ConstraintEngine,
    LineageAndRecEngine,
    StalenessAndEvolutionMonitor,
    ValidationAlert,
)
from ExtractionHelpers import profile_parquet_file, profile_csv_file, profile_os_file
from mimic_case_study import get_mimic_dld_manifest

app = FastAPI(
    title="MeDOM & DLD Metadata Management Service",
    description="Enterprise Metadata Management System based on DLD and MeDOM ontologies, 7 V-Words, and 3-Stage Pilot Wizard.",
    version="1.0.0",
)

# Shared in-memory catalog repository and engines
repository = MetadataRepository()
pilot_engine = DLDSetupEngine(repository=repository)
constraint_engine = ConstraintEngine(repository=repository)
lineage_engine = LineageAndRecEngine(repository=repository)
staleness_monitor = StalenessAndEvolutionMonitor(repository=repository)


# =====================================================================
# Request / Response Schemas for Stage B Binding
# =====================================================================

class BindTVWordsRequest(BaseModel):
    dataset_id: str
    volume: VolumeIndicator
    velocity: VelocityIndicator
    variety: VarietyIndicator
    veracity: VeracityIndicator
    variability: VariabilityIndicator
    value: Optional[ValueIndicator] = None
    vulnerability: Optional[VulnerabilityIndicator] = None


class BindOVWordsRequest(BaseModel):
    data_object_id: str
    dataset_id: str
    volume: VolumeIndicator
    variety: VarietyIndicator
    variability: VariabilityIndicator


class BindVulnerabilityRequest(BaseModel):
    stakeholder_id: str
    dataset_id: str
    vulnerability: VulnerabilityIndicator
    attributes: Optional[Dict[str, Any]] = None


class ProfileFileRequest(BaseModel):
    filepath: str
    physical_type: Optional[str] = None


# =====================================================================
# Pilot Wizard Endpoints (Step 2)
# =====================================================================

@app.post("/pilot/manifest", tags=["Metadata Pilot"])
def ingest_dld_manifest(manifest: DLDManifest) -> Dict[str, Any]:
    """
    Stage A (DLD Setup): Ingests the raw manifest, registers entities,
    and performs DAG cycle validation.
    """
    try:
        res = pilot_engine.ingest_manifest(manifest.model_dump())
        return res
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/pilot/bind/tv-words", tags=["Metadata Pilot"])
def bind_tv_words(req: BindTVWordsRequest) -> Dict[str, Any]:
    """
    Stage B: Bind the 6 TV-words (+ Value if processed) to a DataSet.
    """
    try:
        tmd = pilot_engine.bind_dataset_tv_words(
            dataset_id=req.dataset_id,
            volume=req.volume,
            velocity=req.velocity,
            variety=req.variety,
            veracity=req.veracity,
            variability=req.variability,
            value=req.value,
            vulnerability=req.vulnerability,
        )
        return {"status": "TV-words bound", "tmd_object": tmd.model_dump()}
    except (KeyError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/pilot/bind/ov-words", tags=["Metadata Pilot"])
def bind_ov_words(req: BindOVWordsRequest) -> Dict[str, Any]:
    """
    Stage B: Bind OV-words (OVolume, OVariety, OVariability) to a DataObject.
    """
    try:
        omd = pilot_engine.bind_data_object_ov_words(
            data_object_id=req.data_object_id,
            dataset_id=req.dataset_id,
            volume=req.volume,
            variety=req.variety,
            variability=req.variability,
        )
        return {"status": "OV-words bound", "omd_object": omd.model_dump()}
    except (KeyError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/pilot/bind/vulnerability", tags=["Metadata Pilot"])
def bind_vulnerability(req: BindVulnerabilityRequest) -> Dict[str, Any]:
    """
    Stage B: Bind Vulnerability (authorization policy, encryption) to an InteractsWith edge.
    """
    try:
        rmd = pilot_engine.bind_interacts_with_vulnerability(
            stakeholder_id=req.stakeholder_id,
            dataset_id=req.dataset_id,
            vulnerability=req.vulnerability,
            attributes=req.attributes,
        )
        return {"status": "Vulnerability bound", "rmd_object": rmd.model_dump()}
    except (KeyError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/pilot/register", tags=["Metadata Pilot"])
def register_catalog() -> Dict[str, Any]:
    """
    Stage C: Recursively flattens complex datasets, compiles schema hierarchies,
    and registers the catalog into the persistence repository.
    """
    try:
        res = pilot_engine.register_catalog()
        return res
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# =====================================================================
# Catalog Discovery & Inspection Endpoints
# =====================================================================

@app.get("/catalog/datasets", tags=["Catalog"])
def list_datasets(stage: Optional[str] = None, structure: Optional[str] = None) -> List[Dict[str, Any]]:
    results = []
    for ds_id, ds in repository.datasets.items():
        if stage and ds.lifecycle_stage.value.lower() != stage.lower():
            continue
        if structure and ds.structure_type.value.lower() != structure.lower():
            continue
        tmd = repository.tmd_objects.get(ds_id)
        results.append({
            "id": ds.id,
            "name": ds.name,
            "structure_type": ds.structure_type.value,
            "lifecycle_stage": ds.lifecycle_stage.value,
            "tags": ds.tags,
            "byte_size": tmd.volume.byte_size if tmd else None,
            "record_count": tmd.volume.record_count if tmd else None,
            "column_count": tmd.variety.column_count if tmd else None,
            "has_value_indicator": tmd.value is not None if tmd else False,
        })
    return results


@app.get("/catalog/datasets/{dataset_id}", tags=["Catalog"])
def get_dataset(dataset_id: str) -> Dict[str, Any]:
    if dataset_id not in repository.datasets:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found.")

    ds = repository.datasets[dataset_id]
    tmd = repository.tmd_objects.get(dataset_id)
    objects = [
        repository.omd_objects[do.id].model_dump()
        for do in ds.data_objects
        if do.id in repository.omd_objects
    ]
    return {
        "dataset": ds.model_dump(),
        "tmd_object": tmd.model_dump() if tmd else None,
        "data_objects": objects,
    }


# =====================================================================
# Lineage & Recommendation Endpoints (Step 4)
# =====================================================================

@app.get("/catalog/lineage/{dataset_id}/upstream", tags=["Lineage & Recommendations"])
def get_upstream_lineage(dataset_id: str) -> Dict[str, Any]:
    try:
        return lineage_engine.get_upstream_lineage(dataset_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/catalog/lineage/{dataset_id}/impact", tags=["Lineage & Recommendations"])
def get_downstream_impact(dataset_id: str) -> Dict[str, Any]:
    try:
        return lineage_engine.get_downstream_impact(dataset_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/catalog/recommendations/similar/{dataset_id}", tags=["Lineage & Recommendations"])
def get_similar_datasets(dataset_id: str, top_k: int = Query(default=5, ge=1, le=20)) -> List[Dict[str, Any]]:
    try:
        return lineage_engine.get_similar_datasets(dataset_id, top_k=top_k)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/catalog/recommendations/alternatives/{dataset_id}", tags=["Lineage & Recommendations"])
def get_alternative_datasets(dataset_id: str) -> List[Dict[str, Any]]:
    try:
        return lineage_engine.get_alternative_datasets(dataset_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


# =====================================================================
# Governance & Evolution Monitoring Endpoints (Step 4)
# =====================================================================

@app.get("/governance/audit", tags=["Governance & Monitoring"])
def run_governance_audit() -> List[Dict[str, Any]]:
    alerts = constraint_engine.run_all_checks()
    return [a.model_dump() for a in alerts]


@app.get("/governance/staleness", tags=["Governance & Monitoring"])
def check_staleness() -> List[Dict[str, Any]]:
    return staleness_monitor.check_staleness()


# =====================================================================
# Interactive Canvas Studio Endpoints
# =====================================================================

@app.get("/canvas", response_class=HTMLResponse, tags=["Canvas Studio"])
def get_canvas_studio() -> HTMLResponse:
    """
    Renders the 2D Drag-and-Drop Interactive Metadata Canvas Studio.
    """
    html_path = os.path.join(os.path.dirname(__file__), "canvas_app.html")
    if not os.path.exists(html_path):
        raise HTTPException(status_code=404, detail=f"Canvas UI file not found at {html_path}")
    with open(html_path, "r", encoding="utf-8") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content)


@app.get("/pilot/sample/mimic", tags=["Canvas Studio"])
def get_mimic_sample_manifest() -> Dict[str, Any]:
    """
    Returns the complete MIMIC multimodal data lakehouse manifest
    for one-click loading on the canvas.
    """
    try:
        return get_mimic_dld_manifest()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pilot/profile-file", tags=["Canvas Studio"])
def profile_local_file(req: ProfileFileRequest) -> Dict[str, Any]:
    """
    Profiles a local file (Parquet header stats, CSV sample, or OS stat)
    and returns TVolume and TVariety metrics without full table scans.
    """
    if not os.path.exists(req.filepath):
        raise HTTPException(status_code=404, detail=f"File not found: {req.filepath}")

    lower = req.filepath.lower()
    try:
        if lower.endswith(".parquet") or lower.endswith(".pq"):
            stats = profile_parquet_file(req.filepath)
        elif lower.endswith(".csv") or lower.endswith(".csv.gz") or lower.endswith(".tsv"):
            stats = profile_csv_file(req.filepath)
        else:
            stats = profile_os_file(req.filepath)
            stats["file_format"] = req.physical_type or "Binary"
            stats["record_count"] = None
            stats["column_count"] = 0
            stats["schema_definition"] = {}
        return {"status": "success", "filepath": req.filepath, "stats": stats}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Profiling error: {str(e)}")


@app.post("/pilot/canvas/validate", tags=["Canvas Studio"])
def validate_canvas_graph(manifest: DLDManifest) -> Dict[str, Any]:
    """
    Dry-run validation of a canvas graph topology without mutating global state.
    Checks DAG cycle constraints (derivation vs composition) and stakeholder completeness.
    """
    try:
        dry_run_engine = DLDSetupEngine()
        result = dry_run_engine.ingest_manifest(manifest.model_dump())
        return {
            "valid": True,
            "status": "Graph topology is valid and cycle-free.",
            "details": result,
        }
    except Exception as e:
        return {
            "valid": False,
            "status": f"Validation failed: {str(e)}",
            "error": str(e),
        }


@app.post("/pilot/canvas/commit", tags=["Canvas Studio"])
def commit_canvas_to_catalog(manifest: DLDManifest) -> Dict[str, Any]:
    """
    Commits a canvas manifest into the persistent catalog repository:
    ingests manifest, initializes baseline indicators for any unbound datasets,
    and performs Stage C registration.
    """
    try:
        # 1. Ingest Manifest (Stage A)
        ingest_res = pilot_engine.ingest_manifest(manifest.model_dump())

        # 2. Auto-bind baseline TV-words and Vulnerability for any unbound datasets (Stage B)
        for ds_id, ds in pilot_engine.datasets.items():
            if ds_id not in pilot_engine.tmd_objects:
                val_indicator = None
                if ds.lifecycle_stage.value == "Processed":
                    val_indicator = ValueIndicator(
                        business_criticality=BusinessCriticality.TIER_2,
                        sla_tier="Standard",
                    )
                pilot_engine.bind_dataset_tv_words(
                    dataset_id=ds_id,
                    volume=VolumeIndicator(byte_size=1048576, record_count=10000),
                    velocity=VelocityIndicator(ingestion_mode="Batch", expected_refresh_interval_sec=86400),
                    variety=VarietyIndicator(column_count=10, file_format="Parquet"),
                    veracity=VeracityIndicator(
                        source_origin="Lakehouse Ingestion Canvas",
                        quality_score=0.99,
                        null_rate=0.01,
                    ),
                    variability=VariabilityIndicator(schema_version="v1.0"),
                    value=val_indicator,
                    vulnerability=VulnerabilityIndicator(
                        classification=VulnerabilityClassification.CONFIDENTIAL,
                        access_control_policy="Standard Governance Policy",
                    ),
                )

            # Auto-bind DataObjects if any
            for do in ds.data_objects:
                if do.id not in pilot_engine.omd_objects:
                    pilot_engine.bind_data_object_ov_words(
                        data_object_id=do.id,
                        dataset_id=ds_id,
                        volume=VolumeIndicator(byte_size=524288, record_count=5000),
                        variety=VarietyIndicator(column_count=5, file_format=do.physical_type),
                        variability=VariabilityIndicator(schema_version="v1.0"),
                    )

            # Auto-bind InteractsWith vulnerabilities
            for stk_id in ds.interacts_with:
                rel_id = f"{stk_id}_INTERACTS_{ds_id}"
                if rel_id not in pilot_engine.rmd_objects:
                    pilot_engine.bind_interacts_with_vulnerability(
                        stakeholder_id=stk_id,
                        dataset_id=ds_id,
                        vulnerability=VulnerabilityIndicator(
                            classification=VulnerabilityClassification.CONFIDENTIAL,
                            access_control_policy="Standard Lakehouse Access Control",
                        ),
                    )

        # 3. Perform Stage C registration
        reg_res = pilot_engine.register_catalog()
        return {
            "status": "Committed and Registered",
            "ingest_result": ingest_res,
            "catalog_result": reg_res,
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Commit failed: {str(e)}")

