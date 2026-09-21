from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List

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


def get_mimic_dld_manifest() -> Dict[str, Any]:
    """
    Constructs the DLD Manifest for the MIMIC Multimodal Healthcare Lakehouse.
    Features:
      - Root complex assembly: MIMIC-IV
      - Nested assemblies: Core, ICU, Clinical Notes, CXR (Imaging), ECG (Waveforms)
      - ISA Hierarchies (GeneralizationOf): ICU -> MICU, SICU, TSICU
      - Referential Links: Hospital(hadm_id) = ICU(hadm_id)
      - Multimodal Join Links: Hospital(subject_id) = Notes(subject_id) = CXR(subject_id)
      - Temporal Flows: Emergency -> Hospital Inpatient -> ICU -> Discharge
    """
    return {
        "stakeholders": [
            {
                "id": "stk_physionet_curator",
                "name": "Dr. Leo Celi (PhysioNet Steward)",
                "role": "Chief Medical Data Steward",
                "email": "steward@physionet.org",
                "department": "MIT Laboratory for Computational Physiology",
            },
            {
                "id": "stk_clinical_analyst",
                "name": "Dr. Emily Chen",
                "role": "Intensive Care Clinical Researcher",
                "email": "emily.chen@hospital.edu",
                "department": "Department of Medicine",
            },
            {
                "id": "stk_hipaa_compliance",
                "name": "Marcus Vance",
                "role": "IRB & HIPAA Compliance Officer",
                "email": "irb@hospital.edu",
                "department": "Hospital Data Governance Board",
            },
        ],
        "datasets": [
            # 1. Root Complex Lakehouse: MIMIC-IV
            {
                "id": "ds_mimic_root",
                "name": "MIMIC-IV Multimodal Health Data Lake",
                "structure_type": "Complex",
                "lifecycle_stage": "Processed",
                "description": "Comprehensive multimodal clinical database incorporating inpatient records, intensive care telemetry, diagnostic radiology imaging, and high-frequency physiological waveforms.",
                "tags": ["healthcare", "mimic", "multimodal", "ehr", "icu", "imaging", "waveforms"],
                "interacts_with": ["stk_physionet_curator", "stk_hipaa_compliance"],
                "assembly_children": [
                    "ds_mimic_core",
                    "ds_mimic_icu",
                    "ds_mimic_notes",
                    "ds_mimic_cxr",
                    "ds_mimic_ecg",
                ],
                "processed_from": ["ds_mimic_core", "ds_mimic_icu"],
                "data_objects": [],
            },
            # 2. Sub-Dataset: MIMIC Core (Hospital / Demographics)
            {
                "id": "ds_mimic_core",
                "name": "MIMIC-IV Core (Hospital Admissions & Inpatient Demographics)",
                "structure_type": "Simple",
                "lifecycle_stage": "Processed",
                "description": "Hospital-wide admissions, discharges, patient demographics, and transfer events across all medical wards.",
                "tags": ["hospital", "admissions", "demographics", "transfers"],
                "interacts_with": ["stk_physionet_curator", "stk_clinical_analyst"],
                "processed_from": ["ds_hospital_ehr_raw"],
                "data_objects": [
                    {"id": "obj_patients_parquet", "name": "patients.parquet", "physical_type": "Parquet"},
                    {"id": "obj_admissions_parquet", "name": "admissions.parquet", "physical_type": "Parquet"},
                    {"id": "obj_transfers_csv", "name": "transfers.csv", "physical_type": "CSV"},
                ],
            },
            # 3. Sub-Dataset: MIMIC ICU (Intensive Care Unit)
            {
                "id": "ds_mimic_icu",
                "name": "MIMIC-IV ICU (Critical Care Stays & Measurements)",
                "structure_type": "Simple",
                "lifecycle_stage": "Processed",
                "description": "High-acuity clinical measurements, chart events, input/output events, and stay identifiers for ICU patients.",
                "tags": ["icu", "critical_care", "chartevents", "vitals"],
                "interacts_with": ["stk_clinical_analyst"],
                "processed_from": ["ds_icu_bedside_raw"],
                "data_objects": [
                    {"id": "obj_icustays_parquet", "name": "icustays.parquet", "physical_type": "Parquet"},
                    {"id": "obj_chartevents_parquet", "name": "chartevents.parquet", "physical_type": "Parquet"},
                ],
            },
            # 4. Sub-Dataset: Clinical Notes (Free-Text De-identified)
            {
                "id": "ds_mimic_notes",
                "name": "MIMIC-IV Note (De-identified Clinical Free-Text)",
                "structure_type": "Simple",
                "lifecycle_stage": "Processed",
                "description": "De-identified nursing notes, physician progress notes, and discharge summaries with HIPAA Safe Harbor date-shifting.",
                "tags": ["nlp", "clinical_notes", "discharge_summaries", "free_text"],
                "interacts_with": ["stk_physionet_curator", "stk_clinical_analyst"],
                "processed_from": ["ds_ehr_unstructured_raw"],
                "data_objects": [
                    {"id": "obj_discharge_parquet", "name": "discharge.parquet", "physical_type": "Parquet"},
                    {"id": "obj_radiology_notes_csv", "name": "radiology_reports.csv", "physical_type": "CSV"},
                ],
            },
            # 5. Sub-Dataset: MIMIC-CXR (Diagnostic Imaging)
            {
                "id": "ds_mimic_cxr",
                "name": "MIMIC-CXR (Chest Radiographs Imaging)",
                "structure_type": "Simple",
                "lifecycle_stage": "Processed",
                "description": "377,110 chest radiographs in DICOM and JPG formats with associated radiologist diagnostic reports.",
                "tags": ["radiology", "cxr", "imaging", "dicom", "xray"],
                "interacts_with": ["stk_clinical_analyst"],
                "processed_from": ["ds_pacs_imaging_raw"],
                "data_objects": [
                    {"id": "obj_cxr_dicom_archive", "name": "cxr_studies.dicom", "physical_type": "DICOM Archive"},
                    {"id": "obj_cxr_metadata_csv", "name": "cxr_metadata.csv", "physical_type": "CSV"},
                ],
            },
            # 6. Sub-Dataset: MIMIC-ECG (Physiological Waveforms)
            {
                "id": "ds_mimic_ecg",
                "name": "MIMIC-IV ECG (High-Frequency Waveforms)",
                "structure_type": "Simple",
                "lifecycle_stage": "Processed",
                "description": "Multi-lead electrocardiogram waveforms stored in PhysioNet WFDB format (sampling rate 500 Hz).",
                "tags": ["waveforms", "ecg", "time_series", "wfdb", "electrophysiology"],
                "interacts_with": ["stk_clinical_analyst"],
                "processed_from": ["ds_bedside_telemetry_raw"],
                "data_objects": [
                    {"id": "obj_ecg_wfdb_records", "name": "ecg_leads.wfdb", "physical_type": "WFDB Binary"},
                ],
            },
            # 7. Raw Origin Feeds
            {
                "id": "ds_hospital_ehr_raw",
                "name": "Beth Israel Deaconess Hospital EHR Feed",
                "structure_type": "Simple",
                "lifecycle_stage": "Raw",
                "interacts_with": ["stk_physionet_curator"],
                "data_objects": [{"id": "obj_raw_ehr", "name": "ehr_dump.sql", "physical_type": "SQL Dump"}],
            },
            {
                "id": "ds_icu_bedside_raw",
                "name": "MetaVision / CareVue ICU Bedside Monitor Dump",
                "structure_type": "Simple",
                "lifecycle_stage": "Raw",
                "interacts_with": ["stk_physionet_curator"],
                "data_objects": [{"id": "obj_raw_metavision", "name": "metavision_export.bin", "physical_type": "Binary"}],
            },
            {
                "id": "ds_ehr_unstructured_raw",
                "name": "Raw Medical Record Dictations (Contains PHI)",
                "structure_type": "Simple",
                "lifecycle_stage": "Raw",
                "interacts_with": ["stk_hipaa_compliance"],
                "data_objects": [{"id": "obj_raw_dictations", "name": "dictations.raw", "physical_type": "Text"}],
            },
            {
                "id": "ds_pacs_imaging_raw",
                "name": "Hospital PACS Radiology Picture Archive",
                "structure_type": "Simple",
                "lifecycle_stage": "Raw",
                "interacts_with": ["stk_physionet_curator"],
                "data_objects": [{"id": "obj_raw_pacs", "name": "pacs_archive.tar", "physical_type": "Archive"}],
            },
            {
                "id": "ds_bedside_telemetry_raw",
                "name": "High-Frequency ICU Waveform Hub",
                "structure_type": "Simple",
                "lifecycle_stage": "Raw",
                "interacts_with": ["stk_physionet_curator"],
                "data_objects": [{"id": "obj_raw_telemetry", "name": "telemetry_stream.dat", "physical_type": "Dat Stream"}],
            },
            # 8. ISA Hierarchy (GeneralizationOf): ICU -> MICU, SICU, TSICU
            {
                "id": "ds_abstract_icu",
                "name": "Canonical Intensive Care Specialization Model",
                "structure_type": "Abstract",
                "lifecycle_stage": "Raw",
                "tags": ["canonical", "ontology", "icu_archetype"],
                "interacts_with": ["stk_physionet_curator"],
                "generalization_children": ["ds_micu", "ds_sicu", "ds_tsicu"],
                "data_objects": [],
            },
            {
                "id": "ds_micu",
                "name": "MICU (Medical Intensive Care Unit)",
                "structure_type": "Simple",
                "lifecycle_stage": "Processed",
                "tags": ["micu", "pulmonary", "sepsis"],
                "interacts_with": ["stk_clinical_analyst"],
                "processed_from": ["ds_mimic_icu"],
                "data_objects": [{"id": "obj_micu_slice", "name": "micu_stays.parquet", "physical_type": "Parquet"}],
            },
            {
                "id": "ds_sicu",
                "name": "SICU (Surgical Intensive Care Unit)",
                "structure_type": "Simple",
                "lifecycle_stage": "Processed",
                "tags": ["sicu", "post_operative"],
                "interacts_with": ["stk_clinical_analyst"],
                "processed_from": ["ds_mimic_icu"],
                "data_objects": [{"id": "obj_sicu_slice", "name": "sicu_stays.parquet", "physical_type": "Parquet"}],
            },
            {
                "id": "ds_tsicu",
                "name": "TSICU (Trauma-Surgical Intensive Care Unit)",
                "structure_type": "Simple",
                "lifecycle_stage": "Processed",
                "tags": ["tsicu", "trauma", "neurosurgery"],
                "interacts_with": ["stk_clinical_analyst"],
                "processed_from": ["ds_mimic_icu"],
                "data_objects": [{"id": "obj_tsicu_slice", "name": "tsicu_stays.parquet", "physical_type": "Parquet"}],
            },
        ],
        "relationships": [
            # 1. Referential Join: Hospital(hadm_id) = ICU(hadm_id)
            {
                "relationship_type": "Referential",
                "source_dataset_id": "ds_mimic_core",
                "target_dataset_id": "ds_mimic_icu",
                "attributes": {
                    "join_keys": ["hadm_id", "subject_id"],
                    "cardinality": "1:N (One hospital admission has multiple ICU stays)",
                    "foreign_key": "admissions.hadm_id -> icustays.hadm_id",
                },
            },
            # 2. Multimodal Join: Hospital(subject_id) = Clinical_Notes(subject_id)
            {
                "relationship_type": "Referential",
                "source_dataset_id": "ds_mimic_core",
                "target_dataset_id": "ds_mimic_notes",
                "attributes": {
                    "join_keys": ["subject_id", "hadm_id"],
                    "cardinality": "1:N",
                    "foreign_key": "admissions.hadm_id -> discharge.hadm_id",
                },
            },
            # 3. Multimodal Join: Hospital(subject_id) = CXR(subject_id)
            {
                "relationship_type": "Referential",
                "source_dataset_id": "ds_mimic_core",
                "target_dataset_id": "ds_mimic_cxr",
                "attributes": {
                    "join_keys": ["subject_id"],
                    "cardinality": "1:N (Patient has multiple chest radiographs)",
                    "foreign_key": "patients.subject_id -> cxr_metadata.subject_id",
                },
            },
            # 4. Temporal Flow: Emergency -> Inpatient -> ICU
            {
                "relationship_type": "Temporal",
                "source_dataset_id": "ds_mimic_core",
                "target_dataset_id": "ds_mimic_icu",
                "attributes": {
                    "temporal_order": "admittime < intime <= outtime <= dischtime",
                    "description": "Patient admitted to hospital ward before transferring into ICU",
                },
            },
            # 5. Semantic Correlation: Clinical Notes <-> Radiology CXR
            {
                "relationship_type": "Semantic",
                "source_dataset_id": "ds_mimic_notes",
                "target_dataset_id": "ds_mimic_cxr",
                "attributes": {
                    "concept_overlap": "Radiology Findings & Impression documented in both note text and CXR reports",
                },
            },
        ],
    }


def build_and_register_mimic_lakehouse() -> MetadataRepository:
    """
    Executes Stages A, B, and C for the complete MIMIC case study,
    binding domain-specific healthcare indicators (EHR sources, HIPAA DUA, DICOM/WFDB).
    """
    repo = MetadataRepository()
    pilot = DLDSetupEngine(repository=repo)

    manifest_dict = get_mimic_dld_manifest()

    # =====================================================================
    # STAGE A: DLD Setup & DAG Validation
    # =====================================================================
    pilot.ingest_manifest(manifest_dict)

    # =====================================================================
    # STAGE B: Context-Driven Indicator Binding (The 7 V-Words)
    # =====================================================================

    # 1. Bind Operational OV-Words to Physical Data Objects
    obj_metrics = {
        "obj_patients_parquet": {"bytes": 12_500_000, "records": 315_000, "cols": 6, "format": "Parquet", "compression": "SNAPPY"},
        "obj_admissions_parquet": {"bytes": 45_000_000, "records": 430_000, "cols": 15, "format": "Parquet", "compression": "SNAPPY"},
        "obj_transfers_csv": {"bytes": 98_000_000, "records": 1_890_000, "cols": 8, "format": "CSV", "compression": "NONE"},
        "obj_icustays_parquet": {"bytes": 22_000_000, "records": 73_000, "cols": 9, "format": "Parquet", "compression": "SNAPPY"},
        "obj_chartevents_parquet": {"bytes": 380_000_000_000, "records": 313_000_000, "cols": 10, "format": "Parquet", "compression": "GZIP"},
        "obj_discharge_parquet": {"bytes": 1_200_000_000, "records": 330_000, "cols": 6, "format": "Parquet", "compression": "ZSTD"},
        "obj_radiology_notes_csv": {"bytes": 4_500_000_000, "records": 2_300_000, "cols": 5, "format": "CSV", "compression": "NONE"},
        "obj_cxr_dicom_archive": {"bytes": 4_800_000_000_000, "records": 377_110, "cols": 12, "format": "DICOM Archive", "compression": "JPEG-Lossless"},
        "obj_cxr_metadata_csv": {"bytes": 85_000_000, "records": 377_110, "cols": 14, "format": "CSV", "compression": "NONE"},
        "obj_ecg_wfdb_records": {"bytes": 750_000_000_000, "records": 800_000, "cols": 12, "format": "WFDB Binary", "compression": "Binary-16bit"},
        "obj_micu_slice": {"bytes": 8_500_000, "records": 25_000, "cols": 9, "format": "Parquet", "compression": "SNAPPY"},
        "obj_sicu_slice": {"bytes": 6_200_000, "records": 18_000, "cols": 9, "format": "Parquet", "compression": "SNAPPY"},
        "obj_tsicu_slice": {"bytes": 4_100_000, "records": 12_000, "cols": 9, "format": "Parquet", "compression": "SNAPPY"},
        # Raw feeds
        "obj_raw_ehr": {"bytes": 180_000_000_000, "records": 50_000_000, "cols": 120, "format": "SQL Dump", "compression": "NONE"},
        "obj_raw_metavision": {"bytes": 450_000_000_000, "records": 400_000_000, "cols": 50, "format": "Binary", "compression": "NONE"},
        "obj_raw_dictations": {"bytes": 10_000_000_000, "records": 3_000_000, "cols": 3, "format": "Text", "compression": "NONE"},
        "obj_raw_pacs": {"bytes": 5_200_000_000_000, "records": 400_000, "cols": 20, "format": "Archive", "compression": "NONE"},
        "obj_raw_telemetry": {"bytes": 900_000_000_000, "records": 1_000_000, "cols": 12, "format": "Dat Stream", "compression": "NONE"},
    }

    for do_id, do in pilot.data_objects.items():
        m = obj_metrics.get(do_id, {"bytes": 1_000_000, "records": 10_000, "cols": 5, "format": "File", "compression": "NONE"})
        pilot.bind_data_object_ov_words(
            data_object_id=do_id,
            dataset_id=do.id,  # lookup
            volume=VolumeIndicator(byte_size=m["bytes"], record_count=m["records"]),
            variety=VarietyIndicator(column_count=m["cols"], file_format=m["format"], compression_algorithm=m["compression"]),
            variability=VariabilityIndicator(schema_version="v4.0"),
        )

    # 2. Bind Technical TV-Words to Datasets
    # MIMIC Core
    pilot.bind_dataset_tv_words(
        dataset_id="ds_mimic_core",
        volume=VolumeIndicator(byte_size=155_500_000, record_count=2_635_000),
        velocity=VelocityIndicator(ingestion_mode="Batch Annual Release", expected_refresh_interval_sec=86400 * 365),
        variety=VarietyIndicator(column_count=29, file_format="Parquet / CSV", schema_definition={"subject_id": "int32", "hadm_id": "int32", "admittime": "timestamp", "dischtime": "timestamp", "gender": "string"}),
        veracity=VeracityIndicator(source_origin="Hospital Billing & Patient Master Index (EHR)", quality_score=0.99),
        variability=VariabilityIndicator(schema_version="MIMIC-IV v2.2"),
        value=ValueIndicator(business_criticality=BusinessCriticality.TIER_1, cost_per_query=0.01, sla_tier="Gold Research SLA"),
        vulnerability=VulnerabilityIndicator(
            classification=VulnerabilityClassification.RESTRICTED,
            access_control_policy="PhysioNet Credentialed Access (CITI Training, Signed DUA Required)",
            authorized_roles=["credentialed_researcher", "data_steward"],
        ),
    )

    # MIMIC ICU
    pilot.bind_dataset_tv_words(
        dataset_id="ds_mimic_icu",
        volume=VolumeIndicator(byte_size=380_022_000_000, record_count=313_073_000),
        velocity=VelocityIndicator(ingestion_mode="Batch Annual Release", expected_refresh_interval_sec=86400 * 365),
        variety=VarietyIndicator(column_count=19, file_format="Parquet", schema_definition={"subject_id": "int32", "hadm_id": "int32", "stay_id": "int32", "charttime": "timestamp", "valuenum": "float32"}),
        veracity=VeracityIndicator(source_origin="MetaVision ICU Bedside Monitors", quality_score=0.97),
        variability=VariabilityIndicator(
            schema_version="MIMIC-IV v2.2",
            change_history=[{
                "evolution_note": "Join key migration from MIMIC-III 'icustay_id' to MIMIC-IV 'stay_id'.",
                "timestamp": "2024-01-15T00:00:00Z"
            }]
        ),
        value=ValueIndicator(business_criticality=BusinessCriticality.TIER_1, cost_per_query=0.15, sla_tier="Gold Research SLA"),
        vulnerability=VulnerabilityIndicator(
            classification=VulnerabilityClassification.RESTRICTED,
            access_control_policy="PhysioNet Credentialed Access (CITI Training, Signed DUA Required)",
            authorized_roles=["credentialed_researcher"],
        ),
    )

    # MIMIC Notes
    pilot.bind_dataset_tv_words(
        dataset_id="ds_mimic_notes",
        volume=VolumeIndicator(byte_size=5_700_000_000, record_count=2_630_000),
        velocity=VelocityIndicator(ingestion_mode="Batch Annual Release", expected_refresh_interval_sec=86400 * 365),
        variety=VarietyIndicator(column_count=11, file_format="Parquet / Text", schema_definition={"note_id": "string", "subject_id": "int32", "hadm_id": "int32", "note_type": "string", "text": "string"}),
        veracity=VeracityIndicator(source_origin="Physician & Nursing Dictation Transcripts", quality_score=0.96),
        variability=VariabilityIndicator(schema_version="MIMIC-IV Note v1.0"),
        value=ValueIndicator(business_criticality=BusinessCriticality.TIER_1, cost_per_query=0.08, sla_tier="Gold Research SLA"),
        vulnerability=VulnerabilityIndicator(
            classification=VulnerabilityClassification.RESTRICTED,
            access_control_policy="HIPAA Safe Harbor De-identified, Strictly Prohibits Re-identification Attempt",
            authorized_roles=["credentialed_researcher"],
        ),
    )

    # MIMIC CXR (Imaging)
    pilot.bind_dataset_tv_words(
        dataset_id="ds_mimic_cxr",
        volume=VolumeIndicator(byte_size=4_800_085_000_000, record_count=377_110),
        velocity=VelocityIndicator(ingestion_mode="Batch Annual Release", expected_refresh_interval_sec=86400 * 365),
        variety=VarietyIndicator(column_count=26, file_format="DICOM & JPG", schema_definition={"dicom_id": "string", "subject_id": "int32", "study_id": "int32", "ViewPosition": "string"}),
        veracity=VeracityIndicator(source_origin="Beth Israel Deaconess Hospital RIS/PACS", quality_score=0.99),
        variability=VariabilityIndicator(schema_version="MIMIC-CXR v2.0"),
        value=ValueIndicator(business_criticality=BusinessCriticality.TIER_1, cost_per_query=0.25, sla_tier="Gold Research SLA"),
        vulnerability=VulnerabilityIndicator(
            classification=VulnerabilityClassification.RESTRICTED,
            access_control_policy="PhysioNet Credentialed Tier-3 (DICOM Headers scrubbed of burnt-in PHI text)",
            authorized_roles=["credentialed_researcher"],
        ),
    )

    # MIMIC ECG (Waveforms)
    pilot.bind_dataset_tv_words(
        dataset_id="ds_mimic_ecg",
        volume=VolumeIndicator(byte_size=750_000_000_000, record_count=800_000),
        velocity=VelocityIndicator(ingestion_mode="Batch Annual Release", expected_refresh_interval_sec=86400 * 365),
        variety=VarietyIndicator(column_count=12, file_format="WFDB Multi-Lead Binary", schema_definition={"record_name": "string", "subject_id": "int32", "sampling_frequency": "int32"}),
        veracity=VeracityIndicator(source_origin="Phillips / GE Healthcare Telemetry Bedside Waveform Systems", quality_score=0.98),
        variability=VariabilityIndicator(schema_version="MIMIC-IV-ECG v1.0"),
        value=ValueIndicator(business_criticality=BusinessCriticality.TIER_1, cost_per_query=0.10, sla_tier="Gold Research SLA"),
        vulnerability=VulnerabilityIndicator(
            classification=VulnerabilityClassification.RESTRICTED,
            access_control_policy="PhysioNet Credentialed Tier-3 Access",
            authorized_roles=["credentialed_researcher"],
        ),
    )

    # Root Complex MIMIC Dataset
    pilot.bind_dataset_tv_words(
        dataset_id="ds_mimic_root",
        volume=VolumeIndicator(byte_size=0, record_count=0), # Computed in Stage C
        velocity=VelocityIndicator(ingestion_mode="Federated Release", expected_refresh_interval_sec=86400 * 365),
        variety=VarietyIndicator(column_count=0, file_format="Multimodal Lakehouse (Parquet/DICOM/WFDB)"),
        veracity=VeracityIndicator(source_origin="Beth Israel Deaconess Medical Center & MIT LCP", quality_score=0.985),
        variability=VariabilityIndicator(schema_version="MIMIC Multimodal v4.0"),
        value=ValueIndicator(business_criticality=BusinessCriticality.TIER_1, cost_per_query=0.50, sla_tier="Gold Consortium"),
        vulnerability=VulnerabilityIndicator(
            classification=VulnerabilityClassification.RESTRICTED,
            access_control_policy="PhysioNet Master Data Use Agreement (DUA) with Institutional Review Board (IRB) Protocol",
            authorized_roles=["credentialed_researcher", "data_steward"],
        ),
    )

    # Remaining datasets (MICU, SICU, TSICU, Abstract, Raw)
    for ds_id in ["ds_abstract_icu", "ds_micu", "ds_sicu", "ds_tsicu", "ds_hospital_ehr_raw", "ds_icu_bedside_raw", "ds_ehr_unstructured_raw", "ds_pacs_imaging_raw", "ds_bedside_telemetry_raw"]:
        ds = pilot.datasets[ds_id]
        val_ind = None
        if ds.lifecycle_stage == LifecycleStage.PROCESSED:
            val_ind = ValueIndicator(business_criticality=BusinessCriticality.TIER_2, sla_tier="Silver")
        pilot.bind_dataset_tv_words(
            dataset_id=ds_id,
            volume=VolumeIndicator(byte_size=5_000_000, record_count=10_000),
            velocity=VelocityIndicator(expected_refresh_interval_sec=86400 * 30),
            variety=VarietyIndicator(column_count=9, file_format="Specialized / Raw"),
            veracity=VeracityIndicator(source_origin="Clinical Source System", quality_score=0.98),
            variability=VariabilityIndicator(schema_version="v1.0"),
            value=val_ind,
            vulnerability=VulnerabilityIndicator(
                classification=VulnerabilityClassification.RESTRICTED if "raw" in ds_id else VulnerabilityClassification.INTERNAL,
                access_control_policy="Hospital Internal IRB Protocol",
                authorized_roles=["clinical_researcher"],
            ),
        )

    # 3. Bind InteractsWith Vulnerabilities
    for u, v, data in pilot.graph.edges(data=True):
        if data.get("relation_type") == "InteractsWith":
            pilot.bind_interacts_with_vulnerability(
                stakeholder_id=u,
                dataset_id=v,
                vulnerability=VulnerabilityIndicator(
                    classification=VulnerabilityClassification.RESTRICTED,
                    access_control_policy=f"PhysioNet DUA clearance for stakeholder '{u}' accessing '{v}'",
                    authorized_roles=[pilot.stakeholders[u].role],
                ),
            )

    # =====================================================================
    # STAGE C: Recursive Flattening & Registration
    # =====================================================================
    pilot.register_catalog()
    return repo
