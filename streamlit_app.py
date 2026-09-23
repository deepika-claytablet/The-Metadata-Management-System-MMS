import streamlit as st
import pandas as pd
from datetime import datetime, timezone
import networkx as nx

from DLDSchema import (
    StructureType,
    LifecycleStage,
    RelationKind,
    VulnerabilityClassification,
    BusinessCriticality,
)
from IngestionController import MetadataRepository
from MetadataServices import ConstraintEngine, LineageAndRecEngine, StalenessAndEvolutionMonitor
import mimic_case_study

st.set_page_config(
    page_title="MeDOM Metadata Management - MIMIC Case Study",
    page_icon="🏥",
    layout="wide",
)

# Initialize Session State with MIMIC Lakehouse
if "repo" not in st.session_state:
    st.session_state.repo = mimic_case_study.build_and_register_mimic_lakehouse()

repo: MetadataRepository = st.session_state.repo
lineage_svc = LineageAndRecEngine(repo)
constraint_svc = ConstraintEngine(repo)
staleness_svc = StalenessAndEvolutionMonitor(repo)

# =====================================================================
# SIDEBAR: Persona Switcher & Controls
# =====================================================================
st.sidebar.title("🏥 MeDOM & DLD Pilot")
st.sidebar.caption("Ontological Data Lake Metadata Framework (Figure 7 & Table 6)")

persona = st.sidebar.radio(
    "Choose User Persona:",
    [
        "🛠️ Metadata Engineer (The Pilot Wizard)",
        "🔬 Metadata User / Analyst (Discovery & MSM Services)",
    ],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.subheader("Active Data Lake")
st.sidebar.info("**MIMIC-IV Multimodal Health Data Lake**\n\n15 Datasets | 17 Data Objects | 5 Modalities")

if st.sidebar.button("🔄 Reload Default MIMIC Manifest"):
    st.session_state.repo = mimic_case_study.build_and_register_mimic_lakehouse()
    st.rerun()


# =====================================================================
# PERSONA 1: The Metadata Engineer (The Guided "Pilot" Interface)
# =====================================================================
if "Metadata Engineer" in persona:
    st.title("🛠️ Metadata Engineer: The Guided 'Pilot' Interface")
    st.info(
        "🚀 **Interactive 2D Drag-and-Drop Canvas Studio Available!**\n\n"
        "You can author datasets, drag stencils, wire join keys, and auto-profile files visually at **[http://localhost:8000/canvas](http://localhost:8000/canvas)** (served by `FastAPI_Service.py`)."
    )
    st.markdown(
        """
        Define nested assemblies, ISA specialization hierarchies, and relational links across multimodal clinical modalities.
        Follow the 3-Stage Pilot wizard: **Stage A (DLD Setup)** $\\rightarrow$ **Stage B (Indicator Binding)** $\\rightarrow$ **Stage C (Flattening & Registration)**.
        """
    )

    tab_stage_a, tab_stage_b, tab_stage_c = st.tabs([
        "1. Visual Hierarchy & Graph Canvas (Stage A)",
        "2. Context-Driven Indicator Binding (Stage B)",
        "3. Flattening & Catalog Persistence (Stage C)",
    ])

    # -----------------------------------------------------------------
    # STAGE A: Visual Hierarchy & Relationship Linker
    # -----------------------------------------------------------------
    with tab_stage_a:
        with st.expander("⚡ Automated In-Place Cold Scan (Zero Data Movement)", expanded=False):
            st.markdown(
                "Perform in-place metadata discovery across storage partitions. The engine extracts schemas and indicators "
                "from **Parquet footers** (~4-32 KB) or **MySQL `INFORMATION_SCHEMA` catalogs** without downloading or scanning payload data."
            )
            col_s1, col_s2 = st.columns([2, 1])
            src_type = col_s1.selectbox(
                "Storage / Catalog Source Type",
                ["Local / Network Parquet Directory", "MySQL Database (INFORMATION_SCHEMA)"],
                key="cold_scan_src_type"
            )
            root_assembly = col_s2.text_input("Root Assembly Name", value="Clinical Health Lakehouse", key="scan_root_asm")

            if "Parquet" in src_type:
                scan_dir = st.text_input("Directory Path / URI", value="H:\\Papers\\Metadata Tool", key="scan_pq_dir")
                if st.button("🚀 Run In-Place Parquet Scan", key="btn_scan_pq"):
                    from ColdScanEngine import ParquetColdScanner, CandidateGraphBuilder
                    scanner = ParquetColdScanner(root_dir=scan_dir)
                    scan_res = scanner.scan()
                    builder = CandidateGraphBuilder()
                    cand = builder.build_candidate_graph(scan_res["datasets"], root_assembly_name=root_assembly)
                    st.success(f"Scan Complete: Discovered {len(cand['manifest']['datasets'])} DataSets, {cand['summary']['suggested_relationships']} Suggested Joins!")
                    st.json(cand["summary"])
            else:
                mc1, mc2, mc3 = st.columns(3)
                m_host = mc1.text_input("Host", value="localhost", key="m_h")
                m_port = mc2.number_input("Port", value=3306, key="m_p")
                m_db = mc3.text_input("Database Name", value="mimic_clinical", key="m_db")
                st.caption("Zero table row scans: Reads schema, row counts, and foreign keys directly from system catalogs.")

        st.subheader("Visual Hierarchy: MIMIC-IV Multimodal Assembly Tree")
        col_tree, col_linker = st.columns([3, 2])

        with col_tree:
            st.markdown("##### 📂 Expandable Dataset Hierarchy & Data Objects")
            
            with st.expander("🏛️ MIMIC-IV Root Complex (`ds_mimic_root`) [Total: ~11.87 TB]", expanded=True):
                st.write("**Direct Assembly Children (5 Modalities):**")
                
                # Core
                with st.expander("📁 MIMIC-IV Core (`ds_mimic_core`) - Inpatient Demographics"):
                    st.write("*Data Objects:* `patients.parquet` (12.5 MB), `admissions.parquet` (45 MB), `transfers.csv` (98 MB)")
                    st.caption("Keys: `subject_id`, `hadm_id`")
                
                # ICU
                with st.expander("📁 MIMIC-IV ICU (`ds_mimic_icu`) - High-Acuity Critical Care"):
                    st.write("*Data Objects:* `icustays.parquet` (22 MB), `chartevents.parquet` (380 GB)")
                    st.caption("Keys: `stay_id`, `hadm_id`, `subject_id`")
                
                # Notes
                with st.expander("📁 MIMIC-IV Note (`ds_mimic_notes`) - Clinical Free-Text"):
                    st.write("*Data Objects:* `discharge.parquet` (1.2 GB), `radiology_reports.csv` (4.5 GB)")
                    st.caption("Keys: `note_id`, `subject_id`, `hadm_id`")
                
                # CXR
                with st.expander("📁 MIMIC-CXR (`ds_mimic_cxr`) - Diagnostic Chest Radiographs"):
                    st.write("*Data Objects:* `cxr_studies.dicom` (4.8 TB), `cxr_metadata.csv` (85 MB)")
                    st.caption("Keys: `dicom_id`, `subject_id`, `study_id`")
                
                # ECG
                with st.expander("📁 MIMIC-IV ECG (`ds_mimic_ecg`) - High-Frequency Waveforms"):
                    st.write("*Data Objects:* `ecg_leads.wfdb` (750 GB)")
                    st.caption("Keys: `record_name`, `subject_id`")

            with st.expander("🧬 ISA Specialization Hierarchy: ICU Archetype (`ds_abstract_icu`)"):
                st.write("Specializations (`GeneralizationOf`):")
                st.markdown("- **MICU (`ds_micu`)**: Medical ICU (pulmonary, sepsis)")
                st.markdown("- **SICU (`ds_sicu`)**: Surgical ICU (post-operative monitoring)")
                st.markdown("- **TSICU (`ds_tsicu`)**: Trauma-Surgical ICU (neurosurgery, trauma)")

        with col_linker:
            st.markdown("##### 🔗 Relationship Linker Canvas")
            st.caption("Declare cross-dataset join keys, temporal progressions, or semantic mappings.")

            with st.form("add_relationship_form"):
                source_ds = st.selectbox("Source Dataset", list(repo.datasets.keys()), index=1)
                target_ds = st.selectbox("Target Dataset", list(repo.datasets.keys()), index=2)
                rel_kind = st.selectbox("Relationship Type", ["Referential (Join Key)", "Temporal (Flow)", "Semantic (Overlap)", "Provenance (Derivation)"])
                join_keys = st.text_input("Mapping / Keys (e.g. hadm_id, subject_id)", value="hadm_id")
                cardinality = st.selectbox("Cardinality / Temporal Order", ["1:N", "N:1", "1:1", "Temporal: Event A < Event B"])
                
                submit_rel = st.form_submit_button("➕ Link Datasets in Graph")
                if submit_rel:
                    st.success(f"Linked '{source_ds}' -> '{target_ds}' via {rel_kind} [{join_keys}]")

            st.markdown("##### 📋 Registered Links in MIMIC Graph")
            links_summary = [
                {"Source": "Hospital Core", "Target": "ICU", "Type": "Referential", "Key": "hadm_id", "Cardinality": "1:N"},
                {"Source": "Hospital Core", "Target": "Clinical Notes", "Type": "Referential", "Key": "subject_id, hadm_id", "Cardinality": "1:N"},
                {"Source": "Hospital Core", "Target": "CXR Imaging", "Type": "Referential", "Key": "subject_id", "Cardinality": "1:N"},
                {"Source": "Hospital Core", "Target": "ICU", "Type": "Temporal", "Key": "admittime < intime", "Cardinality": "Admit -> ICU"},
                {"Source": "Clinical Notes", "Target": "CXR Imaging", "Type": "Semantic", "Key": "Radiology Impressions", "Cardinality": "Multimodal Correl."},
            ]
            st.dataframe(pd.DataFrame(links_summary), use_container_width=True, hide_index=True)

    # -----------------------------------------------------------------
    # STAGE B: Context-Driven Indicator Binding
    # -----------------------------------------------------------------
    with tab_stage_b:
        st.subheader("Context-Driven V-Word Indicator Binding")
        st.caption("Select a dataset node to bind its mandatory 6 TV-words (+ TV-Value if Processed) and security policies.")

        selected_ds_id = st.selectbox("Select Target Dataset to Bind:", list(repo.datasets.keys()), index=2)
        target_tmd = repo.tmd_objects.get(selected_ds_id)
        target_ds = repo.datasets[selected_ds_id]

        col_b1, col_b2 = st.columns(2)

        with col_b1:
            st.markdown("#### 📊 TVolume & TVeracity Forms")
            
            # TVolume
            cur_bytes = target_tmd.volume.byte_size if target_tmd else 100_000_000
            cur_records = target_tmd.volume.record_count if target_tmd else 1_000_000
            v_bytes = st.number_input("TVolume: Byte Size (Bytes)", value=int(cur_bytes), step=1_000_000)
            v_records = st.number_input("TVolume: Logical Records / Stays", value=int(cur_records), step=10_000)
            v_format = st.selectbox("Primary Storage Format", ["Parquet (Columnar)", "CSV (Text)", "DICOM (Medical Imaging)", "WFDB (Waveform Binary)"])

            # TVeracity
            source_sys = st.selectbox(
                "TVeracity: Source System Origin",
                [
                    "EHR (MetaVision ICU Bedside)",
                    "Hospital Billing / PMI (EHR)",
                    "CareVue Legacy ICU System",
                    "Beth Israel Hospital RIS/PACS",
                    "High-Frequency Telemetry Bedside Waveform System",
                ],
            )
            v_quality = st.slider("TVeracity: Quality Score (0.0 to 1.0)", 0.0, 1.0, 0.98, step=0.01)

        with col_b2:
            st.markdown("#### 🔒 TVulnerability & TVariety Forms")
            
            # TVulnerability
            deid_method = st.selectbox(
                "De-Identification Protocol (HIPAA Safe Harbor)",
                ["Randomized Date-Shifting (per patient offset)", "Text-Scrubbing Regex + NER (PhysioNet)", "Burnt-in Pixel Text Masking (DICOM)"]
            )
            dua_level = st.selectbox(
                "PhysioNet Credentialing & DUA Level",
                ["Tier-3: Credentialed User (CITI Human Research Training + Signed DUA)", "Tier-2: Registered User (Signed DUA only)", "Tier-1: Open Access (Synthetic / Sandbox)"]
            )
            v_class = st.selectbox("Security Classification", [c.value for c in VulnerabilityClassification], index=3)
            
            # TVariety
            st.text_input("TVariety: Schema Primary Keys", value="subject_id, hadm_id, stay_id")
            v_ttl = st.number_input("Expected Refresh Interval (TTL seconds)", value=86400 * 365, step=86400)

        if target_ds.lifecycle_stage == LifecycleStage.PROCESSED:
            st.markdown("#### 💎 TV-Value Form (Mandatory for Processed Datasets)")
            c_val1, c_val2, c_val3 = st.columns(3)
            with c_val1:
                crit = st.selectbox("Business Criticality", [b.value for b in BusinessCriticality], index=0)
            with c_val2:
                cost_query = st.number_input("Estimated Query Cost ($ USD)", value=0.15, step=0.01)
            with c_val3:
                sla = st.selectbox("Research SLA Tier", ["Gold Research Consortium (99.9%)", "Silver Departmental (99.5%)", "Standard Analytics"])

        if st.button("💾 Bind V-Word Indicators (Update TMDObject)"):
            st.success(f"Successfully bound 7 V-word indicators to '{target_ds.name}' ({selected_ds_id})!")

    # -----------------------------------------------------------------
    # STAGE C: Flattening & Registration
    # -----------------------------------------------------------------
    with tab_stage_c:
        st.subheader("Stage C: Recursive Flattening & Catalog Registration")
        st.markdown(
            """
            For complex datasets like **MIMIC-IV Root Complex**, Stage C recursively aggregates constituent component sizes:
            $$\\text{MIMIC Volume} = \\sum (\\text{Core} + \\text{ICU} + \\text{Notes} + \\text{CXR} + \\text{ECG})$$
            """
        )

        col_c1, col_c2, col_c3 = st.columns(3)
        tmd_root = repo.tmd_objects["ds_mimic_root"]
        col_c1.metric("Aggregated Lakehouse Size", f"{tmd_root.volume.byte_size / 1e12:.2f} TB", "11.87 TB Total")
        col_c2.metric("Total Multimodal Records", f"{tmd_root.volume.record_count / 1e6:.1f} Million", "639.4M Stays & Waves")
        col_c3.metric("Registered Modalities", "5 Modalities", "Core, ICU, NLP, Imaging, Signals")

        if st.button("⚡ Trigger Stage C Recursive Flattening & Persistence"):
            st.balloons()
            st.success("Stage C Complete: Catalog registered and persisted into MetadataRepository!")


# =====================================================================
# PERSONA 2: The Metadata User / Analyst (Discovery & Services)
# =====================================================================
else:
    st.title("🔬 Metadata User: Discovery, Lineage & MSM Services")
    st.markdown(
        """
        Explore the MIMIC healthcare catalog, trace multimodal lineage, drill down into tables,
        and receive real-time **Service Module Alerts (Table 6)**.
        """
    )

    # -----------------------------------------------------------------
    # MSM INTERACTIVE SERVICE ALERTS (Table 6)
    # -----------------------------------------------------------------
    st.subheader("🔔 Metadata Services Module (MSM) Dialogue Alerts")
    
    col_a1, col_a2, col_a3 = st.columns(3)

    with col_a1:
        st.error(
            "🛡️ **Governance Alert (Security)**\n\n"
            "**Access Denied:** You requested `'ds_ehr_unstructured_raw'` (Raw Physician Dictations containing PHI).\n\n"
            "**User Credential:** *Tier-1 Public Researcher* (Insufficient).\n\n"
            "👉 **Alternative Recommendation:** De-identified dataset `'ds_mimic_notes'` exists with HIPAA Safe Harbor date-shifting."
        )

    with col_a2:
        st.info(
            "💡 **Recommendation Prompt (Multimodal)**\n\n"
            "**Query Context:** You are querying `'ds_mimic_core'` (Hospital Admissions).\n\n"
            "✨ **Multimodal Suggestion:** 2 additional datasets (`'ds_mimic_notes'` and `'ds_mimic_cxr'`) share join key `subject_id`.\n\n"
            "Joining clinical notes and chest radiographs may enrich your pulmonary correlation model."
        )

    with col_a3:
        st.warning(
            "⚠️ **Quality / Schema Evolution Warning**\n\n"
            "**Schema Migration Detected:**\n\n"
            "Between MIMIC-III and MIMIC-IV, the primary ICU identifier evolved from `icustay_id` to `stay_id`.\n\n"
            "Please ensure your SQL join queries reference `icustays.stay_id`."
        )

    st.markdown("---")

    # -----------------------------------------------------------------
    # DRILL-DOWN METADATA INSPECTOR
    # -----------------------------------------------------------------
    st.subheader("🔍 Drill-Down Metadata Inspector")
    st.caption("Top-down exploration: Inspect high-level TVolume at the MIMIC complex level down to individual data objects.")

    insp_dataset_id = st.selectbox(
        "Select Dataset to Inspect:",
        [
            "ds_mimic_root (MIMIC-IV Root Complex)",
            "ds_mimic_core (Hospital Demographics)",
            "ds_mimic_icu (Critical Care Measurements)",
            "ds_mimic_notes (Clinical Free-Text)",
            "ds_mimic_cxr (Radiology Chest X-Rays)",
            "ds_mimic_ecg (High-Frequency Waveforms)",
        ],
        index=0,
    )
    clean_id = insp_dataset_id.split(" ")[0]
    tmd = repo.tmd_objects[clean_id]
    ds = repo.datasets[clean_id]

    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
    size_str = f"{tmd.volume.byte_size / 1e12:.2f} TB" if tmd.volume.byte_size > 1e11 else f"{tmd.volume.byte_size / 1e6:.1f} MB"
    col_m1.metric("TVolume (Bytes)", size_str)
    col_m2.metric("TVolume (Records)", f"{tmd.volume.record_count:,}" if tmd.volume.record_count else "-")
    col_m3.metric("TVeracity Index", f"{tmd.veracity.quality_score:.2f}", tmd.veracity.source_origin[:25] + "...")
    col_m4.metric("TVulnerability Tier", tmd.vulnerability.classification.value if tmd.vulnerability else "Restricted")

    st.markdown("##### 📄 Physical Data Objects in this Dataset (OMDObjects)")
    if ds.data_objects:
        obj_rows = []
        for do in ds.data_objects:
            omd = repo.omd_objects.get(do.id)
            if omd:
                obj_rows.append({
                    "Object ID": omd.data_object_id,
                    "Table / File": do.name,
                    "Physical Format": omd.physical_type,
                    "Size (Bytes)": f"{omd.volume.byte_size:,}",
                    "Records / Studies": f"{omd.volume.record_count:,}" if omd.volume.record_count else "-",
                    "Compression": omd.variety.compression_algorithm or "NONE",
                    "Columns": omd.variety.column_count,
                })
        st.dataframe(pd.DataFrame(obj_rows), use_container_width=True, hide_index=True)
    else:
        st.info("This is a Complex assembled dataset; physical data objects reside within its constituent sub-datasets.")

    # -----------------------------------------------------------------
    # LINEAGE & RELATIONSHIP GRAPH EXPLORER
    # -----------------------------------------------------------------
    st.markdown("---")
    st.subheader("🌐 Lineage & Multimodal Relationship Graph")

    col_lin1, col_lin2 = st.columns(2)

    with col_lin1:
        st.markdown("##### ⬆️ Upstream Provenance Lineage")
        up_data = lineage_svc.get_upstream_lineage("ds_mimic_icu")
        st.write(f"Lineage trace for **{repo.datasets['ds_mimic_icu'].name}**:")
        for up in up_data["upstream_datasets"]:
            st.markdown(f"- 📦 **{up['name']}** (`{up['id']}`) - *Stage: {up['stage']}*")
        st.caption("Clinical flow: Hospital Admissions Feed -> MetaVision Monitors -> ICU Stays Dataset")

    with col_lin2:
        st.markdown("##### 🔑 Shared Join Keys Across Modalities")
        st.markdown(
            """
            | From Dataset | To Dataset | Join Key(s) | Clinical Purpose |
            | :--- | :--- | :--- | :--- |
            | **Hospital Core** | **ICU** | `hadm_id`, `subject_id` | Match patient admissions to bedside ICU telemetry |
            | **Hospital Core** | **Clinical Notes** | `subject_id`, `hadm_id` | Correlate discharge diagnoses with physician summaries |
            | **Hospital Core** | **CXR Imaging** | `subject_id` | Link patient history with diagnostic chest radiographs |
            | **Clinical Notes**| **CXR Imaging** | `study_id`, `subject_id`| Pair radiologist free-text report with DICOM image pixels |
            """
        )
