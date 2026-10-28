# The Metadata Management System (MMS) — DLD & MeDOM Studio

An end-to-end **Data Lake Description (DLD)** and **3-Tier Metadata Object Model (MeDOM)** platform for designing, discovering, validating, and governing heterogeneous data lakes and multi-database ecosystems.

---

## Overview

The **Metadata Management System (MMS)** bridges formal data lake metadata modeling with automated physical profiling and interactive visual governance. It implements:

1. **Data Lake Description (DLD) Graph** $\mathcal{D} = (V, E, T, \psi)$:
   - **Vertices ($V$)**:
     - **DataSets**: Classified along two orthogonal dimensions — **Structure** (`Simple`, `Complex`, `Abstract`) and **Lifecycle** (`Raw`, `Processed`).
     - **Data Objects (Leaf Storage Units)**: Physical tables, files (`Parquet`, `CSV`, `JSON`, `DICOM`, `WFDB`), or cloud objects (`s3://`, `gs://`, `abfss://`).
     - **Stakeholders**: Human or automated actors interacting with datasets.
   - **Edges ($E$)**: Directed relationships with explicit semantics:
     - `AssemblyOf` — Hierarchical composition of complex datasets and leaf data objects.
     - `ProcessedFrom` — Temporal and derivation lineage across raw and processed datasets.
     - `GeneralizationOf` — Specialization / ISA hierarchies (`Abstract` archetypes).
     - `RelatesTo` — Referential and joinable links across datasets.
     - `InteractsWith` — Stakeholder access and governance bindings.

2. **3-Tier Metadata Object Model (MeDOM)**:
   - **Tier 1 — TMD Objects (`TMDObject`)**: Dataset-level **7 V-Words** Technical Indicator Model (*Volume, Velocity, Variety, Veracity, Variability, Value, Vulnerability*).
   - **Tier 2 — OMD Objects (`OMDObject`)**: Leaf Data Object-level **OV-Words** (*OVolume, OVariety, OVariability*).
   - **Tier 3 — RMD Objects (`RMDObject`)**: Edge-level Relationship Metadata (*RelatesTo*, *InteractsWith* security policies, transformation logic, and candidate confidence scores).

3. **Extensible Technical Indicator Model (TIM) Parameter Trees**:
   - Interactive hierarchical tree builder with dashed visual connectors allowing users to customize and nest sub-parameters under any of the 7 V-Words (TMD) or OMD indicators, or apply presets (such as Paper Figure 4 storage breakdowns).

---

## Key Features

- **Interactive DLD & MeDOM Visual Canvas Studio (`/canvas`)**:
  - Drag-and-drop Cytoscape.js graph editor for DataSets, Data Objects, and Stakeholders.
  - **Interactive TIM Tree Modal**: Click `+` on any DataSet (`TMD Objects`) or Data Object (`OMD Objects`) to open a scrollable hierarchical indicator tree, add nested sub-parameters, and record leaf values.
  - Real-time structural constraint validation and one-click **3-Stage Pilot Commit** (`Stage A: Graph Topology & Cycle Check` $\rightarrow$ `Stage B: Bottom-Up Indicator Synthesis` $\rightarrow$ `Stage C: Catalog Registration`).
- **Multi-Source Cold Scan & Automated Lineage Discovery**:
  - **In-Place File & Cloud Prefix Scanner**: Profiles local directories (`Parquet`, `CSV`, `JSON`) and cloud object store URIs (`s3://`, `gs://`, `abfss://`) without moving data.
  - **Multi-Database MySQL Cold Scanner**: Connects to one or multiple MySQL schemas simultaneously, extracts `OVolume`, `OVariety`, and `OVariability` directly from `information_schema`, discovers cross-database Foreign Keys (`RelatesTo`), and infers temporal derivation edges (`ProcessedFrom`) using schema overlap and creation/update timestamps.
- **Governance, Staleness & Schema Drift Services**:
  - Automated validation of DLD axioms (e.g., `Simple` datasets cannot have `AssemblyOf` children; `Processed` datasets require `ProcessedFrom` lineage and `TValue` indicators).
  - Continuous drift detection comparing physical file/table snapshots against registered catalog metadata.

---

## Repository Structure

| File | Description |
| :--- | :--- |
| [`DLDSchema.py`](DLDSchema.py) | Pydantic v2 models enforcing DLD structural constraints, 7 V-Word indicators, `TMDObject`, `OMDObject`, `RMDObject`, and `dynamic_parameter_tree`. |
| [`IngestionController.py`](IngestionController.py) | 3-Stage Pilot Ingestion Engine (Stage A DAG validation, Stage B bottom-up indicator synthesis, Stage C catalog registration & 3-tier MeDOM export). |
| [`ColdScanEngine.py`](ColdScanEngine.py) | Zero-copy Cold Scan engine for local files, cloud URIs, and single/multi-database MySQL schemas with automated DAG assembly. |
| [`FastAPI_Service.py`](FastAPI_Service.py) | REST API backend serving the Canvas Studio, Cold Scan endpoints, physical file profilers, and catalog query/export routes. |
| [`canvas_app.html`](canvas_app.html) | Interactive DLD & MeDOM Visual Canvas Studio with the extensible 7 V-Words TIM Tree Editor. |
| [`discover_relationships.py`](discover_relationships.py) | Heuristic relationship discovery engine for join keys (`RelatesTo`) and temporal derivation (`ProcessedFrom`). |
| [`MetadataServices.py`](MetadataServices.py) | Governance constraint engine, staleness monitor, and schema drift detector. |
| [`ExtractionHelpers.py`](ExtractionHelpers.py) | Physical file extractors for CSV, Parquet, and JSON storage units. |
| [`mimic_case_study.py`](mimic_case_study.py) | Pre-configured MIMIC-IV clinical lakehouse reference manifest and case study builder. |
| [`streamlit_app.py`](streamlit_app.py) | Auxiliary Streamlit dashboard for tabular catalog inspection. |
| [`CLI_Dashboard.py`](CLI_Dashboard.py) | Command-line interface for batch manifest ingestion and inspection. |
| [`test_metadata_system.py`](test_metadata_system.py) | Unit tests for DLD axioms, 3-stage ingestion, extractors, and drift detection. |
| [`test_cold_scan.py`](test_cold_scan.py) | Unit tests for file/cloud cold scans, multi-database MySQL scanning, and lineage inference. |
| [`test_canvas_studio.py`](test_canvas_studio.py) | End-to-end API tests for Canvas Studio, validation, commit, and 3-tier MeDOM export. |

---

## Installation & Quick Start

### 1. Prerequisites
- Python 3.10+
- Install required packages:

```bash
pip install fastapi uvicorn pydantic pandas pyarrow pyyaml pymysql requests pytest httpx
```

### 2. Launch the Server & Visual Canvas Studio

Start the FastAPI server from the project root:

```bash
uvicorn FastAPI_Service:app --reload
```

Open your browser to:
- **Visual Canvas Studio**: [http://127.0.0.1:8000/canvas](http://127.0.0.1:8000/canvas)
- **Interactive Swagger API Docs**: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

---

## Using the TIM 7 V-Words & OMD Tree Editor

1. Open [http://127.0.0.1:8000/canvas](http://127.0.0.1:8000/canvas).
2. Drag a **DataSet** (rectangle) or **Data Object** (cylinder) onto the canvas, or load the **MIMIC-IV Sample** / run a **Cold Scan**.
3. Click on a **DataSet** node:
   - In the right-hand inspector sidebar, click **`+ Configure 7 V-Words`** inside the **TMD Objects (7 V-Words)** card.
   - The scrollable TIM Tree modal opens with the canonical structures for **Volume, Velocity, Variety, Veracity, Variability, Value, and Vulnerability**.
   - Click **`+ Sub-param`** next to any parameter (e.g., `size` or `timestamp`) to nest custom child parameters connected by dashed tree lines, or enter leaf values directly.
4. Click on a leaf **Data Object** node:
   - Click **`+ Configure OMD Objects`** inside the **OMD Objects (Object Tier)** card to configure **OVolume, OVariety, and OVariability** parameter trees.
5. Click **Apply & Save Indicators**, then click **Commit to Catalog** or **Export Manifest** to inspect the serialized 3-Tier MeDOM JSON.

---

## Running Tests

Run the full automated test suite (25 tests covering DLD schema rules, 3-stage ingestion, cold scanning, multi-database lineage, and canvas endpoints):

```bash
pytest -v
```
