import os
import glob
import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import pyarrow.parquet as pq

try:
    import pymysql
    HAS_PYMYSQL = True
except ImportError:
    HAS_PYMYSQL = False

from DLDSchema import (
    StructureType,
    LifecycleStage,
    RelationKind,
    CandidateStatus,
    VolumeIndicator,
    VarietyIndicator,
    VariabilityIndicator,
    OMDObject,
    DataObjectInput,
    DataSetInput,
    DatasetRelationshipInput,
    StakeholderInput,
    DLDManifest,
)
from discover_relationships import discover_relates_to


class ParquetColdScanner:
    """
    In-place Parquet file extractor. Reads only metadata footers (~4-32 KB)
    via PyArrow without reading data rows into memory.
    """
    def __init__(self, root_dir: str):
        self.root_dir = os.path.abspath(root_dir)

    def scan(self) -> Dict[str, Any]:
        parquet_files = glob.glob(os.path.join(self.root_dir, "**/*.parquet"), recursive=True)
        parquet_files += glob.glob(os.path.join(self.root_dir, "**/*.pq"), recursive=True)

        if not parquet_files and os.path.isfile(self.root_dir):
            if self.root_dir.endswith((".parquet", ".pq")):
                parquet_files = [self.root_dir]

        datasets_map: Dict[str, Dict[str, Any]] = {}
        data_objects_map: Dict[str, Dict[str, Any]] = {}

        for fpath in parquet_files:
            stat = os.stat(fpath)
            f_size = stat.st_size
            mtime_utc = datetime.fromtimestamp(stat.st_mtime, timezone.utc)

            # PyArrow reads only metadata footer
            pf = pq.ParquetFile(fpath)
            meta = pf.metadata
            schema = pf.schema_arrow

            # Compression codecs & null statistics from row groups
            codecs = set()
            null_counts = {}
            for rg_i in range(meta.num_row_groups):
                rg = meta.row_group(rg_i)
                for col_i in range(rg.num_columns):
                    c = rg.column(col_i)
                    if c.compression:
                        codecs.add(str(c.compression).upper())

            for name in schema.names:
                t_null = 0
                has_null_stat = True
                for rg_i in range(meta.num_row_groups):
                    c_stat = meta.row_group(rg_i).column(schema.names.index(name)).statistics
                    if c_stat and c_stat.has_null_count:
                        t_null += c_stat.null_count
                    else:
                        has_null_stat = False
                        break
                if has_null_stat:
                    null_counts[name] = t_null

            schema_def = {name: str(schema.field(name).type) for name in schema.names}
            codec_str = ", ".join(sorted(codecs)) if codecs else "SNAPPY"

            filename = os.path.basename(fpath)
            parent_dir = os.path.dirname(fpath)
            rel_dir = os.path.relpath(parent_dir, self.root_dir)
            ds_name = os.path.basename(parent_dir) if rel_dir != "." else "root_dataset"
            ds_id = f"ds_{ds_name.replace(' ', '_').lower()}"
            obj_id = f"obj_{os.path.splitext(filename)[0]}_{int(stat.st_mtime)}"

            obj_dict = {
                "id": obj_id,
                "name": filename,
                "physical_type": "Parquet",
                "storage_path": fpath,
                "dataset_id": ds_id,
                "OVolume": {
                    "logical_record_count": meta.num_rows,
                    "column_count": meta.num_columns,
                    "physical_size_bytes": f_size,
                    "partition_count": meta.num_row_groups,
                    "timestamp": mtime_utc.isoformat(),
                },
                "OVariety": {
                    "nature": "Structured",
                    "physical_rep": "Parquet",
                    "schema_definition": schema_def,
                    "columns": schema_def,
                    "compression_algorithm": codec_str,
                    "null_counts": null_counts,
                    "timestamp": mtime_utc.isoformat(),
                },
                "OVariability": {
                    "schema_version": "v1.0",
                    "timestamp": mtime_utc.isoformat(),
                },
            }
            data_objects_map[obj_id] = obj_dict

            if ds_id not in datasets_map:
                datasets_map[ds_id] = {
                    "id": ds_id,
                    "name": ds_name.replace("_", " ").title(),
                    "structure_type": "Simple",
                    "lifecycle_stage": "Raw",
                    "description": f"Auto-discovered dataset from {parent_dir}",
                    "tags": ["parquet", "auto_discovered", ds_name.lower()],
                    "data_objects": [],
                    "assembly_children": [],
                    "generalization_children": [],
                    "processed_from": [],
                }
            datasets_map[ds_id]["data_objects"].append(obj_dict)

        return {
            "datasets": list(datasets_map.values()),
            "data_objects": list(data_objects_map.values()),
        }


class MySQLColdScanner:
    """
    Extracts table schemas, row count estimates, and explicit foreign key relationships
    by querying MySQL INFORMATION_SCHEMA catalogs with zero table payload scans.
    """
    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        database: str = "",
        mock_schema: Optional[Dict[str, Any]] = None,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.mock_schema = mock_schema

    def scan(self) -> Dict[str, Any]:
        if self.mock_schema:
            return self._parse_mock_schema(self.mock_schema)

        if not HAS_PYMYSQL:
            raise ImportError("PyMySQL is required for MySQL scanning. Run `pip install pymysql`.")

        conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            cursorclass=pymysql.cursors.DictCursor,
        )

        try:
            with conn.cursor() as cursor:
                # 1. Query Tables
                cursor.execute(
                    """
                    SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH, CREATE_TIME, UPDATE_TIME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
                    """,
                    (self.database,),
                )
                tables_raw = cursor.fetchall()

                # 2. Query Columns
                cursor.execute(
                    """
                    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s
                    ORDER BY TABLE_NAME, ORDINAL_POSITION
                    """,
                    (self.database,),
                )
                columns_raw = cursor.fetchall()

                # 3. Query Explicit Foreign Keys
                cursor.execute(
                    """
                    SELECT 
                        k.TABLE_NAME, 
                        k.COLUMN_NAME, 
                        k.REFERENCED_TABLE_NAME, 
                        k.REFERENCED_COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
                    JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS c 
                      ON k.CONSTRAINT_NAME = c.CONSTRAINT_NAME 
                     AND k.TABLE_SCHEMA = c.TABLE_SCHEMA
                    WHERE k.TABLE_SCHEMA = %s 
                      AND c.CONSTRAINT_TYPE = 'FOREIGN KEY'
                      AND k.REFERENCED_TABLE_NAME IS NOT NULL
                    """,
                    (self.database,),
                )
                fks_raw = cursor.fetchall()

            return self._assemble_mysql_results(tables_raw, columns_raw, fks_raw)
        finally:
            conn.close()

    def _assemble_mysql_results(
        self,
        tables: List[Dict[str, Any]],
        columns: List[Dict[str, Any]],
        fks: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        # Organize columns by table
        table_cols: Dict[str, Dict[str, str]] = {}
        for col in columns:
            t_name = col["TABLE_NAME"]
            if t_name not in table_cols:
                table_cols[t_name] = {}
            table_cols[t_name][col["COLUMN_NAME"]] = col["DATA_TYPE"]

        datasets_list = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for t in tables:
            t_name = t["TABLE_NAME"]
            rows = t.get("TABLE_ROWS") or 0
            size_bytes = (t.get("DATA_LENGTH") or 0) + (t.get("INDEX_LENGTH") or 0)
            ds_id = f"ds_{t_name.lower()}"
            cols = table_cols.get(t_name, {})

            obj_dict = {
                "id": f"obj_{t_name.lower()}",
                "name": t_name,
                "physical_type": "Relational Table",
                "dataset_id": ds_id,
                "OVolume": {
                    "logical_record_count": rows,
                    "column_count": len(cols),
                    "physical_size_bytes": size_bytes,
                    "partition_count": 1,
                    "timestamp": now_iso,
                },
                "OVariety": {
                    "nature": "Structured",
                    "physical_rep": "MySQL Table",
                    "schema_definition": cols,
                    "columns": cols,
                    "timestamp": now_iso,
                },
                "OVariability": {
                    "schema_version": "v1.0",
                    "timestamp": now_iso,
                },
            }

            datasets_list.append({
                "id": ds_id,
                "name": t_name.replace("_", " ").title(),
                "structure_type": "Simple",
                "lifecycle_stage": "Raw",
                "description": f"MySQL Table {t_name} in database {self.database}",
                "tags": ["mysql", "relational", t_name.lower()],
                "data_objects": [obj_dict],
                "assembly_children": [],
                "generalization_children": [],
                "processed_from": [],
            })

        # Explicit Foreign Key Relationships (100% confidence)
        explicit_relationships = []
        for fk in fks:
            src_id = f"ds_{fk['TABLE_NAME'].lower()}"
            tgt_id = f"ds_{fk['REFERENCED_TABLE_NAME'].lower()}"
            col = fk["COLUMN_NAME"]
            ref_col = fk["REFERENCED_COLUMN_NAME"]

            explicit_relationships.append({
                "relationship_type": "Referential",
                "source_dataset_id": src_id,
                "target_dataset_id": tgt_id,
                "status": "Confirmed",
                "confidence": 1.0,
                "provenance": "mysql_information_schema_fk",
                "attributes": {
                    "join_keys": [col],
                    "referenced_column": ref_col,
                    "cardinality": "N:1",
                    "join_condition": f"{fk['TABLE_NAME']}.{col} = {fk['REFERENCED_TABLE_NAME']}.{ref_col}",
                },
            })

        return {
            "datasets": datasets_list,
            "relationships": explicit_relationships,
        }

    def _parse_mock_schema(self, mock: Dict[str, Any]) -> Dict[str, Any]:
        return self._assemble_mysql_results(
            mock.get("tables", []),
            mock.get("columns", []),
            mock.get("foreign_keys", []),
        )


class CandidateGraphBuilder:
    """
    Builds a Candidate DLD Graph combining scanned datasets, physical data objects,
    explicit foreign keys, and candidate join/temporal heuristics.
    """
    def __init__(self, default_stakeholder: Optional[StakeholderInput] = None):
        self.default_stakeholder = default_stakeholder or StakeholderInput(
            id="stk_data_engineer",
            name="Lead Data Engineer",
            role="Data Architect",
            email="engineer@lakehouse.org",
            department="Data Infrastructure",
        )

    def build_candidate_graph(
        self,
        datasets: List[Dict[str, Any]],
        explicit_relationships: Optional[List[Dict[str, Any]]] = None,
        root_assembly_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        explicit_rels = explicit_relationships or []

        # 1. Run Heuristic Relationship Discovery for candidate joins & temporal links
        heuristic_rels = discover_relates_to(datasets)

        # Merge relationships, prioritizing explicit FKs over heuristics
        existing_pairs = {(r["source_dataset_id"], r["target_dataset_id"]) for r in explicit_rels}
        combined_relationships = list(explicit_rels)

        for h in heuristic_rels:
            pair = (h["source_dataset_id"], h["target_dataset_id"])
            rev_pair = (h["target_dataset_id"], h["source_dataset_id"])
            if pair not in existing_pairs and rev_pair not in existing_pairs:
                combined_relationships.append(h)
                existing_pairs.add(pair)

        # 2. Assign default stakeholder to all datasets if not assigned
        stk_id = self.default_stakeholder.id
        for ds in datasets:
            if not ds.get("interacts_with"):
                ds["interacts_with"] = [stk_id]

        # 3. Optional: Wrap multiple discovered simple datasets under a root Complex Assembly
        final_datasets = list(datasets)
        if root_assembly_name and len(datasets) > 1:
            root_id = f"ds_{root_assembly_name.lower().replace(' ', '_')}"
            child_ids = [d["id"] for d in datasets]
            root_ds = {
                "id": root_id,
                "name": root_assembly_name,
                "structure_type": "Complex",
                "lifecycle_stage": "Raw",
                "description": f"Root Lakehouse Assembly for {root_assembly_name}",
                "tags": ["lakehouse", "root_assembly"],
                "interacts_with": [stk_id],
                "assembly_children": child_ids,
                "generalization_children": [],
                "processed_from": [],
                "data_objects": [],
            }
            final_datasets.insert(0, root_ds)

        # Summary Metrics
        suggested_count = sum(1 for r in combined_relationships if r.get("status") == "Suggested")
        confirmed_count = sum(1 for r in combined_relationships if r.get("status") == "Confirmed")

        return {
            "status": "Candidate DLD Graph Generated",
            "summary": {
                "total_datasets": len(final_datasets),
                "total_relationships": len(combined_relationships),
                "suggested_relationships": suggested_count,
                "confirmed_relationships": confirmed_count,
            },
            "manifest": {
                "stakeholders": [self.default_stakeholder.model_dump()],
                "datasets": final_datasets,
                "relationships": combined_relationships,
            },
        }
