import os
import glob
import time
import networkx as nx
import pyarrow.parquet as pq
from typing import Dict, List, Any

class AutomatedMeDOMBuilder:
    def __init__(self, root_dir: str, default_stakeholder: str = "stk_data_engineer"):
        self.root_dir = root_dir
        self.default_stakeholder = default_stakeholder
        self.dag = nx.DiGraph()
        self.data_objects: Dict[str, Any] = {}
        self.datasets: Dict[str, Any] = {}
        self.relationships: List[Dict[str, Any]] = []

    # -------------------------------------------------------------
    # Stage 1: Fast Parquet Extraction (OVolume & OVariety)
    # -------------------------------------------------------------
    def extract_parquet_object(self, file_path: str) -> Dict[str, Any]:
        """Extracts OVolume and OVariety directly from Parquet metadata footers."""
        meta = pq.read_metadata(file_path)
        schema = pq.read_schema(file_path)
        obj_name = os.path.basename(file_path)
        file_stat = os.stat(file_path)

        # Logical & physical metrics without memory overhead
        num_rows = meta.num_rows
        num_cols = meta.num_columns
        physical_bytes = file_stat.st_size
        modified_time = time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime(file_stat.st_mtime))

        columns_meta = {}
        for col_name in schema.names:
            col_type = str(schema.field(col_name).type)
            columns_meta[col_name] = {"data_type": col_type}

        return {
            "id": f"do_{obj_name}",
            "name": obj_name,
            "path": file_path,
            "OVolume": {
                "logical_record_count": num_rows,
                "column_count": num_cols,
                "physical_size_bytes": physical_bytes,
                "timestamp": modified_time
            },
            "OVariety": {
                "nature": "Structured",
                "physical_rep": "Parquet",
                "columns": columns_meta,
                "timestamp": modified_time
            },
            "OVariability": {
                "last_modified": modified_time
            }
        }

    # -------------------------------------------------------------
    # Stage 2: DLD Assembly & Dataset Ingestion
    # -------------------------------------------------------------
    def build_dld_hierarchy(self):
        """Discovers folders as datasets and parquet files as data objects."""
        parquet_files = glob.glob(os.path.join(self.root_dir, "**/*.parquet"), recursive=True)
        
        for file_path in parquet_files:
            data_obj = self.extract_parquet_object(file_path)
            self.data_objects[data_obj["id"]] = data_obj

            # Map directory structure to datasets
            parent_dir = os.path.dirname(file_path)
            ds_name = os.path.basename(parent_dir) if parent_dir != self.root_dir else "root_dataset"
            ds_id = f"ds_{ds_name}"

            if ds_id not in self.datasets:
                self.datasets[ds_id] = {
                    "id": ds_id,
                    "name": ds_name,
                    "structure_type": "Simple",
                    "lifecycle_stage": "Raw",
                    "stakeholders": [self.default_stakeholder],
                    "data_objects": [],
                    "assembly_children": [],
                    "processed_from": []
                }
            self.datasets[ds_id]["data_objects"].append(data_obj)

    # -------------------------------------------------------------
    # Stage 3: Relationship Discovery & DAG Construction
    # -------------------------------------------------------------
    def discover_relationships_and_build_dag(self):
        """Identifies join keys, establishes Relates to / Processed from, and constructs the DAG."""
        ds_list = list(self.datasets.values())

        # Add dataset nodes to the DAG
        for ds in ds_list:
            self.dag.add_node(
                ds["id"],
                name=ds["name"],
                type="DataSet",
                structure_type=ds["structure_type"]
            )

        # Cross-dataset schema discovery for relationships
        for i in range(len(ds_list)):
            for j in range(i + 1, len(ds_list)):
                ds1, ds2 = ds_list[i], ds_list[j]

                cols1 = {c for do in ds1["data_objects"] for c in do["OVariety"]["columns"].keys()}
                cols2 = {c for do in ds2["data_objects"] for c in do["OVariety"]["columns"].keys()}
                common_keys = cols1.intersection(cols2)

                for key in common_keys:
                    # Key-based heuristic: shared identifier indicates a referential / join relationship
                    if key.endswith(("_id", "Id", "ID", "key", "pk")):
                        edge_data = {
                            "type": "Relates_to",
                            "relationship_nature": "Referential",
                            "join_key": key
                        }
                        self.relationships.append(edge_data)
                        
                        # Add directional edge to DAG (e.g., from primary master dataset to dependent)
                        self.dag.add_edge(ds1["id"], ds2["id"], **edge_data)

    # -------------------------------------------------------------
    # Stage 4: Synthesize TV-Words (TMD Objects)
    # -------------------------------------------------------------
    def synthesize_all_tmd_objects(self):
        """Synthesizes dataset-level indicators based on Section 6.1 rules."""
        for ds_id, ds in self.datasets.items():
            total_bytes = sum(do["OVolume"]["physical_size_bytes"] for do in ds["data_objects"])
            total_records = sum(do["OVolume"]["logical_record_count"] for do in ds["data_objects"])
            latest_ts = max(do["OVariability"]["last_modified"] for do in ds["data_objects"]) if ds["data_objects"] else time.strftime("%Y-%m-%d %H:%M:%SZ")

            ds["TMD_Object"] = {
                "TVolume": {
                    "logical_records": total_records,
                    "physical_bytes": total_bytes,
                    "timestamp": latest_ts
                },
                "TVelocity": {
                    "speed": "Fixed",  # Parquet static partition snapshot
                    "timestamp": latest_ts
                },
                "TVariety": {
                    "nature": "Structured",
                    "physical_rep": "Parquet",
                    "object_count": len(ds["data_objects"]),
                    "timestamp": latest_ts
                },
                "TVeracity": {
                    "source": f"Local Ingest ({self.root_dir})",
                    "timestamp": latest_ts
                },
                "TVulnerability": {
                    "protection": "File System ACL",
                    "access_level": "Restricted",
                    "timestamp": latest_ts
                }
            }

    # -------------------------------------------------------------
    # Run Complete Pipeline
    # -------------------------------------------------------------
    def run(self) -> Dict[str, Any]:
        self.build_dld_hierarchy()
        self.discover_relationships_and_build_dag()
        self.synthesize_all_tmd_objects()

        # Invariant verification: Verify the graph is an acyclic DAG
        is_dag = nx.is_directed_acyclic_graph(self.dag)

        return {
            "success": True,
            "is_valid_dag": is_dag,
            "datasets_found": len(self.datasets),
            "data_objects_found": len(self.data_objects),
            "relationships_discovered": len(self.relationships),
            "topological_sort_order": list(nx.topological_sort(self.dag)) if is_dag else "Graph contains cycles"
        }