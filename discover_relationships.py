from typing import List, Dict, Any, Optional

def discover_relates_to(datasets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Discovers candidate 'RelatesTo' (Referential & Temporal) and 'ProcessedFrom' (Lineage DAG)
    edges across datasets by identifying shared keys, name stems, and downstream analytical patterns.
    """
    discovered_relationships = []

    def get_columns_for_dataset(ds: Dict[str, Any]) -> Dict[str, str]:
        cols = {}
        for do in ds.get("data_objects", []):
            ovariety = do.get("OVariety") or do.get("variety") or {}
            schema_cols = ovariety.get("columns") or ovariety.get("schema_definition") or do.get("schema_definition") or {}
            if isinstance(schema_cols, dict):
                for col_name, col_meta in schema_cols.items():
                    dtype = col_meta.get("data_type") if isinstance(col_meta, dict) else str(col_meta)
                    cols[col_name] = dtype
            elif isinstance(schema_cols, list):
                for col_name in schema_cols:
                    cols[col_name] = "string"
        return cols

    def is_downstream(ds: Dict[str, Any]) -> bool:
        """Determines if a dataset is likely an analytical downstream table (Processed, Dimension, Mart)."""
        stage = (ds.get("lifecycle_stage") or "").lower()
        if stage == "processed":
            return True
        name = (ds.get("name") or "").lower()
        ds_id = (ds.get("id") or "").lower()
        tags = [str(t).lower() for t in ds.get("tags") or []]
        keywords = ["dim_", "fact_", "summary", "agg_", "mart", "dw", "report", "kpi", "analytics"]
        return any(k in name or k in ds_id or any(k in t for t in tags) for k in keywords)

    for i in range(len(datasets)):
        for j in range(i + 1, len(datasets)):
            ds1 = datasets[i]
            ds2 = datasets[j]

            # Only analyze base / simple datasets, skip assembly containers
            if ds1.get("structure_type") == "Complex" or ds2.get("structure_type") == "Complex":
                continue

            ds1_id = ds1.get("id") or ds1.get("name")
            ds2_id = ds2.get("id") or ds2.get("name")
            ds1_name = ds1.get("name") or ds1_id
            ds2_name = ds2.get("name") or ds2_id

            cols1 = get_columns_for_dataset(ds1)
            cols2 = get_columns_for_dataset(ds2)

            # Compare case-insensitively
            cols1_lower = {k.lower(): k for k in cols1.keys()}
            cols2_lower = {k.lower(): k for k in cols2.keys()}
            common_keys_lower = set(cols1_lower.keys()).intersection(set(cols2_lower.keys()))

            if not common_keys_lower:
                # Even without shared column names, check if table name stems overlap between Raw and Processed
                clean1 = ds1_name.lower().replace("dim_", "").replace("fact_", "").replace("summary_", "")
                clean2 = ds2_name.lower().replace("dim_", "").replace("fact_", "").replace("summary_", "")
                if (clean1 == clean2 or clean1 in clean2 or clean2 in clean1) and (is_downstream(ds1) ^ is_downstream(ds2)):
                    upstream_ds = ds2 if is_downstream(ds1) else ds1
                    downstream_ds = ds1 if is_downstream(ds1) else ds2
                    discovered_relationships.append({
                        "relationship_type": "ProcessedFrom",
                        "source_dataset_id": upstream_ds.get("id") or upstream_ds.get("name"),
                        "target_dataset_id": downstream_ds.get("id") or downstream_ds.get("name"),
                        "status": "Suggested",
                        "confidence": 0.82,
                        "provenance": "heuristic_name_stem_lineage",
                        "attributes": {
                            "lineage_rule": f"Inferred lineage from `{upstream_ds.get('name')}` into analytical `{downstream_ds.get('name')}`",
                            "transformation_type": "ETL Transformation / Aggregation",
                        },
                    })
                continue

            # 1. Identify Candidate Referential Join Keys
            join_keys = []
            for lk in sorted(common_keys_lower):
                original_k1 = cols1_lower[lk]
                if (
                    lk.endswith(("_id", "id", "_key", "key", "_no", "_sk", "sk", "code", "pk", "fk", "_number", "name"))
                    or lk in ["id", "uuid", "guid", "code", "customer_name", "account_number", "product_sk"]
                ):
                    join_keys.append(original_k1)

            if join_keys:
                discovered_relationships.append({
                    "relationship_type": "Referential",
                    "source_dataset_id": ds1_id,
                    "target_dataset_id": ds2_id,
                    "status": "Suggested",
                    "confidence": 0.95,
                    "provenance": "heuristic_identifier_match",
                    "attributes": {
                        "join_keys": join_keys,
                        "cardinality": "1:N",
                        "join_condition": " AND ".join(f"{ds1_name}.{k} = {ds2_name}.{cols2_lower[k.lower()]}" for k in join_keys),
                    },
                })

                # Check for Candidate Lineage (ProcessedFrom DAG)
                down1 = is_downstream(ds1)
                down2 = is_downstream(ds2)
                if down1 ^ down2:  # exactly one is downstream analytical table
                    upstream_ds = ds2 if down1 else ds1
                    downstream_ds = ds1 if down1 else ds2
                    discovered_relationships.append({
                        "relationship_type": "ProcessedFrom",
                        "source_dataset_id": upstream_ds.get("id") or upstream_ds.get("name"),
                        "target_dataset_id": downstream_ds.get("id") or downstream_ds.get("name"),
                        "status": "Suggested",
                        "confidence": 0.88,
                        "provenance": "heuristic_lineage_derivation",
                        "attributes": {
                            "lineage_rule": f"Derived from operational `{upstream_ds.get('name')}` into analytical `{downstream_ds.get('name')}`",
                            "transformation_type": "Dimension / Aggregate Derivation",
                            "join_keys": join_keys,
                        },
                    })

            # 2. Identify Candidate Temporal Alignment
            temporal_keys = []
            for lk in sorted(common_keys_lower):
                if any(t in lk for t in ["date", "time", "timestamp", "year", "month", "_at"]):
                    temporal_keys.append(cols1_lower[lk])

            if temporal_keys and not join_keys:
                discovered_relationships.append({
                    "relationship_type": "Temporal",
                    "source_dataset_id": ds1_id,
                    "target_dataset_id": ds2_id,
                    "status": "Suggested",
                    "confidence": 0.80,
                    "provenance": "heuristic_temporal_token",
                    "attributes": {
                        "temporal_keys": temporal_keys,
                        "alignment": f"Temporal correlation on {', '.join(temporal_keys)}",
                    },
                })

    return discovered_relationships