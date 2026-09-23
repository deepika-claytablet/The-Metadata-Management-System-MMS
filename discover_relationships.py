from typing import List, Dict, Any, Optional

def discover_relates_to(datasets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Discovers candidate 'Relates to' edges by identifying shared key identifiers 
    and temporal alignment across datasets. Returns suggested candidate edges.
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

    for i in range(len(datasets)):
        for j in range(i + 1, len(datasets)):
            ds1 = datasets[i]
            ds2 = datasets[j]
            ds1_id = ds1.get("id") or ds1.get("name")
            ds2_id = ds2.get("id") or ds2.get("name")
            ds1_name = ds1.get("name") or ds1_id
            ds2_name = ds2.get("name") or ds2_id

            cols1 = get_columns_for_dataset(ds1)
            cols2 = get_columns_for_dataset(ds2)

            common_keys = set(cols1.keys()).intersection(set(cols2.keys()))
            if not common_keys:
                continue

            # 1. Identify Candidate Referential Join Keys
            join_keys = []
            for key in sorted(common_keys):
                lower_k = key.lower()
                if (
                    lower_k.endswith(("_id", "id", "_key", "key", "_no", "code", "pk", "fk"))
                    or lower_k in ["id", "uuid", "guid", "code"]
                ):
                    join_keys.append(key)

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
                        "join_condition": " AND ".join(f"{ds1_name}.{k} = {ds2_name}.{k}" for k in join_keys),
                    },
                })

            # 2. Identify Candidate Temporal Alignment
            temporal_keys = []
            for key in sorted(common_keys):
                lower_k = key.lower()
                if any(t in lower_k for t in ["date", "time", "timestamp", "year", "month", "_at"]):
                    temporal_keys.append(key)

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