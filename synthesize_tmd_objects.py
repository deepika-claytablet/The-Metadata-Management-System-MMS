from typing import Dict, List, Any

def synthesize_tmd_object(dataset_id: str, dld_graph: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively synthesizes TV-words for Complex datasets from their simple children.
    Implements the assembly aggregation rules defined in Section 6.1.
    """
    ds = dld_graph["datasets"][dataset_id]
    
    # Base Case: Simple Dataset (aggregate directly from its Data Objects)
    if ds["structure_type"] == "Simple":
        total_bytes = sum(do["OVolume"]["physical_size_bytes"] for do in ds["data_objects"])
        total_records = sum(do["OVolume"].get("logical_record_count", 0) for do in ds["data_objects"])
        
        return {
            "TVolume": {
                "logical_records": total_records,
                "physical_bytes": total_bytes,
                "timestamp": ds["t_words"].get("timestamp")
            },
            "TVelocity": ds["t_words"].get("TVelocity"),
            "TVariety": ds["t_words"].get("TVariety"),
            "TVeracity": ds["t_words"].get("TVeracity")
        }

    # Recursive Case: Complex Dataset
    aggregated_bytes = 0
    aggregated_records = 0
    velocity_vector = []
    veracity_sources = []
    component_varieties = []
    latest_timestamp = "1970-01-01 00:00:00"

    for child_id in ds["assembly_children"]:
        child_md = synthesize_tmd_object(child_id, dld_graph)
        
        # 1. Volume Rule: sum of component sizes
        aggregated_bytes += child_md["TVolume"]["physical_bytes"]
        aggregated_records += child_md["TVolume"]["logical_records"]
        
        # 2. Velocity Rule: vector of component arrival speeds
        velocity_vector.append({child_id: child_md["TVelocity"]["speed"]})
        
        # 3. Veracity Rule: union of all source systems
        veracity_sources.append({child_id: child_md["TVeracity"]["source"]})
        
        # 4. Variety Rule: if mixed structured + unstructured -> semi-structured
        component_varieties.append(child_md["TVariety"]["nature"])
        
        # 5. Variability Rule: propagate most recent component timestamp
        child_ts = child_md["TVolume"]["timestamp"]
        if child_ts and child_ts > latest_timestamp:
            latest_timestamp = child_ts

    overall_nature = "Structured"
    if any(v == "Unstructured" for v in component_varieties):
        overall_nature = "Semi-structured" if any(v == "Structured" for v in component_varieties) else "Unstructured"

    return {
        "TVolume": {
            "logical_records": aggregated_records,
            "physical_bytes": aggregated_bytes,
            "timestamp": latest_timestamp
        },
        "TVelocity": {
            "speed_vector": velocity_vector,
            "timestamp": latest_timestamp
        },
        "TVariety": {
            "nature": overall_nature,
            "component_count": len(ds["assembly_children"]),
            "timestamp": latest_timestamp
        },
        "TVeracity": {
            "sources": veracity_sources,
            "timestamp": latest_timestamp
        }
    }