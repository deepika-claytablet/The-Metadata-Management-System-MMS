from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Any, List, Optional, Set
from pydantic import BaseModel
import networkx as nx

from DLDSchema import (
    StructureType,
    LifecycleStage,
    VulnerabilityClassification,
    TMDObject,
    OMDObject,
    RMDObject,
)
from IngestionController import MetadataRepository


class ValidationSeverity(str, Enum):
    CRITICAL = "Critical"
    WARNING = "Warning"
    INFO = "Info"


class ValidationAlert(BaseModel):
    dataset_id: str
    rule_name: str
    severity: ValidationSeverity
    message: str
    suggested_action: str


class ConstraintEngine:
    """
    Validation engine that evaluates MeDOM and DLD integrity rules.
    """
    def __init__(self, repository: MetadataRepository):
        self.repository = repository

    def run_all_checks(self) -> List[ValidationAlert]:
        alerts: List[ValidationAlert] = []
        alerts.extend(self.check_missing_tv_words())
        alerts.extend(self.check_veracity_source())
        alerts.extend(self.check_vulnerability_policies())
        alerts.extend(self.check_processed_value_indicator())
        alerts.extend(self.check_orphaned_datasets())
        return alerts

    def check_missing_tv_words(self) -> List[ValidationAlert]:
        alerts = []
        for ds_id, ds in self.repository.datasets.items():
            if ds_id not in self.repository.tmd_objects:
                alerts.append(ValidationAlert(
                    dataset_id=ds_id,
                    rule_name="MissingTMDObject",
                    severity=ValidationSeverity.CRITICAL,
                    message=f"Dataset '{ds.name}' has no bound TMDObject.",
                    suggested_action="Complete Stage B indicator binding for this dataset."
                ))
            else:
                tmd = self.repository.tmd_objects[ds_id]
                for vword in ["volume", "velocity", "variety", "veracity", "variability"]:
                    if getattr(tmd, vword, None) is None:
                        alerts.append(ValidationAlert(
                            dataset_id=ds_id,
                            rule_name=f"MissingTV_{vword.capitalize()}",
                            severity=ValidationSeverity.CRITICAL,
                            message=f"Dataset '{ds.name}' missing required TV-word: {vword}.",
                            suggested_action=f"Bind {vword} indicator in Stage B."
                        ))
        return alerts

    def check_veracity_source(self) -> List[ValidationAlert]:
        alerts = []
        for ds_id, tmd in self.repository.tmd_objects.items():
            if not tmd.veracity.source_origin or tmd.veracity.source_origin.strip() == "":
                alerts.append(ValidationAlert(
                    dataset_id=ds_id,
                    rule_name="MissingVeracitySourceOrigin",
                    severity=ValidationSeverity.CRITICAL,
                    message=f"TVeracity for dataset '{tmd.dataset_name}' lacks a defined source origin.",
                    suggested_action="Provide provenance/upstream source system name in TVeracity."
                ))
            if tmd.veracity.quality_score < 0.6:
                alerts.append(ValidationAlert(
                    dataset_id=ds_id,
                    rule_name="LowQualityVeracityScore",
                    severity=ValidationSeverity.WARNING,
                    message=f"Dataset '{tmd.dataset_name}' quality score is {tmd.veracity.quality_score:.2f} (< 0.60).",
                    suggested_action="Audit data pipeline quality filters and fix schema errors."
                ))
        return alerts

    def check_vulnerability_policies(self) -> List[ValidationAlert]:
        alerts = []
        for ds_id, tmd in self.repository.tmd_objects.items():
            if tmd.vulnerability is None:
                alerts.append(ValidationAlert(
                    dataset_id=ds_id,
                    rule_name="MissingTVulnerability",
                    severity=ValidationSeverity.WARNING,
                    message=f"Dataset '{tmd.dataset_name}' has no dataset-level Vulnerability indicator.",
                    suggested_action="Define access control policy and security classification."
                ))
            else:
                if not tmd.vulnerability.access_control_policy:
                    alerts.append(ValidationAlert(
                        dataset_id=ds_id,
                        rule_name="MissingAuthorizationPolicy",
                        severity=ValidationSeverity.CRITICAL,
                        message=f"Dataset '{tmd.dataset_name}' has an empty access control policy.",
                        suggested_action="Specify RBAC/ABAC policy for dataset access."
                    ))

        for rel_id, rmd in self.repository.rmd_objects.items():
            if rmd.relationship_type == "InteractsWith":
                if not rmd.vulnerability or not rmd.vulnerability.access_control_policy:
                    alerts.append(ValidationAlert(
                        dataset_id=rmd.target_id,
                        rule_name="MissingInteractionAuthorization",
                        severity=ValidationSeverity.CRITICAL,
                        message=f"Stakeholder interaction '{rel_id}' lacks an authorization policy.",
                        suggested_action="Bind VulnerabilityIndicator to the InteractsWith relationship."
                    ))
        return alerts

    def check_processed_value_indicator(self) -> List[ValidationAlert]:
        alerts = []
        for ds_id, ds in self.repository.datasets.items():
            if ds.lifecycle_stage == LifecycleStage.PROCESSED:
                tmd = self.repository.tmd_objects.get(ds_id)
                if not tmd or tmd.value is None:
                    alerts.append(ValidationAlert(
                        dataset_id=ds_id,
                        rule_name="MissingValueIndicator",
                        severity=ValidationSeverity.CRITICAL,
                        message=f"Processed dataset '{ds.name}' must have a Value indicator (TV-Value).",
                        suggested_action="Bind business criticality, cost per query, and SLA in Stage B."
                    ))
        return alerts

    def check_orphaned_datasets(self) -> List[ValidationAlert]:
        alerts = []
        for ds_id, ds in self.repository.datasets.items():
            if not ds.data_objects and not ds.assembly_children:
                alerts.append(ValidationAlert(
                    dataset_id=ds_id,
                    rule_name="OrphanDatasetNoObjects",
                    severity=ValidationSeverity.WARNING,
                    message=f"Dataset '{ds.name}' has neither physical data objects nor assembly children.",
                    suggested_action="Attach physical DataObjects or child datasets."
                ))
        return alerts


class LineageAndRecEngine:
    """
    Lineage traversal and recommendation engine querying MeDOM graph relationships.
    """
    def __init__(self, repository: MetadataRepository):
        self.repository = repository
        self.graph = repository.graph

    def get_upstream_lineage(self, dataset_id: str) -> Dict[str, Any]:
        if dataset_id not in self.repository.datasets:
            raise KeyError(f"Dataset '{dataset_id}' not found in repository.")

        upstream_nodes = set()
        lineage_edges = []

        # Follow ProcessedFrom edges backward: predecessor -> dataset_id
        def trace_upstream(curr_id: str):
            for predecessor in self.graph.predecessors(curr_id):
                edge_data = self.graph.get_edge_data(predecessor, curr_id)
                if edge_data and edge_data.get("relation_type") == "ProcessedFrom":
                    if predecessor not in upstream_nodes:
                        upstream_nodes.add(predecessor)
                        lineage_edges.append({
                            "from": predecessor,
                            "to": curr_id,
                            "relation": "ProcessedFrom",
                        })
                        trace_upstream(predecessor)

        trace_upstream(dataset_id)

        return {
            "target_dataset_id": dataset_id,
            "upstream_datasets": [
                {
                    "id": uid,
                    "name": self.repository.datasets[uid].name,
                    "stage": self.repository.datasets[uid].lifecycle_stage.value,
                }
                for uid in upstream_nodes
                if uid in self.repository.datasets
            ],
            "edges": lineage_edges,
        }

    def get_downstream_impact(self, dataset_id: str) -> Dict[str, Any]:
        if dataset_id not in self.repository.datasets:
            raise KeyError(f"Dataset '{dataset_id}' not found in repository.")

        downstream_nodes = set()
        impact_edges = []

        def trace_downstream(curr_id: str):
            for successor in self.graph.successors(curr_id):
                edge_data = self.graph.get_edge_data(curr_id, successor)
                if edge_data and edge_data.get("relation_type") == "ProcessedFrom":
                    if successor not in downstream_nodes:
                        downstream_nodes.add(successor)
                        impact_edges.append({
                            "from": curr_id,
                            "to": successor,
                            "relation": "ProcessedFrom",
                        })
                        trace_downstream(successor)

        trace_downstream(dataset_id)

        return {
            "source_dataset_id": dataset_id,
            "blast_radius_count": len(downstream_nodes),
            "impacted_datasets": [
                {
                    "id": did,
                    "name": self.repository.datasets[did].name,
                    "stage": self.repository.datasets[did].lifecycle_stage.value,
                }
                for did in downstream_nodes
                if did in self.repository.datasets
            ],
            "edges": impact_edges,
        }

    def get_similar_datasets(self, dataset_id: str, top_k: int = 5) -> List[Dict[str, Any]]:
        if dataset_id not in self.repository.datasets:
            raise KeyError(f"Dataset '{dataset_id}' not found in repository.")

        target_ds = self.repository.datasets[dataset_id]
        target_tmd = self.repository.tmd_objects.get(dataset_id)
        target_cols = set(target_tmd.variety.schema_definition.keys()) if target_tmd else set()
        target_tags = set(target_ds.tags)

        scored_candidates = []

        for other_id, other_ds in self.repository.datasets.items():
            if other_id == dataset_id:
                continue

            score = 0.0
            reasons = []

            # Check explicit RelatesTo SEMANTIC edge
            if self.graph.has_edge(dataset_id, other_id) or self.graph.has_edge(other_id, dataset_id):
                for u, v in [(dataset_id, other_id), (other_id, dataset_id)]:
                    if self.graph.has_edge(u, v):
                        edge_data = self.graph.get_edge_data(u, v)
                        if edge_data.get("relation_type") == "Semantic":
                            score += 0.5
                            reasons.append("Direct Semantic RelatesTo link")

            # Jaccard similarity of column schemas
            other_tmd = self.repository.tmd_objects.get(other_id)
            if target_cols and other_tmd and other_tmd.variety.schema_definition:
                other_cols = set(other_tmd.variety.schema_definition.keys())
                intersection = target_cols.intersection(other_cols)
                union = target_cols.union(other_cols)
                if union:
                    jaccard = len(intersection) / len(union)
                    if jaccard > 0.1:
                        score += jaccard * 0.4
                        reasons.append(f"Schema overlap ({jaccard * 100:.1f}%)")

            # Tag overlap
            if target_tags and other_ds.tags:
                other_tags = set(other_ds.tags)
                common_tags = target_tags.intersection(other_tags)
                if common_tags:
                    score += 0.2
                    reasons.append(f"Shared tags: {list(common_tags)}")

            if score > 0.0:
                scored_candidates.append({
                    "dataset_id": other_id,
                    "name": other_ds.name,
                    "similarity_score": round(min(1.0, score), 3),
                    "reasons": reasons,
                })

        scored_candidates.sort(key=lambda x: x["similarity_score"], reverse=True)
        return scored_candidates[:top_k]

    def get_alternative_datasets(self, dataset_id: str) -> List[Dict[str, Any]]:
        if dataset_id not in self.repository.datasets:
            raise KeyError(f"Dataset '{dataset_id}' not found in repository.")

        alternatives = []

        # Find abstract parent via GeneralizationOf: abstract_ds -> specialized_ds
        for pred in self.graph.predecessors(dataset_id):
            edge_data = self.graph.get_edge_data(pred, dataset_id)
            if edge_data and edge_data.get("relation_type") == "GeneralizationOf":
                # pred is an Abstract dataset; find all sibling children
                for sibling in self.graph.successors(pred):
                    if sibling != dataset_id and sibling in self.repository.datasets:
                        alternatives.append({
                            "dataset_id": sibling,
                            "name": self.repository.datasets[sibling].name,
                            "reason": f"Sibling specialization under abstract dataset '{self.repository.datasets[pred].name}'",
                        })

        return alternatives


class StalenessAndEvolutionMonitor:
    """
    Monitors dataset freshness against TTL refresh policies and detects schema drift.
    """
    def __init__(self, repository: MetadataRepository):
        self.repository = repository

    def check_staleness(self, as_of: Optional[datetime] = None) -> List[Dict[str, Any]]:
        now = as_of or datetime.now(timezone.utc)
        alerts = []

        for ds_id, tmd in self.repository.tmd_objects.items():
            velocity = tmd.velocity
            if not velocity:
                continue

            ttl_sec = velocity.expected_refresh_interval_sec
            last_ref = velocity.last_refreshed_at

            if last_ref.tzinfo is None:
                last_ref = last_ref.replace(tzinfo=timezone.utc)

            elapsed_sec = (now - last_ref).total_seconds()

            if elapsed_sec > ttl_sec:
                overdue_sec = elapsed_sec - ttl_sec
                alerts.append({
                    "dataset_id": ds_id,
                    "dataset_name": tmd.dataset_name,
                    "status": "STALE, TTL EXCEEDED",
                    "expected_refresh_interval_sec": ttl_sec,
                    "last_refreshed_at": last_ref.isoformat(),
                    "elapsed_sec": round(elapsed_sec, 1),
                    "overdue_sec": round(overdue_sec, 1),
                    "alert_message": (
                        f"Dataset '{tmd.dataset_name}' (ID: {ds_id}) not refreshed within expected cycle: "
                        f"TTL is {ttl_sec}s, last refreshed {int(elapsed_sec)}s ago (overdue by {int(overdue_sec)}s)."
                    ),
                })

        return alerts

    def detect_schema_drift(self, dataset_id: str, current_schema: Dict[str, str]) -> Dict[str, Any]:
        if dataset_id not in self.repository.tmd_objects:
            raise KeyError(f"Dataset '{dataset_id}' not found in repository.")

        tmd = self.repository.tmd_objects[dataset_id]
        baseline_schema = tmd.variety.schema_definition

        added_columns = {k: v for k, v in current_schema.items() if k not in baseline_schema}
        removed_columns = {k: v for k, v in baseline_schema.items() if k not in current_schema}
        type_changes = {
            k: {"from": baseline_schema[k], "to": current_schema[k]}
            for k in baseline_schema
            if k in current_schema and baseline_schema[k] != current_schema[k]
        }

        has_drift = bool(added_columns or removed_columns or type_changes)

        if has_drift:
            tmd.variability.schema_drift_detected = True
            tmd.variability.change_history.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "added_columns": added_columns,
                "removed_columns": removed_columns,
                "type_changes": type_changes,
            })

        return {
            "dataset_id": dataset_id,
            "drift_detected": has_drift,
            "added_columns": added_columns,
            "removed_columns": removed_columns,
            "type_changes": type_changes,
        }
