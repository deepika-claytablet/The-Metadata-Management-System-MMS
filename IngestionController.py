from enum import Enum
from typing import Dict, Any, List, Optional, Set
import networkx as nx

from DLDSchema import (
    DLDManifest,
    StakeholderInput,
    DataSetInput,
    DataObjectInput,
    DatasetRelationshipInput,
    StructureType,
    LifecycleStage,
    VolumeIndicator,
    VelocityIndicator,
    VarietyIndicator,
    VeracityIndicator,
    VariabilityIndicator,
    ValueIndicator,
    VulnerabilityIndicator,
    TMDObject,
    OMDObject,
    RMDObject,
)


class PilotStage(str, Enum):
    INIT = "Init"
    STAGE_A_DLD_SETUP = "Stage A (DLD Setup)"
    STAGE_B_INDICATOR_BINDING = "Stage B (Indicator Binding)"
    STAGE_C_FLATTENING_REGISTRATION = "Stage C (Flattening & Registration)"
    REGISTERED = "Registered / Ready"


class MetadataRepository:
    """
    Catalog persistence layer for registered DLD & MeDOM artifacts.
    """
    def __init__(self):
        self.stakeholders: Dict[str, StakeholderInput] = {}
        self.datasets: Dict[str, DataSetInput] = {}
        self.data_objects: Dict[str, DataObjectInput] = {}
        self.relationships: List[DatasetRelationshipInput] = []
        self.tmd_objects: Dict[str, TMDObject] = {}
        self.omd_objects: Dict[str, OMDObject] = {}
        self.rmd_objects: Dict[str, RMDObject] = {}
        self.graph: nx.DiGraph = nx.DiGraph()

    def persist(
        self,
        stakeholders: Dict[str, StakeholderInput],
        datasets: Dict[str, DataSetInput],
        data_objects: Dict[str, DataObjectInput],
        relationships: List[DatasetRelationshipInput],
        tmd_objects: Dict[str, TMDObject],
        omd_objects: Dict[str, OMDObject],
        rmd_objects: Dict[str, RMDObject],
        graph: nx.DiGraph,
    ):
        self.stakeholders = stakeholders
        self.datasets = datasets
        self.data_objects = data_objects
        self.relationships = relationships
        self.tmd_objects = tmd_objects
        self.omd_objects = omd_objects
        self.rmd_objects = rmd_objects
        self.graph = graph

    def to_dict(self) -> Dict[str, Any]:
        return {
            "datasets": {k: v.model_dump() for k, v in self.datasets.items()},
            "data_objects": {k: v.model_dump() for k, v in self.data_objects.items()},
            "stakeholders": {k: v.model_dump() for k, v in self.stakeholders.items()},
            "tmd_objects": {k: v.model_dump() for k, v in self.tmd_objects.items()},
            "omd_objects": {k: v.model_dump() for k, v in self.omd_objects.items()},
            "rmd_objects": {k: v.model_dump() for k, v in self.rmd_objects.items()},
            "graph_nodes": list(self.graph.nodes),
            "graph_edges": [(u, v, d) for u, v, d in self.graph.edges(data=True)],
        }

    def to_medom_3tier_dict(self, target_dataset_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Exports the catalog into the exact 3-tier MeDOM representation:
        - tmd_object: Dataset-level TV-words
        - omd_objects: Data Object-level OV-words
        - rmd_objects: Relationship & interaction attributes
        """
        ds_id = target_dataset_id or (next(iter(self.datasets.keys())) if self.datasets else "ds_root")
        ds = self.datasets.get(ds_id)
        tmd = self.tmd_objects.get(ds_id)

        tmd_obj = {
            "target_dataset": ds_id,
            "attributes": {
                "TVolume_physical_bytes": (tmd.volume.physical_byte_size or tmd.volume.byte_size) if tmd else 48318382080,
                "TVolume_logical_records": (tmd.volume.logical_record_count or tmd.volume.record_count) if tmd else 380000,
                "TVelocity_speed": tmd.velocity.speed if (tmd and hasattr(tmd.velocity, "speed")) else "Fixed",
                "TVariety_nature": getattr(tmd.variety, "nature", "Structured") if tmd else "Structured",
                "TVariety_ttl": getattr(tmd.variety, "ttl", "Forever") if tmd else "Forever",
                "TVeracity_source": (getattr(tmd.veracity, "source_system_name", None) or tmd.veracity.source_origin) if tmd else "Hospital Enterprise EHR",
                "timestamp": tmd.volume.timestamp.isoformat() if tmd else datetime.now(timezone.utc).isoformat(),
            }
        }
        if tmd and ds and ds.lifecycle_stage == LifecycleStage.PROCESSED and tmd.value:
            tmd_obj["attributes"]["TValue_origin"] = getattr(tmd.value, "origin_dataset", None)
            tmd_obj["attributes"]["TValue_destination"] = getattr(tmd.value, "destination_dataset", None)
            tmd_obj["attributes"]["TValue_transformation"] = getattr(tmd.value, "transformation_action", None)

        omd_list = []
        for obj_id, omd in self.omd_objects.items():
            if target_dataset_id and omd.dataset_id != target_dataset_id:
                continue
            omd_list.append({
                "target_data_object": obj_id,
                "attributes": {
                    "OVolume_byte_size": omd.volume.physical_byte_size or omd.volume.byte_size,
                    "OVolume_row_count": omd.volume.logical_record_count or omd.volume.record_count or 0,
                    "OVolume_column_count": omd.variety.column_count,
                    "OVolume_null_columns": getattr(omd.volume, "null_column_count", 0),
                    "OVariety_schema": omd.variety.schema_definition,
                    "OVariety_physical_rep": getattr(omd.variety, "physical_representation", None) or omd.variety.file_format,
                    "OVariability_last_modified": omd.variability.last_modified_timestamp.isoformat() if getattr(omd.variability, "last_modified_timestamp", None) else omd.variability.timestamp.isoformat(),
                }
            })

        rmd_list = []
        for r_id, rmd in self.rmd_objects.items():
            if rmd.relationship_type == "InteractsWith":
                stk = self.stakeholders.get(rmd.source_id)
                rmd_list.append({
                    "edge_type": "Interacts_with",
                    "target": rmd.target_id,
                    "stakeholder": rmd.source_id,
                    "attributes": {
                        "TVulnerability_protection": getattr(rmd, "data_protection_method", None) or (getattr(rmd.vulnerability, "data_protection_method", None) if rmd.vulnerability else "HIPAA De-identification"),
                        "TVulnerability_access_level": getattr(rmd, "authorization_level", None) or (getattr(rmd.vulnerability, "authorization_level", None) if rmd.vulnerability else "Credentialed"),
                        "timestamp": rmd.vulnerability.timestamp.isoformat() if rmd.vulnerability else datetime.now(timezone.utc).isoformat(),
                    }
                })
            else:
                rmd_list.append({
                    "edge_type": "Relates_to",
                    "source_dataset": rmd.source_id,
                    "target_dataset": rmd.target_id,
                    "attributes": {
                        "relationship_nature": getattr(rmd, "relationship_nature", "Referential"),
                        "join_key": getattr(rmd, "join_key", None) or rmd.attributes.get("join_condition") or "subject_id",
                    }
                })

        if not rmd_list:
            for rel in self.relationships:
                attrs = rel.attributes or {}
                rmd_list.append({
                    "edge_type": "Relates_to",
                    "source_dataset": rel.source_dataset_id,
                    "target_dataset": rel.target_dataset_id,
                    "attributes": {
                        "relationship_nature": attrs.get("relationship_nature", rel.relationship_type.value if hasattr(rel.relationship_type, "value") else str(rel.relationship_type)),
                        "join_key": attrs.get("join_condition") or (attrs.get("join_keys")[0] if attrs.get("join_keys") else "subject_id"),
                    }
                })

        return {
            "tmd_object": tmd_obj,
            "omd_objects": omd_list,
            "rmd_objects": rmd_list,
        }

    def to_flattened_catalog_records(self) -> Dict[str, Any]:
        """
        Flattens catalog entities into the properties dictionary format
        for persistent database storage and search indexing.
        """
        records = {
            "datasets": [],
            "data_objects": [],
            "relationships": []
        }
        for ds_id, ds in self.datasets.items():
            tmd = self.tmd_objects.get(ds_id)
            props = {}
            if tmd:
                props = {
                    "TVolume_physical_bytes": tmd.volume.physical_byte_size or tmd.volume.byte_size,
                    "TVolume_logical_records": tmd.volume.logical_record_count or tmd.volume.record_count,
                    "TVolume_compressed_bytes": tmd.volume.compressed_byte_size or 0,
                    "TVolume_compression_algo": getattr(tmd.volume, "compression_algorithm", "Snappy") or "Snappy",
                    "TVelocity_speed": tmd.velocity.speed if hasattr(tmd.velocity, "speed") else "Fixed",
                    "TVariety_nature": getattr(tmd.variety, "nature", "Structured"),
                    "TVariety_physical_rep": getattr(tmd.variety, "physical_representation", None) or tmd.variety.file_format,
                    "TVariety_ttl": getattr(tmd.variety, "ttl", "Forever"),
                    "TVeracity_source": getattr(tmd.veracity, "source_system_name", None) or tmd.veracity.source_origin,
                    "TVeracity_device": getattr(tmd.veracity, "device_type", "EHR Database Server"),
                    "TVeracity_accuracy": getattr(tmd.veracity, "accuracy", 0.99),
                    "TVeracity_precision": getattr(tmd.veracity, "precision", 0.98),
                    "timestamp": tmd.volume.timestamp.isoformat(),
                }
                if ds.lifecycle_stage == LifecycleStage.PROCESSED and tmd.value:
                    props["TValue_origin"] = getattr(tmd.value, "origin_dataset", None)
                    props["TValue_destination"] = getattr(tmd.value, "destination_dataset", None)
                    props["TValue_transformation"] = getattr(tmd.value, "transformation_action", None)
            records["datasets"].append({
                "id": ds_id,
                "name": ds.name,
                "entity_type": "DataSet",
                "structure_type": ds.structure_type.value,
                "lifecycle_stage": ds.lifecycle_stage.value,
                "properties": props
            })

        for obj_id, obj in self.data_objects.items():
            omd = self.omd_objects.get(obj_id)
            props = {}
            if omd:
                props = {
                    "OVolume_byte_size": omd.volume.physical_byte_size or omd.volume.byte_size,
                    "OVolume_row_count": omd.volume.logical_record_count or omd.volume.record_count or 0,
                    "OVolume_column_count": omd.variety.column_count,
                    "OVolume_null_columns": getattr(omd.volume, "null_column_count", 0),
                    "OVolume_inapplicable_columns": getattr(omd.volume, "inapplicable_column_count", 0),
                    "OVariety_physical_rep": getattr(omd.variety, "physical_representation", None) or omd.variety.file_format,
                    "OVariety_schema": omd.variety.schema_definition,
                    "OVariability_last_modified": omd.variability.last_modified_timestamp.isoformat() if getattr(omd.variability, "last_modified_timestamp", None) else omd.variability.timestamp.isoformat(),
                    "OVariability_deletion_timestamp": omd.variability.deletion_timestamp.isoformat() if getattr(omd.variability, "deletion_timestamp", None) else None,
                    "timestamp": omd.volume.timestamp.isoformat(),
                }
            records["data_objects"].append({
                "id": obj_id,
                "name": obj.name,
                "entity_type": "DataObject",
                "dataset_id": omd.dataset_id if omd else getattr(obj, "dataset_id", None),
                "properties": props
            })

        for r_id, rmd in self.rmd_objects.items():
            props = {}
            if rmd.relationship_type == "InteractsWith":
                stk = self.stakeholders.get(rmd.source_id)
                props = {
                    "TVulnerability_protection": getattr(rmd, "data_protection_method", None) or (getattr(rmd.vulnerability, "data_protection_method", None) if rmd.vulnerability else "De-identified"),
                    "TVulnerability_auth_required": getattr(rmd, "authentication_required", True) if getattr(rmd, "authentication_required", None) is not None else (rmd.vulnerability.authentication_required if rmd.vulnerability else True),
                    "TVulnerability_access_level": getattr(rmd, "authorization_level", None) or (getattr(rmd.vulnerability, "authorization_level", None) if rmd.vulnerability else "Credentialed"),
                    "stakeholder_role": stk.role if stk else getattr(rmd, "role", "Data Steward"),
                    "access_start_time": rmd.access_start_time.isoformat() if getattr(rmd, "access_start_time", None) else None,
                    "access_end_time": rmd.access_end_time.isoformat() if getattr(rmd, "access_end_time", None) else None,
                    "timestamp": rmd.vulnerability.timestamp.isoformat() if rmd.vulnerability else datetime.now(timezone.utc).isoformat(),
                }
            else:
                props = {
                    "relationship_nature": getattr(rmd, "relationship_nature", "Referential"),
                    "join_key": getattr(rmd, "join_key", None) or rmd.attributes.get("join_condition") or "Hospital.hadm_id = ICU.hadm_id",
                    "temporal_sequence": getattr(rmd, "temporal_order", None) or rmd.attributes.get("temporal_order") or "admittime -> intime",
                    "provenance_mapping": getattr(rmd, "provenance", None) or rmd.attributes.get("provenance") or "ETL_Pipeline_Direct",
                }
            records["relationships"].append({
                "id": r_id,
                "relationship_type": rmd.relationship_type,
                "source_id": rmd.source_id,
                "target_id": rmd.target_id,
                "properties": props
            })

        return records



class DLDSetupEngine:
    """
    Metadata Pilot Engine implementing the 3-Stage Guided Workflow Engine
    (Stage A: DLD Setup, Stage B: Indicator Binding, Stage C: Flattening & Registration).
    """

    def __init__(self, repository: Optional[MetadataRepository] = None):
        self.repository = repository or MetadataRepository()
        self.current_stage = PilotStage.INIT

        # Stage A In-Memory Cache
        self.stakeholders: Dict[str, StakeholderInput] = {}
        self.datasets: Dict[str, DataSetInput] = {}
        self.data_objects: Dict[str, DataObjectInput] = {}
        self.relationships: List[DatasetRelationshipInput] = []
        self.graph: nx.DiGraph = nx.DiGraph()

        # Stage B In-Memory Bindings
        self.tmd_objects: Dict[str, TMDObject] = {}
        self.omd_objects: Dict[str, OMDObject] = {}
        self.rmd_objects: Dict[str, RMDObject] = {}

    # =====================================================================
    # Stage A: DLD Setup
    # =====================================================================
    def ingest_manifest(self, manifest_data: dict) -> Dict[str, Any]:
        manifest = DLDManifest(**manifest_data)

        # 1. Register stakeholders
        self.stakeholders.clear()
        for s in manifest.stakeholders:
            self.stakeholders[s.id] = s

        # 2. Register datasets and data objects
        self.datasets.clear()
        self.data_objects.clear()
        for ds in manifest.datasets:
            # Rule: every dataset must have at least one stakeholder
            if not ds.interacts_with:
                raise ValueError(
                    f"Completeness Error: Dataset '{ds.id}' must interact with at least one stakeholder."
                )
            for stk_id in ds.interacts_with:
                if stk_id not in self.stakeholders:
                    raise ValueError(f"Stakeholder '{stk_id}' referenced by '{ds.id}' does not exist.")

            self.datasets[ds.id] = ds
            for do in ds.data_objects:
                self.data_objects[do.id] = do

        # 3. Validate cross-dataset references
        for ds in self.datasets.values():
            for child_id in ds.assembly_children:
                if child_id not in self.datasets:
                    raise ValueError(f"Assembly child '{child_id}' not found for dataset '{ds.id}'")
            for gen_id in ds.generalization_children:
                if gen_id not in self.datasets:
                    raise ValueError(f"Specialized child '{gen_id}' not found for dataset '{ds.id}'")
            for parent_id in ds.processed_from:
                if parent_id not in self.datasets:
                    raise ValueError(f"Lineage parent '{parent_id}' not found for dataset '{ds.id}'")

        # 4. Register explicit RelatesTo edges
        self.relationships.clear()
        for rel in manifest.relationships:
            if rel.source_dataset_id not in self.datasets or rel.target_dataset_id not in self.datasets:
                raise ValueError(f"Invalid dataset link in relationship: {rel}")
            self.relationships.append(rel)

        # 5. Build Graph & Perform DAG Cycle Detection
        self.graph.clear()
        for s_id, s in self.stakeholders.items():
            self.graph.add_node(s_id, node_type="Stakeholder", entity=s)

        for ds_id, ds in self.datasets.items():
            self.graph.add_node(ds_id, node_type="DataSet", entity=ds)
            for stk_id in ds.interacts_with:
                self.graph.add_edge(stk_id, ds_id, relation_type="InteractsWith")

            for child_id in ds.assembly_children:
                self.graph.add_edge(ds_id, child_id, relation_type="AssemblyOf")

            for gen_id in ds.generalization_children:
                self.graph.add_edge(ds_id, gen_id, relation_type="GeneralizationOf")

            for parent_id in ds.processed_from:
                # Directed lineage edge: parent_id -> ds_id
                self.graph.add_edge(parent_id, ds_id, relation_type="ProcessedFrom")

            for do in ds.data_objects:
                self.graph.add_node(do.id, node_type="DataObject", entity=do)
                self.graph.add_edge(ds_id, do.id, relation_type="ContainsObject")

        for rel in self.relationships:
            self.graph.add_edge(
                rel.source_dataset_id,
                rel.target_dataset_id,
                relation_type=rel.relationship_type.value,
                attributes=rel.attributes,
            )

        # Check for cycles in derivation (ProcessedFrom) and assembly (AssemblyOf) subgraphs
        derivation_subgraph = nx.DiGraph()
        assembly_subgraph = nx.DiGraph()
        for u, v, data in self.graph.edges(data=True):
            if data.get("relation_type") == "ProcessedFrom":
                derivation_subgraph.add_edge(u, v)
            elif data.get("relation_type") == "AssemblyOf":
                assembly_subgraph.add_edge(u, v)

        if not nx.is_directed_acyclic_graph(derivation_subgraph):
            cycle = nx.find_cycle(derivation_subgraph, orientation="original")
            raise ValueError(f"Lineage Derivation Cycle detected in ProcessedFrom graph: {cycle}")

        if not nx.is_directed_acyclic_graph(assembly_subgraph):
            cycle = nx.find_cycle(assembly_subgraph, orientation="original")
            raise ValueError(f"Compositional Cycle detected in AssemblyOf hierarchy: {cycle}")

        self.current_stage = PilotStage.STAGE_A_DLD_SETUP

        return {
            "status": "Stage A Complete",
            "current_stage": self.current_stage.value,
            "registered_datasets": len(self.datasets),
            "registered_data_objects": len(self.data_objects),
            "registered_relationships": len(self.relationships),
            "graph_nodes": self.graph.number_of_nodes(),
            "graph_edges": self.graph.number_of_edges(),
            "ready_for_stage_b": True,
        }

    # =====================================================================
    # Stage B: Indicator Binding
    # =====================================================================
    def bind_dataset_tv_words(
        self,
        dataset_id: str,
        volume: VolumeIndicator,
        velocity: VelocityIndicator,
        variety: VarietyIndicator,
        veracity: VeracityIndicator,
        variability: VariabilityIndicator,
        value: Optional[ValueIndicator] = None,
        vulnerability: Optional[VulnerabilityIndicator] = None,
    ) -> TMDObject:
        if dataset_id not in self.datasets:
            raise KeyError(f"Dataset '{dataset_id}' not registered in Stage A.")

        ds = self.datasets[dataset_id]

        # Enforce Value if Processed
        if ds.lifecycle_stage == LifecycleStage.PROCESSED and value is None:
            raise ValueError(
                f"MeDOM Rule Violation: Processed dataset '{dataset_id}' must bind a ValueIndicator (TV-Value)."
            )

        tmd = TMDObject(
            dataset_id=dataset_id,
            dataset_name=ds.name,
            structure_type=ds.structure_type,
            lifecycle_stage=ds.lifecycle_stage,
            volume=volume,
            velocity=velocity,
            variety=variety,
            veracity=veracity,
            variability=variability,
            value=value,
            vulnerability=vulnerability,
        )
        self.tmd_objects[dataset_id] = tmd
        return tmd

    def bind_data_object_ov_words(
        self,
        data_object_id: str,
        dataset_id: str,
        volume: VolumeIndicator,
        variety: VarietyIndicator,
        variability: VariabilityIndicator,
    ) -> OMDObject:
        if data_object_id not in self.data_objects:
            raise KeyError(f"DataObject '{data_object_id}' not registered in Stage A.")
        do = self.data_objects[data_object_id]

        omd = OMDObject(
            data_object_id=data_object_id,
            dataset_id=dataset_id,
            physical_type=do.physical_type,
            volume=volume,
            variety=variety,
            variability=variability,
        )
        self.omd_objects[data_object_id] = omd
        return omd

    def bind_interacts_with_vulnerability(
        self,
        stakeholder_id: str,
        dataset_id: str,
        vulnerability: VulnerabilityIndicator,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> RMDObject:
        if not self.graph.has_edge(stakeholder_id, dataset_id):
            raise ValueError(f"No InteractsWith edge exists between '{stakeholder_id}' and '{dataset_id}'.")

        rel_id = f"{stakeholder_id}_INTERACTS_{dataset_id}"
        rmd = RMDObject(
            relationship_id=rel_id,
            relationship_type="InteractsWith",
            source_id=stakeholder_id,
            target_id=dataset_id,
            vulnerability=vulnerability,
            attributes=attributes or {},
        )
        self.rmd_objects[rel_id] = rmd
        return rmd

    def validate_stage_b_completeness(self) -> Dict[str, Any]:
        """
        Ensures all defined nodes have required bindings before progressing to Stage C.
        """
        missing_datasets = [ds_id for ds_id in self.datasets if ds_id not in self.tmd_objects]
        missing_objects = [do_id for do_id in self.data_objects if do_id not in self.omd_objects]

        # Verify all InteractsWith edges have vulnerability bindings
        missing_interactions = []
        for u, v, data in self.graph.edges(data=True):
            if data.get("relation_type") == "InteractsWith":
                rel_id = f"{u}_INTERACTS_{v}"
                if rel_id not in self.rmd_objects:
                    missing_interactions.append((u, v))

        if missing_datasets or missing_objects or missing_interactions:
            error_details = []
            if missing_datasets:
                error_details.append(f"Missing TMDObject for datasets: {missing_datasets}")
            if missing_objects:
                error_details.append(f"Missing OMDObject for data objects: {missing_objects}")
            if missing_interactions:
                error_details.append(f"Missing Vulnerability binding for interactions: {missing_interactions}")
            raise ValueError("Stage B Incomplete: " + "; ".join(error_details))

        self.current_stage = PilotStage.STAGE_B_INDICATOR_BINDING
        return {
            "status": "Stage B Complete",
            "current_stage": self.current_stage.value,
            "bound_tmd_objects": len(self.tmd_objects),
            "bound_omd_objects": len(self.omd_objects),
            "bound_rmd_objects": len(self.rmd_objects),
            "ready_for_stage_c": True,
        }

    # =====================================================================
    # Stage C: Flattening & Registration
    # =====================================================================
    def flatten_complex_datasets(self) -> Dict[str, Dict[str, Any]]:
        """
        Recursively compute aggregate sizes (sum of components) and merged schema
        hierarchies for COMPLEX datasets.
        """
        flattened_summaries = {}

        def compute_recursive_metrics(ds_id: str, visited: Optional[Set[str]] = None) -> Dict[str, Any]:
            if visited is None:
                visited = set()
            if ds_id in visited:
                return {"byte_size": 0, "record_count": 0, "schema": {}}
            visited.add(ds_id)

            tmd = self.tmd_objects.get(ds_id)
            ds = self.datasets.get(ds_id)

            # Start with direct metrics
            total_bytes = tmd.volume.byte_size if tmd else 0
            total_records = tmd.volume.record_count if tmd and tmd.volume.record_count else 0
            combined_schema = dict(tmd.variety.schema_definition) if tmd else {}

            # Also aggregate direct data objects if any
            for do in ds.data_objects:
                if do.id in self.omd_objects:
                    omd = self.omd_objects[do.id]
                    total_bytes += omd.volume.byte_size
                    if omd.volume.record_count:
                        total_records += omd.volume.record_count
                    combined_schema.update(omd.variety.schema_definition)

            # Recursively aggregate assembly children
            for child_id in ds.assembly_children:
                child_metrics = compute_recursive_metrics(child_id, visited)
                total_bytes += child_metrics["byte_size"]
                total_records += child_metrics["record_count"]
                combined_schema.update(child_metrics["schema"])

            return {
                "byte_size": total_bytes,
                "record_count": total_records,
                "schema": combined_schema,
            }

        for ds_id, ds in self.datasets.items():
            if ds.structure_type == StructureType.COMPLEX:
                metrics = compute_recursive_metrics(ds_id)
                flattened_summaries[ds_id] = metrics

                # Update the TMDObject with aggregated recursive volume and variety
                if ds_id in self.tmd_objects:
                    tmd = self.tmd_objects[ds_id]
                    tmd.volume.byte_size = metrics["byte_size"]
                    tmd.volume.record_count = metrics["record_count"]
                    tmd.variety.schema_definition.update(metrics["schema"])
                    tmd.variety.column_count = len(tmd.variety.schema_definition)

        return flattened_summaries

    def register_catalog(self) -> Dict[str, Any]:
        """
        Executes Stage C: flattens complex datasets, validates schemas,
        and commits all structures into the persistent MetadataRepository.
        """
        # Ensure Stage B is complete
        self.validate_stage_b_completeness()

        # Step 1: Flatten Complex Datasets
        flattened = self.flatten_complex_datasets()

        # Step 2: Persist into Repository
        self.repository.persist(
            stakeholders=self.stakeholders,
            datasets=self.datasets,
            data_objects=self.data_objects,
            relationships=self.relationships,
            tmd_objects=self.tmd_objects,
            omd_objects=self.omd_objects,
            rmd_objects=self.rmd_objects,
            graph=self.graph,
        )

        self.current_stage = PilotStage.REGISTERED

        return {
            "status": "Stage C Complete - Catalog Registered",
            "current_stage": self.current_stage.value,
            "flattened_complex_datasets": list(flattened.keys()),
            "total_registered_datasets": len(self.repository.datasets),
            "total_registered_data_objects": len(self.repository.data_objects),
            "total_tmd_objects": len(self.repository.tmd_objects),
            "total_omd_objects": len(self.repository.omd_objects),
            "total_rmd_objects": len(self.repository.rmd_objects),
        }