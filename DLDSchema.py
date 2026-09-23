from datetime import datetime, timezone
from enum import Enum
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field, model_validator


# =====================================================================
# DLD Categorical Enums
# =====================================================================

class StructureType(str, Enum):
    SIMPLE = "Simple"
    COMPLEX = "Complex"
    ABSTRACT = "Abstract"


class LifecycleStage(str, Enum):
    RAW = "Raw"
    PROCESSED = "Processed"


class RelationKind(str, Enum):
    REFERENTIAL = "Referential"  # Join / Foreign-key associations
    SEMANTIC = "Semantic"        # Conceptual similarity / synonymy
    TEMPORAL = "Temporal"        # Sequential / time-series correlation
    PROVENANCE = "Provenance"    # Historical derivation / attribution


class VulnerabilityClassification(str, Enum):
    PUBLIC = "Public"
    INTERNAL = "Internal"
    CONFIDENTIAL = "Confidential"
    RESTRICTED = "Restricted"


class BusinessCriticality(str, Enum):
    TIER_1 = "Tier-1 (Mission Critical)"
    TIER_2 = "Tier-2 (Business Operational)"
    TIER_3 = "Tier-3 (Analytical / Low)"


class CandidateStatus(str, Enum):
    SUGGESTED = "Suggested"
    CONFIRMED = "Confirmed"
    REJECTED = "Rejected"


# =====================================================================
# Step 1: Core DLD Entities
# =====================================================================

class StakeholderInput(BaseModel):
    id: str
    name: str
    role: str  # Curator, Data Analyst, Data Scientist, Data Engineer, Steward
    email: Optional[str] = None
    department: Optional[str] = None


class DataObjectInput(BaseModel):
    id: str
    name: str
    physical_type: str  # e.g., CSV, Parquet, Relational Table, DICOM, Stream
    storage_path: Optional[str] = None


class DataSetInput(BaseModel):
    id: str
    name: str
    structure_type: StructureType
    lifecycle_stage: LifecycleStage
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    interacts_with: List[str] = Field(
        default_factory=list,
        description="List of Stakeholder IDs"
    )
    assembly_children: List[str] = Field(
        default_factory=list,
        description="IDs of child datasets forming this complex dataset"
    )
    generalization_children: List[str] = Field(
        default_factory=list,
        description="IDs of specialized child datasets"
    )
    processed_from: List[str] = Field(
        default_factory=list,
        description="IDs of source datasets"
    )
    data_objects: List[DataObjectInput] = Field(
        default_factory=list,
        description="Data objects contained directly in this dataset"
    )

    @model_validator(mode="after")
    def validate_dld_integrity(self):
        # A simple dataset must not contain assembly children
        if self.structure_type == StructureType.SIMPLE and self.assembly_children:
            raise ValueError(f"Simple dataset {self.id} cannot have assembly children.")

        # Raw datasets cannot be processed from other datasets
        if self.lifecycle_stage == LifecycleStage.RAW and self.processed_from:
            raise ValueError(f"Raw dataset {self.id} cannot have 'processed_from' dependencies.")

        # Processed datasets must define at least one source
        if self.lifecycle_stage == LifecycleStage.PROCESSED and not self.processed_from:
            raise ValueError(f"Processed dataset {self.id} must define at least one 'processed_from' parent.")

        return self


class DatasetRelationshipInput(BaseModel):
    relationship_type: RelationKind
    source_dataset_id: str
    target_dataset_id: str
    attributes: Dict[str, Any] = Field(default_factory=dict)
    status: CandidateStatus = Field(default=CandidateStatus.CONFIRMED, description="Suggested candidate or Confirmed edge")
    confidence: float = Field(default=1.0, ge=0.0, le=1.0, description="Inference confidence score")
    provenance: Optional[str] = Field(default="manual", description="Heuristic or source of relationship")


class DLDManifest(BaseModel):
    stakeholders: List[StakeholderInput]
    datasets: List[DataSetInput]
    relationships: List[DatasetRelationshipInput] = Field(default_factory=list)


# =====================================================================
# Step 1: Technical Indicator Models (The 7 V-Words)
# Mandatory timestamp support for variability tracking
# =====================================================================

class BaseIndicator(BaseModel):
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Mandatory UTC timestamp for variability & longitudinal tracking"
    )
    recorded_by: Optional[str] = "system"


class VolumeIndicator(BaseIndicator):
    """V-Word 1: Volume (TV-Volume / OVolume)"""
    byte_size: int = Field(..., ge=0, description="Size in bytes")
    record_count: Optional[int] = Field(default=None, ge=0, description="Number of rows/records")
    partition_count: Optional[int] = Field(default=1, ge=1, description="Number of partitions/files")


class VelocityIndicator(BaseIndicator):
    """V-Word 2: Velocity (TV-Velocity)"""
    ingestion_mode: str = Field(default="Batch", description="Batch, Micro-batch, Stream")
    expected_refresh_interval_sec: int = Field(
        ..., ge=0, description="Expected refresh frequency / TTL in seconds"
    )
    last_refreshed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp of the most recent data refresh"
    )
    latency_ms: Optional[float] = Field(default=None, ge=0.0, description="End-to-end ingestion latency")


class VarietyIndicator(BaseIndicator):
    """V-Word 3: Variety (TV-Variety / OVariety)"""
    schema_definition: Dict[str, str] = Field(
        default_factory=dict,
        description="Mapping of column/field names to data types"
    )
    column_count: int = Field(default=0, ge=0)
    file_format: str = Field(default="Unknown", description="CSV, Parquet, JSON, Delta, etc.")
    compression_algorithm: Optional[str] = Field(default=None, description="e.g. SNAPPY, GZIP, NONE")
    null_counts: Dict[str, int] = Field(default_factory=dict, description="Null count per column")


class VeracityIndicator(BaseIndicator):
    """V-Word 4: Veracity (TV-Veracity)"""
    source_origin: str = Field(..., min_length=1, description="Source provenance or system origin")
    quality_score: float = Field(
        default=1.0, ge=0.0, le=1.0, description="Overall quality index between 0.0 and 1.0"
    )
    completeness_ratio: float = Field(default=1.0, ge=0.0, le=1.0)
    null_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    validation_rules_passed: int = Field(default=0, ge=0)
    validation_rules_failed: int = Field(default=0, ge=0)


class VariabilityIndicator(BaseIndicator):
    """V-Word 5: Variability (TV-Variability / OVariability)"""
    schema_version: str = Field(default="v1.0")
    schema_drift_detected: bool = Field(default=False)
    rate_of_change_records_per_day: Optional[float] = Field(default=None, ge=0.0)
    volatility_score: Optional[float] = Field(default=0.0, ge=0.0, le=1.0)
    change_history: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Historical log of schema and data volume evolutions"
    )


class ValueIndicator(BaseIndicator):
    """V-Word 6: Value (TV-Value - Mandatory for Processed Datasets)"""
    business_criticality: BusinessCriticality = Field(default=BusinessCriticality.TIER_2)
    cost_per_query: Optional[float] = Field(default=None, ge=0.0)
    roi_score: Optional[float] = Field(default=None, ge=0.0, le=10.0)
    sla_tier: str = Field(default="Standard", description="SLA Level e.g. Gold, Silver, Bronze")


class VulnerabilityIndicator(BaseIndicator):
    """V-Word 7: Vulnerability (RV-Vulnerability / TV-Vulnerability)"""
    classification: VulnerabilityClassification = Field(default=VulnerabilityClassification.INTERNAL)
    encryption_at_rest: bool = Field(default=True)
    encryption_in_transit: bool = Field(default=True)
    access_control_policy: str = Field(
        ..., min_length=1, description="Defined RBAC or ABAC policy document / rule"
    )
    authorized_roles: List[str] = Field(default_factory=list)
    retention_period_days: Optional[int] = Field(default=365, ge=0)


# =====================================================================
# Step 1: MD Container Objects (MeDOM)
# =====================================================================

class OMDObject(BaseModel):
    """
    Object Metadata Object:
    Binds a physical DataObject to operational OV-words (OVolume, OVariety, OVariability).
    """
    data_object_id: str
    dataset_id: str
    physical_type: str
    volume: VolumeIndicator
    variety: VarietyIndicator
    variability: VariabilityIndicator
    observation_history: List[Dict[str, Any]] = Field(default_factory=list)


class RMDObject(BaseModel):
    """
    Relationship Metadata Object:
    Binds an edge (e.g. InteractsWith or RelatesTo) to security/relational indicators.
    """
    relationship_id: str
    relationship_type: str  # InteractsWith, Referential, Semantic, Temporal, Provenance
    source_id: str
    target_id: str
    vulnerability: Optional[VulnerabilityIndicator] = None
    attributes: Dict[str, Any] = Field(default_factory=dict)


class TMDObject(BaseModel):
    """
    Technical Metadata Object:
    Binds a DLD DataSet to its 6 TV-words (Volume, Velocity, Variety, Veracity, Variability,
    Value—mandatory if Processed) plus optional governance Vulnerability.
    """
    dataset_id: str
    dataset_name: str
    structure_type: StructureType
    lifecycle_stage: LifecycleStage
    volume: VolumeIndicator
    velocity: VelocityIndicator
    variety: VarietyIndicator
    veracity: VeracityIndicator
    variability: VariabilityIndicator
    value: Optional[ValueIndicator] = None
    vulnerability: Optional[VulnerabilityIndicator] = None
    historical_snapshots: List[Dict[str, Any]] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_processed_has_value(self):
        # Enforce Value indicator if dataset is Processed
        if self.lifecycle_stage == LifecycleStage.PROCESSED and self.value is None:
            raise ValueError(f"Processed dataset '{self.dataset_id}' must have a ValueIndicator (TV-Value).")
        return self