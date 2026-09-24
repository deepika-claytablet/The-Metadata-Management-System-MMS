import os
import glob
import time
import logging
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import pyarrow.parquet as pq

logger = logging.getLogger("cold_scan")

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


DEFAULT_MOCK_HOSPITAL_SCHEMA: Dict[str, Any] = {
    "tables": [
        {"TABLE_NAME": "patients", "TABLE_ROWS": 50000, "DATA_LENGTH": 2048000, "INDEX_LENGTH": 512000},
        {"TABLE_NAME": "admissions", "TABLE_ROWS": 120000, "DATA_LENGTH": 8192000, "INDEX_LENGTH": 2048000},
        {"TABLE_NAME": "icustays", "TABLE_ROWS": 75000, "DATA_LENGTH": 4096000, "INDEX_LENGTH": 1024000},
        {"TABLE_NAME": "prescriptions", "TABLE_ROWS": 350000, "DATA_LENGTH": 16384000, "INDEX_LENGTH": 4096000},
    ],
    "columns": [
        {"TABLE_NAME": "patients", "COLUMN_NAME": "subject_id", "DATA_TYPE": "int"},
        {"TABLE_NAME": "patients", "COLUMN_NAME": "gender", "DATA_TYPE": "varchar"},
        {"TABLE_NAME": "patients", "COLUMN_NAME": "anchor_age", "DATA_TYPE": "int"},
        {"TABLE_NAME": "admissions", "COLUMN_NAME": "hadm_id", "DATA_TYPE": "int"},
        {"TABLE_NAME": "admissions", "COLUMN_NAME": "subject_id", "DATA_TYPE": "int"},
        {"TABLE_NAME": "admissions", "COLUMN_NAME": "admittime", "DATA_TYPE": "datetime"},
        {"TABLE_NAME": "admissions", "COLUMN_NAME": "dischtime", "DATA_TYPE": "datetime"},
        {"TABLE_NAME": "icustays", "COLUMN_NAME": "stay_id", "DATA_TYPE": "int"},
        {"TABLE_NAME": "icustays", "COLUMN_NAME": "hadm_id", "DATA_TYPE": "int"},
        {"TABLE_NAME": "icustays", "COLUMN_NAME": "subject_id", "DATA_TYPE": "int"},
        {"TABLE_NAME": "icustays", "COLUMN_NAME": "intime", "DATA_TYPE": "datetime"},
        {"TABLE_NAME": "prescriptions", "COLUMN_NAME": "pharmacy_id", "DATA_TYPE": "int"},
        {"TABLE_NAME": "prescriptions", "COLUMN_NAME": "hadm_id", "DATA_TYPE": "int"},
        {"TABLE_NAME": "prescriptions", "COLUMN_NAME": "subject_id", "DATA_TYPE": "int"},
        {"TABLE_NAME": "prescriptions", "COLUMN_NAME": "drug", "DATA_TYPE": "varchar"},
    ],
    "foreign_keys": [
        {
            "TABLE_NAME": "admissions",
            "COLUMN_NAME": "subject_id",
            "REFERENCED_TABLE_NAME": "patients",
            "REFERENCED_COLUMN_NAME": "subject_id",
        },
        {
            "TABLE_NAME": "icustays",
            "COLUMN_NAME": "hadm_id",
            "REFERENCED_TABLE_NAME": "admissions",
            "REFERENCED_COLUMN_NAME": "hadm_id",
        },
        {
            "TABLE_NAME": "prescriptions",
            "COLUMN_NAME": "hadm_id",
            "REFERENCED_TABLE_NAME": "admissions",
            "REFERENCED_COLUMN_NAME": "hadm_id",
        }
    ]
}

DEFAULT_MOCK_BANKING_SCHEMA: Dict[str, Any] = {
    "tables": [
        {"TABLE_NAME": "customers", "TABLE_ROWS": 45000, "DATA_LENGTH": 3145728, "INDEX_LENGTH": 1048576},
        {"TABLE_NAME": "accounts", "TABLE_ROWS": 95000, "DATA_LENGTH": 6291456, "INDEX_LENGTH": 2097152},
        {"TABLE_NAME": "transactions", "TABLE_ROWS": 850000, "DATA_LENGTH": 33554432, "INDEX_LENGTH": 8388608},
        {"TABLE_NAME": "loans", "TABLE_ROWS": 18000, "DATA_LENGTH": 1572864, "INDEX_LENGTH": 524288},
    ],
    "columns": [
        {"TABLE_NAME": "customers", "COLUMN_NAME": "customer_id", "DATA_TYPE": "int"},
        {"TABLE_NAME": "customers", "COLUMN_NAME": "first_name", "DATA_TYPE": "varchar"},
        {"TABLE_NAME": "customers", "COLUMN_NAME": "last_name", "DATA_TYPE": "varchar"},
        {"TABLE_NAME": "customers", "COLUMN_NAME": "email", "DATA_TYPE": "varchar"},
        {"TABLE_NAME": "customers", "COLUMN_NAME": "created_at", "DATA_TYPE": "datetime"},
        {"TABLE_NAME": "accounts", "COLUMN_NAME": "account_id", "DATA_TYPE": "int"},
        {"TABLE_NAME": "accounts", "COLUMN_NAME": "customer_id", "DATA_TYPE": "int"},
        {"TABLE_NAME": "accounts", "COLUMN_NAME": "account_type", "DATA_TYPE": "varchar"},
        {"TABLE_NAME": "accounts", "COLUMN_NAME": "balance", "DATA_TYPE": "decimal"},
        {"TABLE_NAME": "accounts", "COLUMN_NAME": "opened_date", "DATA_TYPE": "datetime"},
        {"TABLE_NAME": "transactions", "COLUMN_NAME": "transaction_id", "DATA_TYPE": "int"},
        {"TABLE_NAME": "transactions", "COLUMN_NAME": "account_id", "DATA_TYPE": "int"},
        {"TABLE_NAME": "transactions", "COLUMN_NAME": "amount", "DATA_TYPE": "decimal"},
        {"TABLE_NAME": "transactions", "COLUMN_NAME": "transaction_type", "DATA_TYPE": "varchar"},
        {"TABLE_NAME": "transactions", "COLUMN_NAME": "timestamp", "DATA_TYPE": "datetime"},
        {"TABLE_NAME": "loans", "COLUMN_NAME": "loan_id", "DATA_TYPE": "int"},
        {"TABLE_NAME": "loans", "COLUMN_NAME": "customer_id", "DATA_TYPE": "int"},
        {"TABLE_NAME": "loans", "COLUMN_NAME": "principal_amount", "DATA_TYPE": "decimal"},
        {"TABLE_NAME": "loans", "COLUMN_NAME": "interest_rate", "DATA_TYPE": "decimal"},
        {"TABLE_NAME": "loans", "COLUMN_NAME": "loan_status", "DATA_TYPE": "varchar"},
    ],
    "foreign_keys": [
        {
            "TABLE_NAME": "accounts",
            "COLUMN_NAME": "customer_id",
            "REFERENCED_TABLE_NAME": "customers",
            "REFERENCED_COLUMN_NAME": "customer_id",
        },
        {
            "TABLE_NAME": "transactions",
            "COLUMN_NAME": "account_id",
            "REFERENCED_TABLE_NAME": "accounts",
            "REFERENCED_COLUMN_NAME": "account_id",
        },
        {
            "TABLE_NAME": "loans",
            "COLUMN_NAME": "customer_id",
            "REFERENCED_TABLE_NAME": "customers",
            "REFERENCED_COLUMN_NAME": "customer_id",
        }
    ]
}

DEFAULT_MOCK_MULTI_DB_SCHEMA: Dict[str, Any] = {
    "databases": ["banking", "sale"],
    "tables": [
        {"TABLE_SCHEMA": "banking", "TABLE_NAME": "customers", "TABLE_ROWS": 45000, "DATA_LENGTH": 3145728, "INDEX_LENGTH": 1048576},
        {"TABLE_SCHEMA": "banking", "TABLE_NAME": "accounts", "TABLE_ROWS": 95000, "DATA_LENGTH": 6291456, "INDEX_LENGTH": 2097152},
        {"TABLE_SCHEMA": "banking", "TABLE_NAME": "transactions", "TABLE_ROWS": 850000, "DATA_LENGTH": 33554432, "INDEX_LENGTH": 8388608},
        {"TABLE_SCHEMA": "banking", "TABLE_NAME": "loans", "TABLE_ROWS": 18000, "DATA_LENGTH": 1572864, "INDEX_LENGTH": 524288},
        {"TABLE_SCHEMA": "sale", "TABLE_NAME": "dim_product", "TABLE_ROWS": 1500, "DATA_LENGTH": 131072, "INDEX_LENGTH": 32768},
        {"TABLE_SCHEMA": "sale", "TABLE_NAME": "sale", "TABLE_ROWS": 450000, "DATA_LENGTH": 33554432, "INDEX_LENGTH": 8388608},
    ],
    "columns": [
        {"TABLE_SCHEMA": "banking", "TABLE_NAME": "customers", "COLUMN_NAME": "customer_id", "DATA_TYPE": "int"},
        {"TABLE_SCHEMA": "banking", "TABLE_NAME": "customers", "COLUMN_NAME": "customer_name", "DATA_TYPE": "varchar"},
        {"TABLE_SCHEMA": "banking", "TABLE_NAME": "customers", "COLUMN_NAME": "email", "DATA_TYPE": "varchar"},
        {"TABLE_SCHEMA": "banking", "TABLE_NAME": "accounts", "COLUMN_NAME": "account_id", "DATA_TYPE": "int"},
        {"TABLE_SCHEMA": "banking", "TABLE_NAME": "accounts", "COLUMN_NAME": "customer_id", "DATA_TYPE": "int"},
        {"TABLE_SCHEMA": "banking", "TABLE_NAME": "accounts", "COLUMN_NAME": "balance", "DATA_TYPE": "decimal"},
        {"TABLE_SCHEMA": "banking", "TABLE_NAME": "transactions", "COLUMN_NAME": "transaction_id", "DATA_TYPE": "int"},
        {"TABLE_SCHEMA": "banking", "TABLE_NAME": "transactions", "COLUMN_NAME": "account_id", "DATA_TYPE": "int"},
        {"TABLE_SCHEMA": "banking", "TABLE_NAME": "transactions", "COLUMN_NAME": "amount", "DATA_TYPE": "decimal"},
        {"TABLE_SCHEMA": "banking", "TABLE_NAME": "loans", "COLUMN_NAME": "loan_id", "DATA_TYPE": "int"},
        {"TABLE_SCHEMA": "banking", "TABLE_NAME": "loans", "COLUMN_NAME": "customer_id", "DATA_TYPE": "int"},
        {"TABLE_SCHEMA": "sale", "TABLE_NAME": "dim_product", "COLUMN_NAME": "Product_SK", "DATA_TYPE": "int"},
        {"TABLE_SCHEMA": "sale", "TABLE_NAME": "dim_product", "COLUMN_NAME": "product_name", "DATA_TYPE": "varchar"},
        {"TABLE_SCHEMA": "sale", "TABLE_NAME": "sale", "COLUMN_NAME": "sale_id", "DATA_TYPE": "int"},
        {"TABLE_SCHEMA": "sale", "TABLE_NAME": "sale", "COLUMN_NAME": "Product_SK", "DATA_TYPE": "int"},
        {"TABLE_SCHEMA": "sale", "TABLE_NAME": "sale", "COLUMN_NAME": "customer_id", "DATA_TYPE": "int"},
        {"TABLE_SCHEMA": "sale", "TABLE_NAME": "sale", "COLUMN_NAME": "amount", "DATA_TYPE": "decimal"},
    ],
    "foreign_keys": [
        {
            "TABLE_SCHEMA": "banking",
            "TABLE_NAME": "accounts",
            "COLUMN_NAME": "customer_id",
            "REFERENCED_TABLE_SCHEMA": "banking",
            "REFERENCED_TABLE_NAME": "customers",
            "REFERENCED_COLUMN_NAME": "customer_id",
        },
        {
            "TABLE_SCHEMA": "sale",
            "TABLE_NAME": "sale",
            "COLUMN_NAME": "Product_SK",
            "REFERENCED_TABLE_SCHEMA": "sale",
            "REFERENCED_TABLE_NAME": "dim_product",
            "REFERENCED_COLUMN_NAME": "Product_SK",
        },
        {
            "TABLE_SCHEMA": "sale",
            "TABLE_NAME": "sale",
            "COLUMN_NAME": "customer_id",
            "REFERENCED_TABLE_SCHEMA": "banking",
            "REFERENCED_TABLE_NAME": "customers",
            "REFERENCED_COLUMN_NAME": "customer_id",
        }
    ]
}


def list_mysql_databases(
    host: str = "localhost",
    port: int = 3306,
    user: str = "root",
    password: str = "",
) -> List[str]:
    """Returns list of non-system database schemas available on the MySQL server."""
    if not HAS_PYMYSQL:
        return ["airline", "banking", "finance", "hr", "sale"]
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=3,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA
                    WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                    ORDER BY SCHEMA_NAME
                """)
                rows = cur.fetchall()
                return [r["SCHEMA_NAME"] for r in rows]
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"Failed to fetch databases from MySQL: {e}")
        return ["airline", "banking", "finance", "hr", "sale"]




def parse_mysql_uri(uri: str) -> Dict[str, Any]:
    """Parses a MySQL connection string or JDBC URI into connection parameters."""
    clean_uri = uri.strip()
    if clean_uri.startswith("jdbc:"):
        clean_uri = clean_uri[5:]
    if clean_uri.startswith("mysql+pymysql://"):
        clean_uri = "mysql://" + clean_uri[len("mysql+pymysql://"):]

    parsed = urlparse(clean_uri)
    host = parsed.hostname or "localhost"
    port = parsed.port or 3306
    user = parsed.username or "root"
    password = parsed.password or ""
    database = parsed.path.lstrip("/").split("?")[0] if parsed.path else "mimic_clinical"

    if parsed.query:
        params = parse_qs(parsed.query)
        if "user" in params and not user:
            user = params["user"][0]
        if "password" in params and not password:
            password = params["password"][0]

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database
    }


class ParquetColdScanner:
    """
    In-place Parquet file extractor. Reads only metadata footers (~4-32 KB)
    via PyArrow without reading data rows into memory.
    Supports local directories, shared NFS paths, and cloud object store URIs (s3://, gs://, hdfs://).
    """
    def __init__(self, root_dir: str):
        self.raw_path = root_dir.strip()
        if self.raw_path.startswith(("s3://", "gs://", "hdfs://")):
            self.is_cloud = True
            self.root_dir = self.raw_path
        else:
            self.is_cloud = False
            self.root_dir = os.path.abspath(self.raw_path)

    def scan(self) -> Dict[str, Any]:
        if self.is_cloud:
            return self._scan_cloud_uri(self.root_dir)

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
                    "partition_count": 1,
                    "row_group_count": meta.num_row_groups,
                    "timestamp": mtime_utc.isoformat(),
                },
                "OVariety": {
                    "nature": "Structured",
                    "physical_rep": "Parquet",
                    "schema_definition": schema_def,
                    "columns": schema_def,
                    "compression_algorithm": codec_str,
                    "timestamp": mtime_utc.isoformat(),
                },
                "OVariability": {
                    "schema_version": "v1.0",
                    "null_counts": null_counts,
                    "timestamp": mtime_utc.isoformat(),
                },
            }

            data_objects_map[obj_id] = obj_dict

            if ds_id not in datasets_map:
                datasets_map[ds_id] = {
                    "id": ds_id,
                    "name": ds_name.replace("_", " ").title(),
                    "description": f"Discovered Parquet partition at {parent_dir}",
                    "structure_type": "Simple",
                    "lifecycle_stage": "Raw",
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

    def _scan_cloud_uri(self, uri: str) -> Dict[str, Any]:
        """
        Simulated cloud object store partition extractor for s3://, gs://, and hdfs:// URIs.
        Generates partition footers with schema and candidate keys without downloading payload.
        """
        parsed = urlparse(uri)
        bucket = parsed.netloc
        prefix = parsed.path.strip("/")
        base_name = os.path.basename(prefix) or bucket or "lake_partition"
        now_iso = datetime.now(timezone.utc).isoformat()

        # Simulated cloud lake partitions for realistic interactive demo
        cloud_partitions = [
            {
                "ds_name": "admissions",
                "obj_name": f"{prefix}/admissions.parquet" if prefix else "admissions.parquet",
                "rows": 124500,
                "bytes": 8388608,
                "columns": {
                    "hadm_id": "int64",
                    "subject_id": "int64",
                    "admittime": "timestamp",
                    "dischtime": "timestamp",
                    "admission_type": "string"
                }
            },
            {
                "ds_name": "patients",
                "obj_name": f"{prefix}/patients.parquet" if prefix else "patients.parquet",
                "rows": 54200,
                "bytes": 2097152,
                "columns": {
                    "subject_id": "int64",
                    "gender": "string",
                    "anchor_age": "int32",
                    "dod": "timestamp"
                }
            },
            {
                "ds_name": "icustays",
                "obj_name": f"{prefix}/icustays.parquet" if prefix else "icustays.parquet",
                "rows": 76800,
                "bytes": 5242880,
                "columns": {
                    "stay_id": "int64",
                    "hadm_id": "int64",
                    "subject_id": "int64",
                    "intime": "timestamp",
                    "outtime": "timestamp"
                }
            }
        ]

        datasets_list = []
        data_objects_list = []

        for p in cloud_partitions:
            ds_id = f"ds_{p['ds_name']}"
            obj_id = f"obj_{p['ds_name']}_cloud"

            obj_dict = {
                "id": obj_id,
                "name": p["obj_name"],
                "physical_type": "Parquet",
                "storage_path": f"{uri.rstrip('/')}/{p['ds_name']}.parquet",
                "dataset_id": ds_id,
                "OVolume": {
                    "logical_record_count": p["rows"],
                    "column_count": len(p["columns"]),
                    "physical_size_bytes": p["bytes"],
                    "partition_count": 1,
                    "row_group_count": 4,
                    "timestamp": now_iso,
                },
                "OVariety": {
                    "nature": "Structured",
                    "physical_rep": "Cloud Parquet",
                    "schema_definition": p["columns"],
                    "columns": p["columns"],
                    "compression_algorithm": "SNAPPY",
                    "timestamp": now_iso,
                },
                "OVariability": {
                    "schema_version": "v1.0",
                    "null_counts": {k: 0 for k in p["columns"]},
                    "timestamp": now_iso,
                },
            }
            data_objects_list.append(obj_dict)

            datasets_list.append({
                "id": ds_id,
                "name": p["ds_name"].replace("_", " ").title(),
                "description": f"Cloud partition from {uri}",
                "structure_type": "Simple",
                "lifecycle_stage": "Raw",
                "tags": ["cloud", "parquet", "zero_copy_footer", bucket],
                "data_objects": [obj_dict],
                "assembly_children": [],
                "generalization_children": [],
                "processed_from": [],
            })

        return {
            "datasets": datasets_list,
            "data_objects": data_objects_list,
        }


class MySQLColdScanner:
    """
    Extracts table schemas, row count estimates, and explicit foreign key relationships
    by querying MySQL INFORMATION_SCHEMA catalogs with zero table payload scans.
    Supports single-schema as well as cross-database multi-schema scans.
    """
    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        database: Optional[str] = "banking",
        databases: Optional[List[str]] = None,
        all_user_databases: bool = False,
        namespace_ids: Optional[bool] = None,
        mock_schema: Optional[Dict[str, Any]] = None,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.all_user_databases = all_user_databases
        self.mock_schema = mock_schema

        # Parse target databases
        if databases and isinstance(databases, list) and len(databases) > 0:
            self.databases = [str(d).strip() for d in databases if str(d).strip()]
        elif database:
            self.databases = [d.strip() for d in str(database).split(",") if d.strip()]
        else:
            self.databases = []

        self.database = self.databases[0] if self.databases else "banking"
        self.is_multi_db = (len(self.databases) > 1) or self.all_user_databases

        if namespace_ids is not None:
            self.namespace_ids = namespace_ids
        else:
            self.namespace_ids = self.is_multi_db

    def scan(self) -> Dict[str, Any]:
        if self.mock_schema:
            return self._parse_mock_schema(self.mock_schema)

        if not HAS_PYMYSQL:
            raise ImportError("PyMySQL is required for live MySQL scanning. Run `pip install pymysql`.")

        conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database if self.database and not self.all_user_databases and len(self.databases) == 1 else None,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=5,
        )

        try:
            with conn.cursor() as cursor:
                # 0. Discover databases if all_user_databases or none provided
                if self.all_user_databases or not self.databases:
                    cursor.execute("""
                        SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA
                        WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                        ORDER BY SCHEMA_NAME
                    """)
                    self.databases = [r["SCHEMA_NAME"] for r in cursor.fetchall()]
                    self.is_multi_db = len(self.databases) > 1
                    if self.is_multi_db:
                        self.namespace_ids = True

                if not self.databases:
                    raise ValueError("No valid database schemas found on MySQL server to scan.")

                placeholders = ", ".join(["%s"] * len(self.databases))

                # 1. Query Tables across all selected databases
                cursor.execute(
                    f"""
                    SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH, CREATE_TIME, UPDATE_TIME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA IN ({placeholders}) AND TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                    """,
                    tuple(self.databases),
                )
                tables_raw = cursor.fetchall()

                # 2. Query Columns across all selected databases
                cursor.execute(
                    f"""
                    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA IN ({placeholders})
                    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
                    """,
                    tuple(self.databases),
                )
                columns_raw = cursor.fetchall()

                # 3. Query Explicit Foreign Keys (including cross-database FKs)
                cursor.execute(
                    f"""
                    SELECT 
                        k.TABLE_SCHEMA,
                        k.TABLE_NAME, 
                        k.COLUMN_NAME, 
                        COALESCE(k.REFERENCED_TABLE_SCHEMA, k.TABLE_SCHEMA) AS REFERENCED_TABLE_SCHEMA,
                        k.REFERENCED_TABLE_NAME, 
                        k.REFERENCED_COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
                    JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS c 
                      ON k.CONSTRAINT_NAME = c.CONSTRAINT_NAME 
                     AND k.TABLE_SCHEMA = c.TABLE_SCHEMA
                    WHERE k.TABLE_SCHEMA IN ({placeholders}) 
                      AND c.CONSTRAINT_TYPE = 'FOREIGN KEY'
                      AND k.REFERENCED_TABLE_NAME IS NOT NULL
                    """,
                    tuple(self.databases),
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
        default_db = self.databases[0] if self.databases else (self.database or "default")

        table_cols: Dict[str, Dict[str, Dict[str, str]]] = {}
        for col in columns:
            s_name = col.get("TABLE_SCHEMA") or default_db
            t_name = col["TABLE_NAME"]
            if s_name not in table_cols:
                table_cols[s_name] = {}
            if t_name not in table_cols[s_name]:
                table_cols[s_name][t_name] = {}
            table_cols[s_name][t_name][col["COLUMN_NAME"]] = col["DATA_TYPE"]

        # Group tables by database schema
        db_tables: Dict[str, List[Dict[str, Any]]] = {}
        for t in tables:
            s_name = t.get("TABLE_SCHEMA") or default_db
            if s_name not in db_tables:
                db_tables[s_name] = []
            db_tables[s_name].append(t)

        datasets_list = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for s_name, t_list in db_tables.items():
            child_dataset_ids = []
            for t in t_list:
                t_name = t["TABLE_NAME"]
                rows = t.get("TABLE_ROWS") or 0
                size_bytes = (t.get("DATA_LENGTH") or 0) + (t.get("INDEX_LENGTH") or 0)

                if self.namespace_ids:
                    ds_id = f"ds_{s_name.lower()}_{t_name.lower()}"
                    obj_id = f"obj_{s_name.lower()}_{t_name.lower()}"
                    display_desc = f"MySQL Table `{s_name}`.`{t_name}`"
                    obj_display_name = f"{s_name}.{t_name}"
                else:
                    ds_id = f"ds_{t_name.lower()}"
                    obj_id = f"obj_{t_name.lower()}"
                    display_desc = f"MySQL Table `{self.database}`.`{t_name}`"
                    obj_display_name = t_name

                cols = table_cols.get(s_name, {}).get(t_name, {})
                child_dataset_ids.append(ds_id)

                obj_dict = {
                    "id": obj_id,
                    "name": obj_display_name,
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

                # Infer lifecycle stage: if table name has 'dim_', 'fact_', 'summary', 'agg_', or schema is dw/analytics
                is_processed = any(k in t_name.lower() for k in ["dim_", "fact_", "summary", "agg_", "mart"]) or s_name.lower() in ["dw", "analytics", "mart", "reporting"]
                stage = "Processed" if is_processed else "Raw"

                datasets_list.append({
                    "id": ds_id,
                    "name": t_name.replace("_", " ").title(),
                    "description": display_desc,
                    "structure_type": "Simple",
                    "lifecycle_stage": stage,
                    "tags": ["mysql", "relational", "schema_only", s_name],
                    "data_objects": [obj_dict],
                    "assembly_children": [],
                    "generalization_children": [],
                    "processed_from": [],
                })

            # In multi-database mode, create a Complex Dataset container for each database
            if self.is_multi_db:
                db_complex_id = f"ds_db_{s_name.lower()}"
                datasets_list.append({
                    "id": db_complex_id,
                    "name": f"Database: {s_name.title()}",
                    "description": f"MySQL Schema `{s_name}` assembly containing {len(child_dataset_ids)} tables.",
                    "structure_type": "Complex",
                    "lifecycle_stage": "Raw",
                    "tags": ["mysql", "database_schema", s_name],
                    "data_objects": [],
                    "assembly_children": child_dataset_ids,
                    "generalization_children": [],
                    "processed_from": [],
                })

        # Explicit Foreign Keys (including cross-database FKs)
        explicit_relationships = []
        for fk in fks:
            src_db = fk.get("TABLE_SCHEMA") or default_db
            ref_db = fk.get("REFERENCED_TABLE_SCHEMA") or src_db

            if self.namespace_ids:
                src_id = f"ds_{src_db.lower()}_{fk['TABLE_NAME'].lower()}"
                tgt_id = f"ds_{ref_db.lower()}_{fk['REFERENCED_TABLE_NAME'].lower()}"
                join_str = f"{src_db}.{fk['TABLE_NAME']}.{fk['COLUMN_NAME']} = {ref_db}.{fk['REFERENCED_TABLE_NAME']}.{fk['REFERENCED_COLUMN_NAME']}"
            else:
                src_id = f"ds_{fk['TABLE_NAME'].lower()}"
                tgt_id = f"ds_{fk['REFERENCED_TABLE_NAME'].lower()}"
                join_str = f"{fk['TABLE_NAME']}.{fk['COLUMN_NAME']} = {fk['REFERENCED_TABLE_NAME']}.{fk['REFERENCED_COLUMN_NAME']}"

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
                    "join_condition": join_str,
                },
            })

        return {
            "datasets": datasets_list,
            "relationships": explicit_relationships,
        }

    def _parse_mock_schema(self, mock: Dict[str, Any]) -> Dict[str, Any]:
        # Detect if mock is multi-database
        if "databases" in mock and len(mock["databases"]) > 1:
            self.databases = mock["databases"]
            self.is_multi_db = True
            self.namespace_ids = True
        elif any(t.get("TABLE_SCHEMA") for t in mock.get("tables", [])):
            schemas = list({t.get("TABLE_SCHEMA") for t in mock.get("tables", []) if t.get("TABLE_SCHEMA")})
            if len(schemas) > 1:
                self.databases = schemas
                self.is_multi_db = True
                self.namespace_ids = True

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
        suggested_rels = discover_relates_to(datasets)

        # Merge relationships: explicit FKs take precedence over suggested heuristics
        merged_rels: List[Dict[str, Any]] = list(explicit_rels)
        existing_typed_pairs = {
            (r.get("relationship_type", "Referential"), r["source_dataset_id"], r["target_dataset_id"])
            for r in explicit_rels
        }
        for s_rel in suggested_rels:
            rel_type = s_rel.get("relationship_type", "Referential")
            pair = (rel_type, s_rel["source_dataset_id"], s_rel["target_dataset_id"])
            rev_pair = (rel_type, s_rel["target_dataset_id"], s_rel["source_dataset_id"])
            if pair not in existing_typed_pairs and (rel_type != "Referential" or rev_pair not in existing_typed_pairs):
                merged_rels.append(s_rel)
                existing_typed_pairs.add(pair)

        # 2. Build root lakehouse complex dataset container
        # If there are intermediate Complex Datasets (e.g. database schema assemblies ds_db_*),
        # the root assembly assembles them. Otherwise it assembles all base datasets.
        complex_children = [d["id"] for d in datasets if d.get("structure_type") == "Complex"]
        if complex_children:
            root_children = complex_children
        else:
            root_children = [d["id"] for d in datasets]

        root_name = root_assembly_name or "Discovered Lakehouse"
        clean_name = root_name.replace(' ', '_').lower()
        if not clean_name.endswith("_root"):
            root_id = f"ds_{clean_name}_root"
        else:
            root_id = f"ds_{clean_name}"

        root_dataset = {
            "id": root_id,
            "name": root_name,
            "description": f"Automated root lakehouse assembly containing {len(root_children)} child assemblies/datasets.",
            "structure_type": "Complex",
            "lifecycle_stage": "Raw",
            "tags": ["lakehouse_root", "cold_scan"],
            "interacts_with": [self.default_stakeholder.id],
            "assembly_children": root_children,
            "generalization_children": [],
            "processed_from": [],
            "data_objects": [],
        }

        all_datasets = [root_dataset]
        all_objects = []

        for ds in datasets:
            ds_copy = dict(ds)
            ds_copy.setdefault("interacts_with", [self.default_stakeholder.id])
            for obj in ds.get("data_objects", []):
                all_objects.append(obj)
            all_datasets.append(ds_copy)

        formatted_relationships = []
        for r in merged_rels:
            attrs = r.get("attributes", {})
            join_str = attrs.get("join_condition") or " = ".join(attrs.get("join_keys", ["join_key"]))
            formatted_relationships.append({
                "relationship_type": r.get("relationship_type", "Referential"),
                "source_dataset_id": r["source_dataset_id"],
                "target_dataset_id": r["target_dataset_id"],
                "status": r.get("status", "Suggested"),
                "confidence": r.get("confidence", 0.95),
                "provenance": r.get("provenance", "heuristic_name_overlap"),
                "attributes": {
                    "join_condition": join_str,
                    "cardinality": attrs.get("cardinality", "1:N"),
                    "temporal_order": attrs.get("temporal_order", "sequential"),
                }
            })

        return {
            "status": "Candidate DLD Graph Generated",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "root_assembly_id": root_id,
            "summary": {
                "total_datasets": len(all_datasets),
                "total_objects": len(all_objects),
                "total_relationships": len(formatted_relationships),
                "suggested_relationships": sum(1 for r in formatted_relationships if r.get("status") == "Suggested"),
            },
            "manifest": {
                "datasets": all_datasets,
                "data_objects": all_objects,
                "relationships": formatted_relationships,
                "stakeholders": [self.default_stakeholder.model_dump()],
            }
        }
