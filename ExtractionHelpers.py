import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import pandas as pd
import pyarrow.parquet as pq

from DLDSchema import (
    VolumeIndicator,
    VarietyIndicator,
    VariabilityIndicator,
    OMDObject,
)

def profile_os_file(filepath: str) -> Dict[str, Any]:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f'File not found: {filepath}')

    stat = os.stat(filepath)
    return {
        'byte_size': stat.st_size,
        'last_modified': datetime.fromtimestamp(stat.st_mtime, timezone.utc),
    }

def profile_parquet_file(filepath: str) -> Dict[str, Any]:
    os_info = profile_os_file(filepath)
    parquet_file = pq.ParquetFile(filepath)
    meta = parquet_file.metadata
    schema = parquet_file.schema_arrow

    compression_codecs = set()
    for rg_idx in range(meta.num_row_groups):
        rg = meta.row_group(rg_idx)
        for col_idx in range(rg.num_columns):
            codec = rg.column(col_idx).compression
            if codec:
                compression_codecs.add(str(codec).upper())

    compression_str = ', '.join(sorted(compression_codecs)) if compression_codecs else 'NONE'
    schema_def = {name: str(schema.field(name).type) for name in schema.names}

    null_counts = {}
    for col_name in schema.names:
        total_nulls = 0
        has_stats = True
        for rg_idx in range(meta.num_row_groups):
            col_stat = meta.row_group(rg_idx).column(schema.names.index(col_name)).statistics
            if col_stat and col_stat.has_null_count:
                total_nulls += col_stat.null_count
            else:
                has_stats = False
                break
        if has_stats:
            null_counts[col_name] = total_nulls

    return {
        'byte_size': os_info['byte_size'],
        'record_count': meta.num_rows,
        'partition_count': meta.num_row_groups,
        'column_count': meta.num_columns,
        'schema_definition': schema_def,
        'file_format': 'Parquet',
        'compression_algorithm': compression_str,
        'null_counts': null_counts,
        'last_modified': os_info['last_modified'],
    }

def profile_csv_file(filepath: str, sample_rows: Optional[int] = 50000) -> Dict[str, Any]:
    os_info = profile_os_file(filepath)

    compression = None
    if filepath.endswith('.gz'):
        compression = 'gzip'
    elif filepath.endswith('.bz2'):
        compression = 'bz2'
    elif filepath.endswith('.zip'):
        compression = 'zip'

    df_sample = pd.read_csv(filepath, nrows=sample_rows, compression=compression)

    total_rows = len(df_sample)
    if sample_rows and len(df_sample) == sample_rows:
        try:
            with open(filepath, 'rb') as f:
                total_rows = sum(1 for _ in f) - 1
        except Exception:
            pass

    schema_def = {str(col): str(dtype) for col, dtype in df_sample.dtypes.items()}
    null_counts = {str(col): int(df_sample[col].isna().sum()) for col in df_sample.columns}

    return {
        'byte_size': os_info['byte_size'],
        'record_count': max(0, total_rows),
        'partition_count': 1,
        'column_count': len(df_sample.columns),
        'schema_definition': schema_def,
        'file_format': 'CSV',
        'compression_algorithm': compression.upper() if compression else 'NONE',
        'null_counts': null_counts,
        'last_modified': os_info['last_modified'],
    }

def auto_bind_data_object(
    data_object_id: str,
    dataset_id: str,
    filepath: str,
    physical_type: Optional[str] = None,
) -> OMDObject:
    lower_path = filepath.lower()
    if lower_path.endswith('.parquet') or lower_path.endswith('.pq'):
        stats = profile_parquet_file(filepath)
        detected_type = physical_type or 'Parquet'
    elif lower_path.endswith('.csv') or lower_path.endswith('.tsv') or lower_path.endswith('.csv.gz'):
        stats = profile_csv_file(filepath)
        detected_type = physical_type or 'CSV'
    else:
        os_info = profile_os_file(filepath)
        stats = {
            'byte_size': os_info['byte_size'],
            'record_count': None,
            'partition_count': 1,
            'column_count': 0,
            'schema_definition': {},
            'file_format': physical_type or 'Binary File',
            'compression_algorithm': None,
            'null_counts': {},
            'last_modified': os_info['last_modified'],
        }
        detected_type = physical_type or 'File'

    volume = VolumeIndicator(
        byte_size=stats['byte_size'],
        record_count=stats['record_count'],
        partition_count=stats['partition_count'],
        timestamp=stats['last_modified'],
    )

    variety = VarietyIndicator(
        schema_definition=stats['schema_definition'],
        column_count=stats['column_count'],
        file_format=stats['file_format'],
        compression_algorithm=stats['compression_algorithm'],
        null_counts=stats['null_counts'],
        timestamp=stats['last_modified'],
    )

    variability = VariabilityIndicator(
        schema_version='v1.0',
        schema_drift_detected=False,
        timestamp=stats['last_modified'],
    )

    return OMDObject(
        data_object_id=data_object_id,
        dataset_id=dataset_id,
        physical_type=detected_type,
        volume=volume,
        variety=variety,
        variability=variability,
    )
