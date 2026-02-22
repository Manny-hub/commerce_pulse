from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd
from google.cloud import bigquery

@dataclass(frozen=True)
class TableSpec:
    table_id: str  # dataset.table
    schema: List[bigquery.SchemaField]
    partition_field: Optional[str] = None  # e.g., "date" or "payment_ts"
    clustering_fields: Optional[List[str]] = None

def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str) -> None:
    ds_ref = bigquery.Dataset(dataset_id)
    ds_ref.location = location
    try:
        client.get_dataset(ds_ref)
    except Exception:
        client.create_dataset(ds_ref, exists_ok=True)

def ensure_table(client: bigquery.Client, spec: TableSpec) -> None:
    table_ref = bigquery.Table(spec.table_id, schema=spec.schema)

    if spec.partition_field:
        table_ref.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=spec.partition_field,
        )

    if spec.clustering_fields:
        table_ref.clustering_fields = spec.clustering_fields

    try:
        client.get_table(spec.table_id)
    except Exception:
        client.create_table(table_ref, exists_ok=True)

def load_df(
    client: bigquery.Client,
    df: pd.DataFrame,
    spec: TableSpec,
    write_disposition: str,
) -> None:
    """
    write_disposition: WRITE_TRUNCATE for dims/snapshots; WRITE_APPEND for append-only facts
    """
    if df is None or df.empty:
        return

    job_config = bigquery.LoadJobConfig(
        schema=spec.schema,
        write_disposition=write_disposition,
    )

    job = client.load_table_from_dataframe(df, spec.table_id, job_config=job_config)
    job.result()
