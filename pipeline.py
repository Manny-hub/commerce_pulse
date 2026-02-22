from __future__ import annotations

import pandas as pd
from google.cloud import bigquery

from src.config.settings import load_bq_config, load_mongo_config
from src.database.fetch_data import fetch_mongo_collection
from src.transform.normalize_orders import normalize_orders
from src.transform.normalize_payments import normalize_payments
from src.transform.normalize_refunds import normalize_refunds
from src.transform.normalize_shipments import normalize_shipments
from src.transform.dim_table import _dim_customer, _dim_product, _dim_date
from src.transform.facts_table import _fact_orders_agg, _fact_order_daily
from src.bq.bq_loads import ensure_dataset, ensure_table, load_df
from src.bq.bq_models import bq_table_specs

def _safe_float(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def run():
    mongo = load_mongo_config()
    bq = load_bq_config()

    # ---------- Extract ----------
    print("⏳ Fetching raw data from MongoDB...")
    raw = fetch_mongo_collection(mongo.uri, mongo.db, mongo.col)

    # ---------- Transform ----------
    print("⏳ Transforming data...")
    orders_df, order_items_df = normalize_orders(raw)
    payments_df = normalize_payments(raw)
    refunds_df, _refund_items_df = normalize_refunds(raw)
    shipment_events_df = normalize_shipments_events(raw)    
    # numeric hygiene
    orders_df = _safe_float(orders_df, ["order_total"])
    payments_df = _safe_float(payments_df, ["amount"])
    refunds_df = _safe_float(refunds_df, ["refund_amount"])

    # Dimensions
    dim_customer = build_dim_customer(orders_df)
    dim_product = build_dim_product(order_items_df)

    # dim_date range from min/max across all event tables
    all_ts = []
    for col, df in [
        ("order_created_at", orders_df),
        ("payment_ts", payments_df),
        ("refund_ts", refunds_df),
        ("event_ts", shipment_events_df),
    ]:
        if not df.empty and col in df.columns:
            all_ts.append(df[col])

    if all_ts:
        min_ts = pd.concat(all_ts).min()
        max_ts = pd.concat(all_ts).max()
        dim_date = build_dim_date(min_ts, max_ts)
        # BigQuery DATE type expects date without tz/time; convert
        dim_date["date"] = dim_date["date"].dt.date
    else:
        dim_date = build_dim_date(pd.NaT, pd.NaT)

    # Facts
    fact_orders = build_fact_orders(orders_df, payments_df, refunds_df, shipment_events_df)
    fact_order_daily = build_fact_order_daily(fact_orders)

    # ---------- Load ----------
    client = bigquery.Client(project=bq.project_id, location=bq.location)
    dataset_fq = f"{bq.project_id}.{bq.dataset_id}"
    ensure_dataset(client, dataset_fq, bq.location)

    specs = bq_table_specs(bq.project_id, bq.dataset_id)
    for spec in specs.values():
        ensure_table(client, spec)

    # Dims + snapshot facts: truncate
    load_df(client, dim_customer, specs["dim_customer"], write_disposition="WRITE_TRUNCATE")
    load_df(client, dim_product, specs["dim_product"], write_disposition="WRITE_TRUNCATE")
    load_df(client, dim_date, specs["dim_date"], write_disposition="WRITE_TRUNCATE")
    load_df(client, fact_orders, specs["fact_orders"], write_disposition="WRITE_TRUNCATE")
    load_df(client, fact_order_daily, specs["fact_order_daily"], write_disposition="WRITE_TRUNCATE")

    # Append-only facts: append
    load_df(client, payments_df, specs["fact_payments"], write_disposition="WRITE_APPEND")
    load_df(client, refunds_df, specs["fact_refunds"], write_disposition="WRITE_APPEND")
    load_df(client, shipment_events_df, specs["fact_shipments"], write_disposition="WRITE_APPEND")

    print("✅ BigQuery load complete")

if __name__ == "__main__":
    run()
