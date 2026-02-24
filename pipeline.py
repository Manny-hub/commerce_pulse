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
    # fetch_mongo_collection now returns a list of raw event dicts (full documents),
    # each normalize_* function filters to its own event types internally.
    raw = fetch_mongo_collection(mongo.uri, mongo.db, mongo.col)

    # ---------- Transform ----------
    print("⏳ Transforming data...")

    # FIX: normalize_shipments returns TWO values — unpack both
    orders_df, order_items_df = normalize_orders(raw)
    payments_df = normalize_payments(raw)
    refunds_df, _refund_items_df = normalize_refunds(raw)
    _shipments_df, shipment_events_df = normalize_shipments(raw)

    # Numeric hygiene
    orders_df = _safe_float(orders_df, ["order_total"])
    payments_df = _safe_float(payments_df, ["amount"])
    refunds_df = _safe_float(refunds_df, ["refund_amount"])

    # FIX: use correct function names (they are prefixed with _ in the module)
    dim_customer = _dim_customer(orders_df)
    dim_product = _dim_product(order_items_df)

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
        dim_date = _dim_date(min_ts, max_ts)
        dim_date["date"] = dim_date["date"].dt.date
    else:
        dim_date = _dim_date(pd.NaT, pd.NaT)

    # FIX: items_count — compute from order_items and join onto fact_orders
    if not order_items_df.empty:
        items_count = (
            order_items_df.groupby("order_id", as_index=False)["sku"]
            .count()
            .rename(columns={"sku": "items_count"})
        )
    else:
        items_count = pd.DataFrame(columns=["order_id", "items_count"])

    fact_orders = _fact_orders_agg(orders_df, payments_df, refunds_df, shipment_events_df)
    if not fact_orders.empty and not items_count.empty:
        fact_orders = fact_orders.merge(items_count, on="order_id", how="left")

    fact_order_daily = _fact_order_daily(fact_orders)

    # ---------- Load ----------
    client = bigquery.Client(project=bq.project_id, location=bq.location)
    dataset_fq = f"{bq.project_id}.{bq.dataset_id}"
    ensure_dataset(client, dataset_fq, bq.location)

    specs = bq_table_specs(bq.project_id, bq.dataset_id)
    for spec in specs.values():
        ensure_table(client, spec)

    # Dims + snapshot facts: truncate-and-replace (safe to re-run)
    load_df(client, dim_customer, specs["dim_customer"], write_disposition="WRITE_TRUNCATE")
    load_df(client, dim_product, specs["dim_product"], write_disposition="WRITE_TRUNCATE")
    load_df(client, dim_date, specs["dim_date"], write_disposition="WRITE_TRUNCATE")
    load_df(client, fact_orders, specs["fact_orders"], write_disposition="WRITE_TRUNCATE")
    load_df(client, fact_order_daily, specs["fact_order_daily"], write_disposition="WRITE_TRUNCATE")

    # FIX: append-only facts use WRITE_TRUNCATE too, since we re-fetch all events
    # from Mongo every run. Using WRITE_APPEND would duplicate rows on each pipeline run.
    # If you add incremental watermark logic later, switch these back to WRITE_APPEND.
    load_df(client, payments_df, specs["fact_payments"], write_disposition="WRITE_TRUNCATE")
    load_df(client, refunds_df, specs["fact_refunds"], write_disposition="WRITE_TRUNCATE")
    load_df(client, shipment_events_df, specs["fact_shipments"], write_disposition="WRITE_TRUNCATE")

    print("✅ BigQuery load complete")


if __name__ == "__main__":
    run()
