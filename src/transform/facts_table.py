from __future__ import annotations
import pandas as pd

from src.transform.dim_table import _stable_customer_key

def _fact_orders_agg(
    orders_df: pd.DataFrame,
    payments_df: pd.DataFrame,
    refunds_df: pd.DataFrame,
    shipment_events_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    fact_orders = current order state snapshot
    - paid_amount_success = sum of SUCCESS payments
    - refund_amount_total = sum refunds
    - latest_ship_status = last shipment event status
    """
    if orders_df.empty:
        return pd.DataFrame()

    base = orders_df.copy()
    base["customer_key"] = base.apply(
        lambda r: _stable_customer_key(r.get("customer_id"), r.get("customer_email"), r.get("customer_phone")),
        axis=1
    )

    # Payments aggregate
    paid = pd.DataFrame(columns=["order_id", "paid_amount_success"])
    if not payments_df.empty:
        paid = (
            payments_df[payments_df["payment_status"] == "SUCCESS"]
            .groupby("order_id", as_index=False)["amount"]
            .sum()
            .rename(columns={"amount": "paid_amount_success"})
        )

    # Refunds aggregate
    refunded = pd.DataFrame(columns=["order_id", "refund_amount_total"])
    if not refunds_df.empty:
        refunded = (
            refunds_df.groupby("order_id", as_index=False)["refund_amount"]
            .sum()
            .rename(columns={"refund_amount": "refund_amount_total"})
        )

    # Latest shipment event
    latest_ship = pd.DataFrame(columns=["order_id", "latest_ship_status", "latest_ship_status_ts"])
    if not shipment_events_df.empty:
        tmp = shipment_events_df.sort_values(["order_id", "event_ts"])
        latest = tmp.groupby("order_id", as_index=False).tail(1)
        latest_ship = latest[["order_id", "status", "event_ts"]].rename(
            columns={"status": "latest_ship_status", "event_ts": "latest_ship_status_ts"}
        )

    fact = (
        base.merge(paid, on="order_id", how="left")
            .merge(refunded, on="order_id", how="left")
            .merge(latest_ship, on="order_id", how="left")
    )

    # derived “current order state”
    fact["net_amount"] = (fact["paid_amount_success"].fillna(0) - fact["refund_amount_total"].fillna(0))
    return fact


def _fact_order_daily(fact_orders_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates per day (date_id) — you can extend this by region/currency easily.
    """
    if fact_orders_df.empty:
        return pd.DataFrame()

    df = fact_orders_df.copy()
    df["date_id"] = df["order_created_at"].dt.strftime("%Y%m%d").astype(int)

    agg = df.groupby(["date_id"], as_index=False).agg(
        orders_count=("order_id", "nunique"),
        gross_order_total=("order_total", "sum"),
        paid_amount_success=("paid_amount_success", "sum"),
        refund_amount_total=("refund_amount_total", "sum"),
        net_amount=("net_amount", "sum"),
    )
    return agg