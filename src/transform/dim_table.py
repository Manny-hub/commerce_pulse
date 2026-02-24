from __future__ import annotations

import hashlib
from typing import Optional

import pandas as pd


def _stable_customer_key(
    customer_id: Optional[str],
    email: Optional[str],
    phone: Optional[str],
) -> str:
    """
    Customer IDs are inconsistent/missing, so we create a stable surrogate using best available identity.
    Priority: customer_id -> email -> phone -> "unknown"
    """
    # pandas can pass float NaN for missing string columns — cast to str safely
    def _clean(v):
        if v is None:
            return None
        s = str(v).strip()
        return s if s and s.lower() != "nan" else None

    base = _clean(customer_id) or _clean(email) or _clean(phone) or "unknown"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:24]


def _dim_customer(orders_df: pd.DataFrame) -> pd.DataFrame:
    if orders_df.empty:
        return pd.DataFrame(columns=[
            "customer_key", "customer_id", "email", "phone",
            "first_seen_ts", "last_seen_ts",
        ])

    tmp = orders_df.copy()
    tmp["customer_key"] = tmp.apply(
        lambda r: _stable_customer_key(
            r.get("customer_id"), r.get("customer_email"), r.get("customer_phone")
        ),
        axis=1,
    )

    # FIX: compute first/last seen timestamps that were declared in the schema but never built
    grp = tmp.groupby("customer_key", as_index=False).agg(
        customer_id=("customer_id", "first"),
        email=("customer_email", "first"),
        phone=("customer_phone", "first"),
        first_seen_ts=("order_created_at", "min"),
        last_seen_ts=("order_created_at", "max"),
    )
    return grp


def _dim_product(order_items_df: pd.DataFrame) -> pd.DataFrame:
    if order_items_df.empty:
        return pd.DataFrame(columns=["product_key", "sku"])

    df = order_items_df[["sku"]].dropna().drop_duplicates().copy()
    df["product_key"] = df["sku"].astype(str)
    return df[["product_key", "sku"]]


def _dim_date(min_ts: pd.Timestamp, max_ts: pd.Timestamp) -> pd.DataFrame:
    if pd.isna(min_ts) or pd.isna(max_ts):
        return pd.DataFrame(columns=[
            "date_id", "date", "year", "month", "day", "day_of_week", "is_weekend"
        ])

    start = min_ts.normalize()
    end = max_ts.normalize()
    dates = pd.date_range(start=start, end=end, freq="D", tz="UTC")
    df = pd.DataFrame({"date": dates})
    df["date_id"] = df["date"].dt.strftime("%Y%m%d").astype(int)
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["day_of_week"] = df["date"].dt.dayofweek + 1   # Mon=1 … Sun=7
    df["is_weekend"] = df["day_of_week"].isin([6, 7])
    return df[["date_id", "date", "year", "month", "day", "day_of_week", "is_weekend"]]
