from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


# ---------------------------
# Helpers
# ---------------------------

def coalesce(*vals):
    """Return first non-null/meaningful value."""
    for v in vals:
        if v is None:
            continue
        # treat empty strings as null
        if isinstance(v, str) and v.strip() == "":
            continue
        return v
    return None


def deep_get(d: Any, path: str, default=None):
    """Safely get nested dict keys: deep_get(doc, 'shipping.address.city')."""
    cur = d
    for part in path.split("."):
        if cur is None:
            return default
        if isinstance(cur, dict):
            cur = cur.get(part, default)
        else:
            return default
    return cur


def parse_ts(value: Any) -> pd.Timestamp:
    """
    Parse mixed timestamps:
    - ISO with Z: 2023-02-19T20:43:27Z
    - ISO without Z: 2023-07-23T12:07:13
    - Slash format: 2023/04/18 17:12:36
    - Space format: 2023-06-14 04:03
    - Unix epoch seconds: 1703109546
    """
    if value is None:
        return pd.NaT

    # epoch seconds
    if isinstance(value, (int, float)) and value > 10_000_000:
        return pd.to_datetime(int(value), unit="s", utc=True)

    s = str(value).strip()

    # normalize Z
    # pandas handles "Z" with utc=True; it also handles many formats automatically
    try:
        # Try with utc parsing first
        return pd.to_datetime(s, utc=True, errors="raise")
    except Exception:
        pass

    # Try common fallback patterns
    for fmt in ("%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            ts = pd.to_datetime(s, format=fmt, errors="raise")
            # assume UTC if no tz info
            return ts.tz_localize("UTC")
        except Exception:
            continue

    # last resort: coerce
    return pd.to_datetime(s, utc=True, errors="coerce")


def normalize_currency(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip().upper()
    # basic sanity: allow NGN/USD etc
    if re.fullmatch(r"[A-Z]{3}", s):
        return s
    return s or None


def normalize_status(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip().upper()
    # common variants: SUCCESS/FAILED, sometimes "state"/"status"/"payment_status"
    return s or None


# ---------------------------
# Mongo fetch
# ---------------------------

def fetch_mongo_collection(
    mongo_uri: str,
    db_name: str,
    collection: str,
    query: Optional[Dict[str, Any]] = None,
    projection: Optional[Dict[str, int]] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch documents from MongoDB.
    Requires: pip install pymongo
    """
    from pymongo import MongoClient  # local import so script still runs without pymongo if using JSON files

    client = MongoClient(mongo_uri)
    col = client[db_name][collection]

    cursor = col.find(query or {}, projection)
    if limit:
        cursor = cursor.limit(limit)

    docs = list(cursor)
    # Optional: remove Mongo ObjectId field if you don't need it
    for d in docs:
        d.pop("_id", None)
    return docs


# ---------------------------
# Normalizers
# ---------------------------

def normalize_orders(raw_orders: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      orders_df: 1 row per order
      order_items_df: 1 row per order item
    """
    order_rows: List[Dict[str, Any]] = []
    item_rows: List[Dict[str, Any]] = []

    for doc in raw_orders:
        order_id = coalesce(
            doc.get("orderRef"),
            doc.get("order_id"),
            deep_get(doc, "order.id"),
        )

        created_raw = coalesce(doc.get("created"), doc.get("created_at"))
        order_created_at = parse_ts(created_raw)

        customer = doc.get("customer") or {}
        customer_id = customer.get("id")
        customer_email = customer.get("email")
        customer_phone = customer.get("phone")

        # totals and currency can be total/currency OR amount/ccy
        order_total = coalesce(doc.get("total"), doc.get("amount"))
        currency = normalize_currency(coalesce(doc.get("currency"), doc.get("ccy")))

        # region can be region OR geo.region
        region = coalesce(doc.get("region"), deep_get(doc, "geo.region"))

        # shipping address can be:
        # - shippingAddress: "35 GRA Rd, Kaduna" (string)
        # - shippingAddress: {line1, city, country}
        # - shipping: {address: {line1, city, country}}
        ship_line1 = ship_city = ship_country = None

        ship_addr = coalesce(doc.get("shippingAddress"), deep_get(doc, "shipping.address"))
        if isinstance(ship_addr, str):
            ship_line1 = ship_addr
        elif isinstance(ship_addr, dict):
            ship_line1 = ship_addr.get("line1")
            ship_city = ship_addr.get("city")
            ship_country = ship_addr.get("country")

        order_rows.append(
            {
                "order_id": order_id,
                "order_created_at": order_created_at,
                "customer_id": customer_id,
                "customer_email": customer_email,
                "customer_phone": customer_phone,
                "order_total": order_total,
                "currency": currency,
                "region": region,
                "ship_line1": ship_line1,
                "ship_city": ship_city,
                "ship_country": ship_country,
            }
        )

        # items
        items = doc.get("items") or []
        for it in items:
            sku = it.get("sku")
            qty = it.get("qty")
            unit_price = it.get("price")
            line_total = None
            if qty is not None and unit_price is not None:
                line_total = qty * unit_price

            item_rows.append(
                {
                    "order_id": order_id,
                    "sku": sku,
                    "qty": qty,
                    "unit_price": unit_price,
                    "line_total": line_total,
                }
            )

    orders_df = pd.DataFrame(order_rows)
    order_items_df = pd.DataFrame(item_rows)

    # Basic cleanup
    orders_df["order_id"] = orders_df["order_id"].astype("string")
    orders_df = orders_df.dropna(subset=["order_id"]).drop_duplicates(subset=["order_id"], keep="last")

    order_items_df["order_id"] = order_items_df["order_id"].astype("string")
    order_items_df["sku"] = order_items_df["sku"].astype("string")

    return orders_df, order_items_df


def normalize_payments(raw_payments: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for doc in raw_payments:
        order_id = coalesce(doc.get("orderRef"), doc.get("order_id"), doc.get("order"))

        # time can be timestamp epoch OR paid_at OR paidAt
        payment_ts = parse_ts(coalesce(doc.get("timestamp"), doc.get("paid_at"), doc.get("paidAt")))

        # status can be state OR payment_status OR status
        payment_status = normalize_status(coalesce(doc.get("state"), doc.get("payment_status"), doc.get("status")))

        amount = coalesce(doc.get("amt"), doc.get("amountPaid"), doc.get("amount"))
        currency = normalize_currency(coalesce(doc.get("ccy"), doc.get("currencyCode"), doc.get("currency")))

        method = coalesce(doc.get("paymentMethod"), doc.get("channel"), doc.get("method"))
        txn_id = coalesce(doc.get("txn"), doc.get("transaction_id"), doc.get("txRef"))

        rows.append(
            {
                "order_id": order_id,
                "payment_ts": payment_ts,
                "payment_status": payment_status,
                "amount": amount,
                "currency": currency,
                "method": method,
                "txn_id": txn_id,
            }
        )

    df = pd.DataFrame(rows)
    df["order_id"] = df["order_id"].astype("string")
    df["txn_id"] = df["txn_id"].astype("string")
    df["method"] = df["method"].astype("string")
    df["payment_status"] = df["payment_status"].astype("string")

    # remove obvious junk
    df = df.dropna(subset=["order_id", "payment_ts"])
    return df


def normalize_refunds(raw_refunds: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      refunds_df: 1 row per refund record
      refund_items_df: 1 row per refunded item (if any)
    """
    refund_rows: List[Dict[str, Any]] = []
    item_rows: List[Dict[str, Any]] = []

    for doc in raw_refunds:
        order_id = coalesce(doc.get("orderRef"), doc.get("order_id"), doc.get("order"))
        refund_ts = parse_ts(coalesce(doc.get("ts"), doc.get("refunded_at"), doc.get("refundedAt")))

        refund_amount = coalesce(doc.get("amt"), doc.get("refundAmount"), doc.get("amount"))
        currency = normalize_currency(coalesce(doc.get("ccy"), doc.get("currencyCode"), doc.get("currency")))
        reason = coalesce(doc.get("reason"), doc.get("refund_reason"))

        refund_rows.append(
            {
                "order_id": order_id,
                "refund_ts": refund_ts,
                "refund_amount": refund_amount,
                "currency": currency,
                "reason": reason,
            }
        )

        refunded_items = coalesce(doc.get("items_refunded"), doc.get("refunded_items"), doc.get("items")) or []
        if refunded_items is None:
            refunded_items = []

        for it in refunded_items:
            item_rows.append(
                {
                    "order_id": order_id,
                    "refund_ts": refund_ts,
                    "sku": it.get("sku"),
                    "qty": it.get("qty"),
                    "amount": it.get("amount"),
                }
            )

    refunds_df = pd.DataFrame(refund_rows).dropna(subset=["order_id", "refund_ts"])
    refund_items_df = pd.DataFrame(item_rows)

    refunds_df["order_id"] = refunds_df["order_id"].astype("string")
    refund_items_df["order_id"] = refund_items_df["order_id"].astype("string")
    refund_items_df["sku"] = refund_items_df["sku"].astype("string")

    return refunds_df, refund_items_df


def normalize_shipments(raw_shipments: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      shipments_df: 1 row per shipment record
      shipment_events_df: 1 row per shipment event (status/time)
    """
    ship_rows: List[Dict[str, Any]] = []
    event_rows: List[Dict[str, Any]] = []

    for doc in raw_shipments:
        order_id = coalesce(doc.get("orderRef"), doc.get("order_id"), deep_get(doc, "order.id"))
        carrier = coalesce(doc.get("carrier"), doc.get("logistics_partner"))
        tracking_code = coalesce(doc.get("tracking"), doc.get("tracking_code"))

        ship_rows.append(
            {
                "order_id": order_id,
                "carrier": carrier,
                "tracking_code": tracking_code,
            }
        )

        events = coalesce(doc.get("updates"), doc.get("status_history"), doc.get("timeline")) or []
        for ev in events:
            event_rows.append(
                {
                    "order_id": order_id,
                    "carrier": carrier,
                    "tracking_code": tracking_code,
                    "status": normalize_status(ev.get("status")),
                    "event_ts": parse_ts(ev.get("time")),
                }
            )

    shipments_df = pd.DataFrame(ship_rows).dropna(subset=["order_id"])
    shipments_df["order_id"] = shipments_df["order_id"].astype("string")

    shipment_events_df = pd.DataFrame(event_rows).dropna(subset=["order_id", "event_ts"])
    shipment_events_df["order_id"] = shipment_events_df["order_id"].astype("string")
    shipment_events_df["status"] = shipment_events_df["status"].astype("string")

    # A shipment might appear multiple times with different tracking; keep distinct combos
    shipments_df = shipments_df.drop_duplicates(subset=["order_id", "carrier", "tracking_code"], keep="last")

    return shipments_df, shipment_events_df


# ---------------------------
# Example runner
# ---------------------------

def load_json_list(path: str) -> List[Dict[str, Any]]:
    import json
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


if __name__ == "__main__":
    # Option A) Test with local exports (your uploaded files)
    orders_raw = load_json_list("/mnt/data/orders_2023.json")
    payments_raw = load_json_list("/mnt/data/payments_2023.json")
    refunds_raw = load_json_list("/mnt/data/refunds_2023.json")
    shipments_raw = load_json_list("/mnt/data/shipments_2023.json")

    orders_df, order_items_df = normalize_orders(orders_raw)
    payments_df = normalize_payments(payments_raw)
    refunds_df, refund_items_df = normalize_refunds(refunds_raw)
    shipments_df, shipment_events_df = normalize_shipments(shipments_raw)

    # Option B) Fetch from Mongo instead (uncomment and configure)
    # mongo_uri = "mongodb://user:pass@host:27017"
    # db_name = "commercepulse"
    # orders_raw = fetch_mongo_collection(mongo_uri, db_name, "orders_2023")
    # payments_raw = fetch_mongo_collection(mongo_uri, db_name, "payments_2023")
    # refunds_raw = fetch_mongo_collection(mongo_uri, db_name, "refunds_2023")
    # shipments_raw = fetch_mongo_collection(mongo_uri, db_name, "shipments_2023")

    # Useful integrity checks (optional)
    print("orders:", len(orders_df), "order_items:", len(order_items_df))
    print("payments:", len(payments_df), "refunds:", len(refunds_df), "refund_items:", len(refund_items_df))
    print("shipments:", len(shipments_df), "shipment_events:", len(shipment_events_df))

    # Example: build a clean "order fact" view by joining aggregates
    payments_agg = (
        payments_df[payments_df["payment_status"] == "SUCCESS"]
        .groupby("order_id", as_index=False)["amount"]
        .sum()
        .rename(columns={"amount": "paid_amount_success"})
    )

    refunds_agg = (
        refunds_df.groupby("order_id", as_index=False)["refund_amount"]
        .sum()
        .rename(columns={"refund_amount": "refund_amount_total"})
    )

    latest_ship_status = (
        shipment_events_df.sort_values(["order_id", "event_ts"])
        .groupby("order_id", as_index=False)
        .tail(1)[["order_id", "status", "event_ts"]]
        .rename(columns={"status": "latest_ship_status", "event_ts": "latest_ship_status_ts"})
    )

    fact_order = (
        orders_df
        .merge(payments_agg, on="order_id", how="left")
        .merge(refunds_agg, on="order_id", how="left")
        .merge(latest_ship_status, on="order_id", how="left")
    )

    print(fact_order.head(5))