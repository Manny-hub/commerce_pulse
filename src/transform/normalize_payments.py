from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from src.utils.helpers import coalesce, deep_get, parse_ts, normalize_currency, normalize_status


def normalize_payments(raw_docs: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Accepts the full list of raw MongoDB documents (all event types).
    Filters to payment events and normalises into 1 row per payment.

    Real payload field variations observed in data:
      historical_payment:
        order_id | paid_at | payment_status | amountPaid | currencyCode
        channel | transaction_id
      payment_succeeded:
        order (string order_id) | timestamp (epoch) | state | amt | ccy
        paymentMethod | txn
    """
    PAYMENT_EVENT_TYPES = {"historical_payment", "payment_succeeded"}

    rows: List[Dict[str, Any]] = []

    for doc in raw_docs:
        if doc.get("event_type") not in PAYMENT_EVENT_TYPES:
            continue

        payload = doc.get("payload") or {}

        order_id = coalesce(
            payload.get("order_id"),
            payload.get("orderRef"),
            payload.get("order"),          # payment_succeeded uses plain string
        )

        payment_ts = parse_ts(coalesce(
            payload.get("paid_at"),
            payload.get("paidAt"),
            payload.get("timestamp"),      # epoch in payment_succeeded
        ))

        payment_status = normalize_status(coalesce(
            payload.get("payment_status"),
            payload.get("state"),
            payload.get("status"),
        ))

        amount = coalesce(
            payload.get("amountPaid"),
            payload.get("amt"),
            payload.get("amount"),
        )
        currency = normalize_currency(coalesce(
            payload.get("currencyCode"),
            payload.get("ccy"),
            payload.get("currency"),
        ))

        method = coalesce(
            payload.get("channel"),
            payload.get("paymentMethod"),
            payload.get("method"),
        )
        txn_id = coalesce(
            payload.get("transaction_id"),
            payload.get("txn"),
            payload.get("txRef"),
        )

        rows.append({
            "order_id": order_id,
            "payment_ts": payment_ts,
            "payment_status": payment_status,
            "amount": amount,
            "currency": currency,
            "method": method,
            "txn_id": txn_id,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["order_id"] = df["order_id"].astype("string")
    df["txn_id"] = df["txn_id"].astype("string")
    df["method"] = df["method"].astype("string")
    df["payment_status"] = df["payment_status"].astype("string")

    df = df.dropna(subset=["order_id", "payment_ts"])

    # Deduplicate on txn_id to prevent double-counting on re-runs
    df = df.dropna(subset=["txn_id"]).drop_duplicates(subset=["txn_id"], keep="last")

    return df
