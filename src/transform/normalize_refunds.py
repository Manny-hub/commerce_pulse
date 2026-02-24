from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd

from src.utils.helpers import coalesce, parse_ts, normalize_currency


def normalize_refunds(raw_docs: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Accepts the full list of raw MongoDB documents (all event types).
    Filters to refund events and normalises into:
      refunds_df      — 1 row per refund
      refund_items_df — 1 row per refunded item

    Real payload field variations observed in data:
      historical_refund:
        orderRef | refundedAt | amount | currency | reason | items (null or list)
      refund_issued:
        order_id | refunded_at | refundAmount | currencyCode
        refunded_items[].{sku, qty, amount} | reason
    """
    REFUND_EVENT_TYPES = {"historical_refund", "refund_issued"}

    refund_rows: List[Dict[str, Any]] = []
    item_rows: List[Dict[str, Any]] = []

    for doc in raw_docs:
        if doc.get("event_type") not in REFUND_EVENT_TYPES:
            continue

        payload = doc.get("payload") or {}

        order_id = coalesce(
            payload.get("order_id"),
            payload.get("orderRef"),
        )
        refund_ts = parse_ts(coalesce(
            payload.get("refunded_at"),
            payload.get("refundedAt"),
            payload.get("ts"),
        ))

        refund_amount = coalesce(
            payload.get("refundAmount"),
            payload.get("amount"),
            payload.get("amt"),
        )
        currency = normalize_currency(coalesce(
            payload.get("currencyCode"),
            payload.get("currency"),
            payload.get("ccy"),
        ))
        reason = coalesce(payload.get("reason"), payload.get("refund_reason"))

        refund_rows.append({
            "order_id": order_id,
            "refund_ts": refund_ts,
            "refund_amount": refund_amount,
            "currency": currency,
            "reason": reason,
        })

        # Items: historical_refund uses items (can be null); refund_issued uses refunded_items
        refunded_items = coalesce(
            payload.get("refunded_items"),
            payload.get("items_refunded"),
            payload.get("items"),
        ) or []   # coalesce + or [] handles None in one shot

        for it in refunded_items:
            item_rows.append({
                "order_id": order_id,
                "refund_ts": refund_ts,
                "sku": it.get("sku"),
                "qty": it.get("qty"),
                "amount": it.get("amount"),
            })

    refunds_df = pd.DataFrame(refund_rows)
    refund_items_df = pd.DataFrame(item_rows)

    if not refunds_df.empty:
        refunds_df = refunds_df.dropna(subset=["order_id", "refund_ts"])
        refunds_df["order_id"] = refunds_df["order_id"].astype("string")

    if not refund_items_df.empty:
        refund_items_df["order_id"] = refund_items_df["order_id"].astype("string")
        refund_items_df["sku"] = refund_items_df["sku"].astype("string")

    return refunds_df, refund_items_df
