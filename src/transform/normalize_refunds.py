from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

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