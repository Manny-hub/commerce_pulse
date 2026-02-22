from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

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