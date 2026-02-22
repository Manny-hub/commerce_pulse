from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

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
