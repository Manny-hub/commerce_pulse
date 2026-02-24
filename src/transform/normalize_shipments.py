from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd

from src.utils.helpers import coalesce, deep_get, parse_ts, normalize_status


def normalize_shipments(raw_docs: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Accepts the full list of raw MongoDB documents (all event types).
    Filters to shipment events and normalises into:
      shipments_df        — 1 row per shipment (carrier + tracking)
      shipment_events_df  — 1 row per status event

    Real payload field variations observed in data:
      historical_shipment:
        orderRef / order_id / order.id
        carrier / logistics_partner
        tracking / tracking_code
        updates / status_history / timeline  -> [{status, time}]
      shipment_updated:
        order.id | tracking | state | ts (epoch)
        (single status event, no history array)
    """
    SHIPMENT_EVENT_TYPES = {"historical_shipment", "shipment_updated"}

    ship_rows: List[Dict[str, Any]] = []
    event_rows: List[Dict[str, Any]] = []

    for doc in raw_docs:
        if doc.get("event_type") not in SHIPMENT_EVENT_TYPES:
            continue

        payload = doc.get("payload") or {}

        order_id = coalesce(
            payload.get("order_id"),
            payload.get("orderRef"),
            deep_get(payload, "order.id"),
        )
        if not order_id:
            continue

        carrier = coalesce(
            payload.get("carrier"),
            payload.get("logistics_partner"),
        )
        tracking_code = coalesce(
            payload.get("tracking"),
            payload.get("tracking_code"),
        )

        ship_rows.append({
            "order_id": order_id,
            "carrier": carrier,
            "tracking_code": tracking_code,
        })

        # historical_shipment: list of {status, time} under updates/status_history/timeline
        # shipment_updated: single event via top-level state + ts
        event_type = doc.get("event_type")
        if event_type == "shipment_updated":
            event_rows.append({
                "order_id": order_id,
                "carrier": carrier,
                "tracking_code": tracking_code,
                "status": normalize_status(payload.get("state")),
                "event_ts": parse_ts(coalesce(payload.get("ts"), doc.get("event_time"))),
            })
        else:
            events = coalesce(
                payload.get("updates"),
                payload.get("status_history"),
                payload.get("timeline"),
            ) or []
            for ev in events:
                event_rows.append({
                    "order_id": order_id,
                    "carrier": carrier,
                    "tracking_code": tracking_code,
                    "status": normalize_status(ev.get("status")),
                    "event_ts": parse_ts(ev.get("time")),
                })

    shipments_df = pd.DataFrame(ship_rows)
    shipment_events_df = pd.DataFrame(event_rows)

    if not shipments_df.empty:
        shipments_df = shipments_df.dropna(subset=["order_id"])
        shipments_df["order_id"] = shipments_df["order_id"].astype("string")
        shipments_df = shipments_df.drop_duplicates(
            subset=["order_id", "carrier", "tracking_code"], keep="last"
        )

    if not shipment_events_df.empty:
        shipment_events_df = shipment_events_df.dropna(subset=["order_id", "event_ts"])
        shipment_events_df["order_id"] = shipment_events_df["order_id"].astype("string")
        shipment_events_df["status"] = shipment_events_df["status"].astype("string")

    return shipments_df, shipment_events_df
