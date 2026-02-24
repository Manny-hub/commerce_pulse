from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd

from src.utils.helpers import coalesce, deep_get, parse_ts, normalize_currency


def normalize_orders(raw_docs: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Accepts the full list of raw MongoDB documents (all event types).
    Filters to order events and normalises into:
      orders_df     — 1 row per order
      order_items_df — 1 row per order item

    Real payload field variations observed in data:
      historical_order:
        order_id | created_at | buyerEmail | buyerPhone | customerId
        totalAmount | currencyCode | state | address.{line1,city,country}
        line_items[].{sku, quantity, unit_price}
      order_created:
        order.id | order.ts | email | amount | ccy | geo.region
        items[].{productSku, qty, price}
      order_updated:
        order_id | updated_at | change_type  (no items — skip)
    """
    ORDER_EVENT_TYPES = {"historical_order", "order_created", "order_updated"}

    order_rows: List[Dict[str, Any]] = []
    item_rows: List[Dict[str, Any]] = []

    for doc in raw_docs:
        if doc.get("event_type") not in ORDER_EVENT_TYPES:
            continue

        payload = doc.get("payload") or {}

        # --- order_id ---
        order_id = coalesce(
            payload.get("order_id"),
            payload.get("orderRef"),
            deep_get(payload, "order.id"),
        )
        if not order_id:
            continue

        # --- timestamp ---
        raw_ts = coalesce(
            payload.get("created_at"),
            payload.get("updated_at"),
            deep_get(payload, "order.ts"),
            doc.get("event_time"),
        )
        order_created_at = parse_ts(raw_ts)

        # --- customer fields ---
        # historical_order uses buyerEmail/buyerPhone/customerId
        # order_created uses top-level email, no phone/customerId
        customer_id = coalesce(payload.get("customerId"), payload.get("customer_id"))
        customer_email = coalesce(
            payload.get("buyerEmail"),
            payload.get("email"),
            deep_get(payload, "customer.email"),
        )
        customer_phone = coalesce(
            payload.get("buyerPhone"),
            payload.get("phone"),
            deep_get(payload, "customer.phone"),
        )

        # --- totals ---
        order_total = coalesce(
            payload.get("totalAmount"),
            payload.get("total"),
            payload.get("amount"),
        )
        currency = normalize_currency(coalesce(
            payload.get("currencyCode"),
            payload.get("currency"),
            payload.get("ccy"),
        ))

        # --- region / shipping ---
        # historical_order: state + address dict
        # order_created: geo.region, no address block
        region = coalesce(
            payload.get("state"),
            deep_get(payload, "geo.region"),
            payload.get("region"),
        )

        ship_line1 = ship_city = ship_country = None
        ship_addr = coalesce(
            payload.get("address"),
            payload.get("shippingAddress"),
            deep_get(payload, "shipping.address"),
        )
        if isinstance(ship_addr, str):
            ship_line1 = ship_addr
        elif isinstance(ship_addr, dict):
            ship_line1 = ship_addr.get("line1")
            ship_city = ship_addr.get("city")
            ship_country = ship_addr.get("country")

        order_rows.append({
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
        })

        # --- items ---
        # historical_order: line_items[].{sku, quantity, unit_price}
        # order_created:    items[].{productSku, qty, price}
        items = coalesce(payload.get("line_items"), payload.get("items")) or []
        for it in items:
            sku = coalesce(it.get("sku"), it.get("productSku"))
            qty = coalesce(it.get("quantity"), it.get("qty"))
            unit_price = coalesce(it.get("unit_price"), it.get("price"))
            line_total = qty * unit_price if qty is not None and unit_price is not None else None
            item_rows.append({
                "order_id": order_id,
                "sku": sku,
                "qty": qty,
                "unit_price": unit_price,
                "line_total": line_total,
            })

    orders_df = pd.DataFrame(order_rows)
    order_items_df = pd.DataFrame(item_rows)

    if not orders_df.empty:
        orders_df["order_id"] = orders_df["order_id"].astype("string")
        orders_df = orders_df.dropna(subset=["order_id"]).drop_duplicates(
            subset=["order_id"], keep="last"
        )

    if not order_items_df.empty:
        order_items_df["order_id"] = order_items_df["order_id"].astype("string")
        order_items_df["sku"] = order_items_df["sku"].astype("string")

    return orders_df, order_items_df
