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