from google.cloud import bigquery
from src.bq.bq_loads import TableSpec

def bq_table_specs(project_id: str, dataset_id: str) -> dict[str, TableSpec]:
    def tid(name: str) -> str:
        return f"{project_id}.{dataset_id}.{name}"

    return {
        # -------- Dimensions --------
        "dim_customer": TableSpec(
            table_id=tid("dim_customer"),
            schema=[
                bigquery.SchemaField("customer_key", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("customer_id", "STRING"),
                bigquery.SchemaField("email", "STRING"),
                bigquery.SchemaField("phone", "STRING"),
            ],
            clustering_fields=["customer_key"],
        ),
        "dim_product": TableSpec(
            table_id=tid("dim_product"),
            schema=[
                bigquery.SchemaField("product_key", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("sku", "STRING", mode="REQUIRED"),
            ],
            clustering_fields=["product_key"],
        ),
        "dim_date": TableSpec(
            table_id=tid("dim_date"),
            schema=[
                bigquery.SchemaField("date_id", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("year", "INT64"),
                bigquery.SchemaField("month", "INT64"),
                bigquery.SchemaField("day", "INT64"),
                bigquery.SchemaField("day_of_week", "INT64"),
                bigquery.SchemaField("is_weekend", "BOOL"),
            ],
            partition_field="date",
        ),

        # -------- Facts --------
        "fact_orders": TableSpec(
            table_id=tid("fact_orders"),
            schema=[
                bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("order_created_at", "TIMESTAMP"),
                bigquery.SchemaField("customer_key", "STRING"),
                bigquery.SchemaField("order_total", "FLOAT64"),
                bigquery.SchemaField("currency", "STRING"),
                bigquery.SchemaField("region", "STRING"),
                bigquery.SchemaField("ship_line1", "STRING"),
                bigquery.SchemaField("ship_city", "STRING"),
                bigquery.SchemaField("ship_country", "STRING"),
                bigquery.SchemaField("items_count", "INT64"),
                bigquery.SchemaField("paid_amount_success", "FLOAT64"),
                bigquery.SchemaField("refund_amount_total", "FLOAT64"),
                bigquery.SchemaField("net_amount", "FLOAT64"),
            ],
            partition_field="order_created_at",
            clustering_fields=["order_id", "customer_key"],
        ),

        # append-only
        "fact_payments": TableSpec(
            table_id=tid("fact_payments"),
            schema=[
                bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("payment_ts", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("payment_status", "STRING"),
                bigquery.SchemaField("amount", "FLOAT64"),
                bigquery.SchemaField("currency", "STRING"),
                bigquery.SchemaField("method", "STRING"),
                bigquery.SchemaField("txn_id", "STRING"),
            ],
            partition_field="payment_ts",
            clustering_fields=["order_id", "txn_id"],
        ),

        "fact_refunds": TableSpec(
            table_id=tid("fact_refunds"),
            schema=[
                bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("refund_ts", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("refund_amount", "FLOAT64"),
                bigquery.SchemaField("currency", "STRING"),
                bigquery.SchemaField("reason", "STRING"),
            ],
            partition_field="refund_ts",
            clustering_fields=["order_id"],
        ),

        # append-only events
        "fact_shipments": TableSpec(
            table_id=tid("fact_shipments"),
            schema=[
                bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("carrier", "STRING"),
                bigquery.SchemaField("tracking_code", "STRING"),
                bigquery.SchemaField("status", "STRING"),
                bigquery.SchemaField("event_ts", "TIMESTAMP", mode="REQUIRED"),
            ],
            partition_field="event_ts",
            clustering_fields=["order_id", "tracking_code"],
        ),

        "fact_order_daily": TableSpec(
            table_id=tid("fact_order_daily"),
            schema=[
                bigquery.SchemaField("date_id", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("orders_count", "INT64"),
                bigquery.SchemaField("gross_order_total", "FLOAT64"),
                bigquery.SchemaField("paid_amount_success", "FLOAT64"),
                bigquery.SchemaField("refund_amount_total", "FLOAT64"),
                bigquery.SchemaField("net_amount", "FLOAT64"),
            ],
            clustering_fields=["date_id"],
        ),
    }