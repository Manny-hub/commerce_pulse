# CommercePulse Data Engineering Case Study

**Reliable Analytics from Legacy Batch Data & Live Events**

---

## 📌 Overview

CommercePulse is an e-commerce aggregation platform operating across African markets. As the business scaled, inconsistent historical data and unreliable live events led to:

* Conflicting revenue reports
* Missing refunds
* Duplicate and late events
* No audit trail
* Unsafe reprocessing

This project implements a **hybrid, idempotent data pipeline** that supports:

* Historical batch bootstrap
* Continuous live event ingestion
* Late and duplicate handling
* Analytics-ready warehouse tables
* Full auditability

---

# 🏗 Architecture Overview

## High-Level System Architecture

```
             Historical JSON Dumps
                   (Batch)
                       │
                       ▼
               ┌────────────────┐
               │   MongoDB      │
               │  events_raw    │
               │ (Raw Store)    │
               └────────────────┘
                       ▲
                       │ Upsert (event_id)
                       │
        ┌─────────────────────────────────┐
        │                                 │
   Live Event API                   JSONL Files
   (Out-of-order,                   data/YYYY-MM-DD
    duplicates)                           │
        │                                 │
        └─────────────────────┬───────────┘
                              ▼
                       Raw Landing Layer
```

MongoDB acts as the **immutable system of record**.

---

## Transformation & Warehouse Layer

```
            MongoDB (events_raw)
                     │
                     │ Incremental Extraction
                     ▼
            Pandas Transformation
        (Normalization + Reconciliation)
                     │
                     ▼
               BigQuery Warehouse
         ┌────────────────────────────┐
         │ Dimensions                 │
         │ - dim_customer             │
         │ - dim_product              │
         │ - dim_date                 │
         │                            │
         │ Facts                      │
         │ - fact_orders              │
         │ - fact_payments            │
         │ - fact_refunds             │
         │ - fact_shipments           │
         │ - fact_order_daily         │
         └────────────────────────────┘
```

BigQuery serves business intelligence and analytics use cases.

---

# 🔹 Data Model

## MongoDB — Raw Event Schema

```
event_id        (unique, deterministic)
event_type
event_time
vendor
payload         (original record)
ingested_at
```

* Upsert by `event_id`
* No transformations applied
* Schema-flexible
* Unique index enforced

---

# 🔹 Data Flow

## Phase 1 — Historical Bootstrap

Location:

```
data/bootstrap/
```

Steps:

* Read raw JSON exports
* Wrap as synthetic events
* Generate deterministic `event_id`
* Insert into MongoDB
* Preserve original payload

No cleaning performed.

---

## Phase 2 — Live Event Ingestion

Generate:

```
python src/live_event_generator.py --out data/live_events --events 2000
```

Steps:

* Read JSONL line-by-line
* Upsert into MongoDB
* Accept duplicates, late events, schema drift

---

# 🔁 Idempotency Strategy

* Unique `event_id` index
* MongoDB upsert
* Incremental extraction
* Safe re-runs
* No full reloads

---

# 📊 Analytics Capabilities

The warehouse supports:

* Daily gross vs net revenue
* Vendor payment success rate
* Average order-to-payment time
* Refund rate (including partial refunds)
* Late-arriving event percentage
* Top products by revenue

---

# ⚖ Key Design Decisions

| Concern            | Decision                       |
| ------------------ | ------------------------------ |
| Historical vs Live | Unified as event model         |
| Raw vs Analytics   | MongoDB vs BigQuery separation |
| Orders table       | Upsert current state           |
| Payments/Refunds   | Append-only                    |
| Transform layer    | Pandas                         |
| Idempotency        | Enforced at ingestion          |

Correctness prioritized over premature optimization.

---

# 🧪 Data Quality Monitoring

Daily checks include:

* Duplicate event rate
* Late-arriving percentage
* Refund reconciliation
* Revenue drift
* Payment without order

Reports stored under:

```
reports/YYYY-MM-DD/
```

---

# 🚀 Setup

## Requirements

* Python 3.9+
* MongoDB (local)
* BigQuery dataset
* Git

## Installation

```
python -m venv venv
source venv/bin/activate
pip install pymongo pandas python-dotenv
```

## Environment Variables

```
MONGO_URI=mongodb://localhost:27017
MONGO_DB=commercepulse
```

Create index:

```
db.events_raw.createIndex({"event_id": 1}, {unique: true})
```