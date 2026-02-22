# CommercePulse ETL

## Overview

This project ingests both historical and live transactional data into MongoDB, normalizes inconsistent schemas using pandas, and loads a dimensional model into BigQuery for analytics.

The pipeline prioritizes correctness, auditability, and maintainability.

---

## Architecture

Sources (historical backfills + live updates)  
→ MongoDB (raw operational data)  
→ Extract (pymongo)  
→ Transform + Dedupe (pandas)  
→ Load (BigQuery star schema)

### BigQuery Tables

#### Dimensions
- `dim_customer`
- `dim_product`
- `dim_date`

#### Facts
- `fact_orders` (snapshot)
- `fact_payments` (append-only)
- `fact_refunds` (append-only)
- `fact_shipments` (append-only)
- `fact_order_daily` (aggregates)

---

## Design Decisions & Trade-offs

### Unified Raw Store (Historical + Live)

**Decision:** Both batch backfills and live updates are ingested into MongoDB as raw operational data.

**Trade-off:** Increased storage footprint and duplicate persistence.

**Reasoning:** Provides a single source of truth, replay capability, and consistent schema reconciliation.

---

### Batch Processing over Streaming

**Decision:** Transformations run in scheduled batch jobs (pandas), even though live data is continuously ingested.

**Trade-off:** Analytics are near-real-time, not true real-time.

**Reasoning:** Simplifies deduplication, late-arriving data handling, and snapshot recomputation while maintaining correctness.

---

### MongoDB vs BigQuery Roles

**Decision:** MongoDB stores raw operational data; BigQuery stores curated, analytics-ready tables.

**Trade-off:** Data exists in two systems.

**Reasoning:** Clear separation of operational and analytical workloads.

---

### Append-Only vs Snapshot Modeling

**Decision:**
- Event tables (`fact_payments`, `fact_refunds`, `fact_shipments`) are append-only.
- `fact_orders` is recomputed as a snapshot each run.

**Trade-off:** Snapshot recomputation increases compute cost.

**Reasoning:** Ensures deterministic results and simplifies reconciliation of updates.

---

### Pandas vs SQL

**Decision:** Transformations are implemented in pandas.

**Trade-off:** Limited horizontal scalability compared to distributed engines.

**Reasoning:** Better flexibility for inconsistent Mongo schemas and nested structures.

---

### Correctness vs Performance

**Decision:** Prioritize correctness.

- Deterministic hashing for dedupe
- Explicit timestamp normalization

**Trade-off:** Slightly higher compute cost.

**Reasoning:** Ensures reproducible analytics outputs.

---

## Assumptions

- `order_id` uniquely identifies orders.
- Payments marked `SUCCESS` represent revenue.
- All timestamps are normalized to UTC.
- Data volume fits within pandas memory constraints.
