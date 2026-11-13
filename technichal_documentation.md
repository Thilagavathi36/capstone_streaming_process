# AtliQ Commerce — Phase 2 Technical Write-Up
**Real-Time Data Engineering Pipeline | Kafka • Databricks • Delta Lake • Airflow**

## 1. Project Overview

The objective of Phase 2 was to build a real-time data processing pipeline alongside the existing batch architecture.

The pipeline uses Confluent Cloud Kafka to ingest order events, Databricks Structured Streaming to process them through Bronze, Silver, and Gold Delta tables, and Apache Airflow to orchestrate operational tasks.

**Architecture:**

`Python Producer → Confluent Cloud Kafka → Databricks Bronze → Silver → Gold → Airflow Operations`

## 2. When Does AtliQ Need Real-Time Processing?

AtliQ needs real-time processing when business decisions depend on recent events.

- **Real-time use case:** Monitoring incoming payments, order cancellations, and revenue in near real time to identify operational issues quickly.
- **Batch use case:** Generating historical sales reports and performing long-term customer analysis, where nightly processing is sufficient.

Both approaches serve different business requirements and complement each other.

## 3. Kafka Partitioning Strategy

We used `order_id` as the Kafka message key so events belonging to the same order are routed to the same partition.

Kafka preserves message order within a partition. This helps maintain the correct sequence of related events, such as order placement, payment, and shipment.

Using random keys could distribute events from the same order across different partitions, where their relative ordering would not be guaranteed.

## 4. Databricks Streaming and Gold Windowing

The streaming pipeline follows the Medallion Architecture:

- **Bronze:** Stores raw Kafka messages, including message payloads and Kafka metadata.
- **Silver:** Parses JSON events, applies data-quality checks, and implements event-level deduplication.
- **Gold:** Aggregates payment revenue into 5-minute event-time windows.

The pipeline uses a 10-minute watermark to manage late-arriving events. Gold windows can appear later because event-time processing must account for late data, and the project runs with `availableNow=True` rather than a continuously running stream.

**Observed Gold result:** One 5-minute window contained 214 paid orders, generating revenue of 786,665.

The exact time this window became visible was not recorded.

## 5. Silver Data Quality Validation

We validated the data in Databricks SQL after processing.

| Validation | Result |
|---|---:|
| Bronze events | 2,508 |
| Silver events | 2,508 |
| Duplicate event IDs in Bronze | 0 |
| Duplicate event IDs in Silver | 0 |
| Missing required fields in Bronze | 0 |
| Null required fields in Silver | 0 |
| Records excluded from Silver | 0 |

All 2,508 events reached Silver. No duplicates or missing required fields were detected in the checked columns.

Although the pipeline implements deduplication and validation logic, this dataset did not contain records that demonstrated those filters removing events. Very late-event handling was not independently tested.

## 6. Apache Airflow Orchestration

We implemented an Airflow DAG named `atliq_streaming_ops_dag` with three sequential tasks:

1. **`check_fresh_events`** — Validates that Silver contains events from the last two hours. The task raises a custom error when no fresh events are found.
2. **`optimize_tables`** — Executes Delta Lake `OPTIMIZE` commands for Silver and Gold tables.
3. **`refresh_daily_summary`** — Refreshes the daily Gold summary containing order, payment, cancellation, and revenue metrics.

The DAG connects to Databricks using the Databricks SQL Warehouse.

**Successful execution:** All three Airflow tasks completed successfully.

**Failure testing:** We deliberately changed the freshness condition to match no events. Airflow correctly reported:

`DQ FAILED: No events received in the last 2 hours`

After validating the failure behavior, we restored the original freshness condition.

## 7. Final Outcome

The pipeline successfully processed 2,508 Kafka events into Bronze and Silver Delta tables, generated 5-minute revenue aggregations in Gold, and executed operational workflows through Airflow.

The project demonstrates event ingestion, Structured Streaming, Medallion Architecture, data-quality validation, Kafka partitioning, event-time aggregation, and workflow orchestration.

The implementation was tested using Databricks Free Edition and a locally hosted Docker-based Airflow environment.