# AtliQ Commerce — Real-Time Data Engineering Pipeline

An end-to-end real-time data engineering project using Apache Kafka, Databricks Structured Streaming, Delta Lake, and Apache Airflow.

## Architecture

**Python Producer → Confluent Cloud Kafka → Bronze → Silver → Gold → Airflow Operations**

## Tech Stack

- **Apache Kafka (Confluent Cloud):** Event ingestion
- **Python:** Order event producer
- **Databricks & PySpark:** Structured Streaming
- **Delta Lake:** Bronze, Silver, and Gold tables
- **Apache Airflow:** Workflow orchestration
- **Docker:** Local Airflow environment

## Pipeline Implementation

**Bronze:** Ingests raw Kafka messages with metadata.

**Silver:** Parses JSON, validates required fields, applies a 10-minute watermark, and deduplicates events using event ID and timestamp.

**Gold:** Aggregates payment revenue into 5-minute event-time windows.

**Airflow:** Runs freshness checks, optimizes Delta tables, and refreshes the daily summary.

## Validation Results

| Metric | Result |
|---|---:|
| Bronze events | 2,508 |
| Silver events | 2,508 |
| Duplicate event IDs in Silver | 0 |
| Missing required fields checked in Silver | 0 |
| Airflow DAG execution | Successful |
| Intentional DQ failure test | Successful |

## Project Structure

- `producer/` — Kafka producer and environment template
- `databricks/` — Bronze, Silver, and Gold streaming notebook
- `airflow/` — Airflow DAG and Docker configuration
- `screenshots/` — Execution and failure evidence
- `TECHNICAL_WRITEUP.md` — Technical explanation

## Documentation

See [Technical Write-Up](TECHNICAL_WRITEUP.md) for implementation details, design decisions, and validation findings.

## Security

Credentials are excluded from version control. Use `producer/.env.example` as a template for configuring your own Kafka connection.