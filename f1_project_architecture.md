---
aliases: [F1 Lambda Architecture, AWS Vocareum Data Pipeline]
tags: [college-project, data-engineering, aws, lambda-architecture, fastf1, kafka, spark, grafana]
creation-date: 2026-06-29
---

# F1 Telemetry: Lambda Architecture Pipeline

> [!abstract] Project Overview
> A full-scale Lambda Architecture data pipeline built to process Formula 1 telemetry. Since live race data isn't available on demand, the project uses the `fastf1` Python library to stream a historical race (2026 Austrian GP) as a "mock live" feed. The pipeline features parallel processing paths (Batch for history, Speed for live metrics) and unifies them in a self-hosted Grafana dashboard.

---

## 🏗️ Architecture Blueprint

The system splits data into two parallel paths to balance **freshness** (Speed Layer) with **correctness** (Batch Layer), merging them in the **Serving Layer**.

- **Ingestion:** Custom Python script (`fastf1`) -> Apache Kafka (Docker)
- **Speed Layer (Hot Path):** Kafka -> Apache Spark Streaming -> Amazon DynamoDB
- **Batch Layer (Cold Path):** Kafka -> Amazon Kinesis Firehose -> Amazon S3 -> Spark Batch
- **Serving Layer:** Grafana (Docker) querying DynamoDB and S3/Athena simultaneously.

---

## 🚀 Implementation Phases

### Phase 1: Data Ingestion (Mock Live Stream)
**Goal:** Simulate a real-time 100Hz F1 car telemetry stream.
- **Tool:** Python (`fastf1` library).
- **Process:** Load a completed 2026 session. Iterate through the lap and telemetry data row-by-row, applying a `time.sleep()` delay to mimic real-world timing loops.
- **Output:** Convert rows to JSON and publish to a local **Apache Kafka** topic (e.g., `f1-live-telemetry`).

### Phase 2: The Speed Layer (Low-Latency)
**Goal:** Calculate what is happening *right now*.
- **Tool:** Apache Spark Streaming (PySpark).
- **Process:** Consume the Kafka stream. Compute ultra-fast rolling metrics using sliding windows (e.g., a driver's rolling 3-lap average pace, or instantaneous throttle application).
- **Storage:** Continuously overwrite the driver's current state in **Amazon DynamoDB** for sub-second retrieval.

### Phase 3: The Batch Layer (Comprehensive History)
**Goal:** Establish the baseline truth over the entire session.
- **Tool:** Amazon Kinesis Firehose & Apache Spark (Batch).
- **Process:** 1. Firehose automatically dumps raw Kafka events directly into an **Amazon S3** Data Lake.
  2. A scheduled Spark job reads the full S3 dataset to calculate heavy metrics (e.g., cumulative tire degradation curves, theoretical best lap times).
- **Storage:** Save processed baseline views back to S3, queryable via **Amazon Athena**.

### Phase 4: The Serving Layer (Pit Wall Dashboard)
**Goal:** Unify the historical and live data into a single visual interface.
- **Tool:** Grafana (Self-hosted via Docker).
- **Process:** - Connect the **DynamoDB Data Source** to stream flashing, sub-second live telemetry.
  - Connect the **Athena Data Source** to overlay the season-long or session-long baselines.
- **Result:** A unified chart showing a driver's live lap pace against their expected session tire degradation.

---

## ⚠️ AWS Vocareum Lab Constraints & Fixes

> [!warning] Lab Limitations
> AWS student labs restrict IAM permissions, vCPU quotas, and block subscription-based services like Amazon QuickSight.

To bypass these limits without sacrificing the architecture:
- [ ] **Bypass vCPU Limits:** Do not use Amazon EMR clusters. Run Apache Spark locally on a single `t3.medium` or `m5.large` EC2 instance.
- [ ] **Bypass IAM/QuickSight Blocks:** Run **Grafana** in a Docker container on your EC2 instance alongside Kafka. 
- [ ] **Authentication:** Attach the lab-provided IAM Role (e.g., `LabRole`) to your EC2 instance. Grafana and Spark will automatically inherit permissions to read/write to S3 and DynamoDB without needing hardcoded keys.

---

## 💻 Tech Stack Summary
* **Producer:** Python, `fastf1`, Pandas
* **Message Broker:** Apache Kafka (Dockerized)
* **Stream/Batch Processing:** Apache Spark
* **Cloud Storage:** Amazon S3 (Data Lake), Amazon DynamoDB (NoSQL State)
* **Visualization:** Grafana (Dockerized)