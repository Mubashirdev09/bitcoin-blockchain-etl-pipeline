# Architecture Document — Bitcoin Blockchain ETL Pipeline

---

## 1. Overview

This document describes the high-level architecture of the Bitcoin
Blockchain ETL Pipeline. It shows how every component fits together,
how data moves through the system, and why each piece was chosen.

The goal was simple — build a pipeline that:
- collects real Bitcoin data automatically every day
- cleans and organizes it in the cloud
- makes it easy to analyze and visualize

---

## 2. High Level Architecture

```mermaid
graph TD
    A[🌐 AWS Public Blockchain\nS3 Parquet Files] -->|daily download| B[🐍 Python Scripts\ndownload_btc.py]
    B -->|local storage| C[💾 Local Machine\ndata/ folder]
    C -->|upload| D[☁️ Azure Data Lake Gen2\nbtcetlstorage]
    D -->|read| E[⚡ Azure Synapse\nPySpark ETL]
    E -->|write| D
    D -->|CSV export| F[📊 Power BI\nDashboard]
    E -->|stream| G[🔄 Apache Kafka\nbtc-transactions topic]
    H[⏰ Apache Airflow\nDAG - daily] -->|orchestrates| B
    H -->|orchestrates| D
    H -->|orchestrates| G
```

---

## 3. Medallion Architecture

The data is organized into three layers inside Azure Data Lake.
This is called Medallion Architecture — an industry standard
pattern used by companies like Databricks and Microsoft.

```mermaid
graph LR
    A[Raw AWS Data] --> B

    subgraph Bronze
        B[🥉 /bronze/\nRaw Data\n8.7M rows\n20 columns\nUntouched]
    end

    subgraph Silver
        C[🥈 /silver/\nCleaned Data\n8.7M rows\n12 columns\nTransformed]
    end

    subgraph Gold
        D[🥇 /gold/\nAggregated Data\n6 rows\n8 metrics\nBusiness Ready]
    end

    B --> C
    C --> D
    D --> E[📊 Power BI]

    style B fill:#cd7f32,color:#fff
    style C fill:#C0C0C0,color:#000
    style D fill:#FFD700,color:#000
```

### What each layer contains:

| Layer | Folder | Rows | Columns | Purpose |
|-------|--------|------|---------|---------|
| 🥉 Bronze | `/bronze/` | 8,781,456 | 20 | Raw data — never modified |
| 🥈 Silver | `/silver/` | 8,772,036 | 12 | Cleaned + feature engineered |
| 🥇 Gold | `/gold/` | 6 | 8 | Aggregated yearly insights |

---

## 4. Data Pipeline Flow

This shows exactly how data moves from source to dashboard,
step by step.

```mermaid
sequenceDiagram
    participant AWS as AWS S3
    participant PY as Python Script
    participant LOCAL as Local Storage
    participant ADLS as Azure Data Lake
    participant SPARK as Synapse Spark
    participant KAFKA as Apache Kafka
    participant BI as Power BI

    AWS->>PY: parquet file available daily
    PY->>LOCAL: download to data/ folder
    LOCAL->>ADLS: upload to bronze/ layer
    ADLS->>SPARK: read raw parquet files
    SPARK->>ADLS: write cleaned data to silver/
    SPARK->>ADLS: write aggregated data to gold/
    ADLS->>BI: export CSV for dashboard
    ADLS->>KAFKA: stream transactions
    KAFKA->>KAFKA: real-time processing
```

---

## 5. Airflow DAG Architecture

Apache Airflow automates the pipeline. The DAG runs every day
at midnight without any manual intervention.

```mermaid
graph LR
    A[⏰ Airflow Scheduler\nRuns at midnight] --> B

    subgraph DAG - btc_etl_pipeline
        B[Task 1\ndownload_btc_data\nFetch from AWS] -->|success| C
        C[Task 2\nupload_to_azure\nPush to Data Lake] -->|success| D
        D[Task 3\nsend_to_kafka\nStream transactions]
    end

    B -->|fail| E[Retry after 5 mins]
    C -->|fail| E
    D -->|fail| E
```

**DAG Configuration:**
- **Schedule:** `@daily` (every midnight)
- **Retries:** 1 retry per task
- **Retry delay:** 5 minutes
- **Start date:** January 1, 2026

---

## 6. Kafka Streaming Architecture

Apache Kafka handles real-time streaming of Bitcoin transactions.

```mermaid
graph LR
    A[🐍 kafka_producer.py\nReads parquet files] -->|JSON messages| B
    
    subgraph Kafka Cluster - Docker
        B[Zookeeper\nPort 2181] --> C
        C[Kafka Broker\nPort 9092]
        C --> D[Topic\nbtc-transactions]
    end

    D -->|consume messages| E[🐍 kafka_consumer.py\nReal-time processing]
```

**Kafka Configuration:**
- **Broker:** `localhost:9092`
- **Topic:** `btc-transactions`
- **Zookeeper:** `localhost:2181`
- **Image:** `confluentinc/cp-kafka:7.4.0`
- **Message format:** JSON

---

## 7. Azure Infrastructure

All cloud resources live under one Azure subscription.

```mermaid
graph TD
    subgraph Azure Subscription
        subgraph Storage
            A[Azure Data Lake Gen2\nbtcetlstorage\n/bronze /silver /gold]
        end

        subgraph Compute
            B[Azure Synapse Workspace\nbtc-etl-synapse]
            C[Apache Spark Pool\netlsparkpool\nSmall - 3 nodes]
        end

        subgraph Visualization
            D[Power BI Desktop\n2 page dashboard]
        end
    end

    A <-->|read/write| B
    B --> C
    A -->|CSV export| D
```

**Azure Resources:**

| Resource | Name | Type | Region |
|----------|------|------|--------|
| Storage Account | `btcetlstorage` | ADLS Gen2 | East US |
| Synapse Workspace | `btc-etl-synapse` | Analytics | East US |
| Spark Pool | `etlsparkpool` | Apache Spark | East US |

---

## 8. Local Infrastructure

Some components run locally on the developer's machine using Docker.

```mermaid
graph TD
    subgraph Docker - Local Machine
        A[Zookeeper\nconfluentinc/cp-zookeeper:7.4.0]
        B[Kafka Broker\nconfluentinc/cp-kafka:7.4.0]
        C[PostgreSQL\npostgres:13\nAirflow metadata]
        D[Apache Airflow\napache/airflow:2.6.0]
    end

    A --> B
    C --> D
    D -->|triggers| B
```

**Docker Compose Files:**
- `docker-compose.yml` → Kafka + Zookeeper only
- `docker-compose-v2.yml` → Kafka + Zookeeper + Airflow + PostgreSQL

---

## 9. Security Architecture

```mermaid
graph LR
    A[Developer] -->|Azure AD login| B[Azure Portal]
    A -->|Connection String| C[Azure Data Lake]
    A -->|admin/admin| D[Airflow UI\nlocalhost:8080]
    C -->|IAM Role\nStorage Blob Data Contributor| E[Synapse Workspace]
```

**Security Notes:**
- Azure connection strings are stored locally — never committed to GitHub
- Airflow runs locally — not exposed to internet
- Azure IAM role grants Synapse access to Data Lake
- All credentials replaced with placeholders in public code

---

## 10. Deployment Architecture

Current state vs target state:

```mermaid
graph LR
    subgraph Current State
        A1[Manual notebook run] --> B1[Manual CSV export] --> C1[Manual BI refresh]
    end

    subgraph Target State
        A2[GitHub Actions] --> B2[Airflow triggers Synapse] --> C2[Power BI auto refresh]
    end
```

### Current State:
- Data ingestion → automated via Airflow ✅
- Synapse ETL → manual (run notebook) ❌
- Power BI refresh → manual ❌

### Target State:
- Data ingestion → automated via Airflow ✅
- Synapse ETL → triggered via REST API from Airflow ❌
- Power BI refresh → scheduled via API ❌
- GitHub Actions → auto data pulls on push ❌

---

## 11. Technology Choices

| Tool | Alternative Considered | Why We Chose This |
|------|----------------------|-------------------|
| Azure Synapse | Databricks | Free with student credits |
| Apache Kafka | Azure Event Hubs | Free via Docker locally |
| Apache Airflow | Azure Data Factory | More control, open source |
| Power BI | Tableau | Microsoft ecosystem, free desktop |
| Parquet | CSV | Faster reads, smaller file size |
| Docker | Manual install | Portable, consistent environment |

---

## 12. Future Architecture

When fully deployed, the architecture will look like this:

```mermaid
graph TD
    A[AWS S3\nDaily Bitcoin Data] -->|GitHub Actions\nautomated trigger| B[Azure Data Lake\nBronze Layer]
    B -->|Airflow REST API\nauto trigger| C[Azure Synapse\nPySpark ETL]
    C -->|write| D[Azure Data Lake\nSilver + Gold Layers]
    D -->|Power BI\nscheduled refresh| E[Live Dashboard\nPublic URL]
    D -->|Azure Event Hubs\nmanaged Kafka| F[Real-time Stream]
    G[GitHub Actions\nCI/CD] -->|on push| H[Run tests\nDeploy updates]
```
