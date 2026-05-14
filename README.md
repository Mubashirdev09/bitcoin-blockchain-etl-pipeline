# ₿ Bitcoin Blockchain ETL Pipeline

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apache-spark&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-0089D6?style=flat&logo=microsoft-azure&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat&logo=apache-kafka&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apache-airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=flat&logo=power-bi&logoColor=black)

> End-to-end automated data pipeline processing 8.7 million real Bitcoin
> transactions to answer one question — **when were Bitcoin fees lowest,
> and what caused them to spike?**

---

## The Problem

Bitcoin transaction fees are not fixed. They change based on how busy
the network is. During 2017, fees jumped from around 1% to over 3% —
a 220% increase — because too many people were transacting at the same
time and the network got congested.

Most Bitcoin users have no way to understand this pattern or use it
to make better decisions about when to transact.

This pipeline collects, processes, and visualizes 5 years of real
Bitcoin data so users can see exactly when fees were high, why they
spiked, and how network activity drove those changes.

→ Full problem statement: [docs/PROBLEM_STATEMENT.md](docs/PROBLEM_STATEMENT.md)

---

## Dashboard Preview

![Dashboard Page 1](dashboard_page1.png)
![Dashboard Page 2](dashboard_page2.png)

---

## Pipeline Architecture

```
AWS S3 (Parquet Files)
        ↓
Python Download Scripts
        ↓
Azure Data Lake Gen2
   ├── bronze/  (raw data — 8.7M rows)
   ├── silver/  (cleaned data — 8.7M rows)
   └── gold/    (aggregated by year — 6 rows)
        ↓
Azure Synapse + PySpark (ETL)
        ↓
Apache Kafka (real-time streaming)
        ↓
Apache Airflow (daily automation)
        ↓
Power BI Dashboard (2 pages)
```

---

## Key Findings

| Finding | Detail |
|---------|--------|
| Transaction growth | 484% increase from 2013 to 2017 |
| Peak fee year | 2017 — average fee hit 3.22% |
| Highest growth year | 2015 — 84% YoY growth (post Mt.Gox recovery) |
| Largest transaction | 92,114 BTC in a single transfer |
| Whale activity | Dropped from 5.54% (2013) to 2.71% (2017) as retail grew |
| Fee correlation | Fees are NOT correlated with transaction value (0.01) |

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python | Data download, scripting |
| PySpark | Big data transformation on Synapse |
| Azure Data Lake Gen2 | Cloud storage — Bronze/Silver/Gold layers |
| Azure Synapse Analytics | Cloud compute engine |
| Apache Kafka | Real-time transaction streaming |
| Apache Airflow | Daily pipeline automation |
| Docker | Containerization for Kafka and Airflow |
| Power BI | Interactive dashboard |
| Parquet | Big data file format |

---

## Project Structure

```
bitcoin-blockchain-etl-pipeline/
│
├── dags/
│   └── btc_pipeline.py          # Airflow DAG
│
├── docs/
│   ├── PROBLEM_STATEMENT.md     # Why this project exists
│   ├── TECHNICAL_DESIGN.md      # How the system works
│   ├── ARCHITECTURE.md          # System architecture + diagrams
│   ├── DASHBOARD_GUIDE.md       # How to use the dashboard
│   └── DATA_DICTIONARY.md       # Column definitions
│
├── docker-compose.yml           # Kafka + Zookeeper
├── docker-compose-v2.yml        # Kafka + Zookeeper + Airflow
├── download_btc.py              # Download Bitcoin parquet files
├── check_files.py               # Remove corrupted files
├── kafka_producer.py            # Stream transactions to Kafka
├── kafka_consumer.py            # Receive messages from Kafka
├── Notebook 1.ipynb             # Synapse PySpark ETL notebook
├── requirements.txt             # Python dependencies
├── dashboard_page1.png          # Dashboard screenshot - Page 1
├── dashboard_page2.png          # Dashboard screenshot - Page 2
└── README.md
```

---

## Quick Start

### Prerequisites

- Python 3.8+
- Docker Desktop
- Azure account (free student account works)
- Power BI Desktop (free)

### Installation

```bash
git clone https://github.com/Mubashirdev09/bitcoin-blockchain-etl-pipeline.git
cd bitcoin-blockchain-etl-pipeline
pip install -r requirements.txt
```

### Step 1 — Download Bitcoin data

```bash
python download_btc.py
```

Downloads 59 parquet files covering 2013–2017 from AWS public dataset.
Takes 30–45 minutes depending on internet speed.

### Step 2 — Check for corrupted files

```bash
python check_files.py
```

### Step 3 — Upload to Azure Data Lake

Use Azure Storage Explorer to upload all files from the `data/` folder
to your `btc-transactions` container.

### Step 4 — Run the ETL in Synapse

Open `Notebook 1.ipynb` in Azure Synapse Studio and click Run All.
This reads from bronze, transforms to silver and gold layers.

### Step 5 — Start Kafka and Airflow

```bash
docker-compose -f docker-compose-v2.yml up -d
```

Airflow UI available at: `http://localhost:8080`
Login: `admin` / `admin`

### Step 6 — Stream transactions

```bash
# Terminal 1
python kafka_producer.py

# Terminal 2
python kafka_consumer.py
```

### Step 7 — Open the dashboard

Open `btc_dashboard.pbix` in Power BI Desktop and click Refresh.

---

## Azure Setup

You will need to create these Azure resources:

| Resource | Name | Type |
|----------|------|------|
| Storage Account | `btcetlstorage` | ADLS Gen2 |
| Container | `btc-transactions` | Blob Container |
| Synapse Workspace | `btc-etl-synapse` | Synapse Analytics |
| Spark Pool | `etlsparkpool` | Apache Spark (Small, 3 nodes) |

Add your Azure connection string to the notebook and DAG file
where indicated by `YOUR_AZURE_CONNECTION_STRING_HERE`.

---

## Documentation

Full documentation is in the `docs/` folder:

- [Problem Statement](docs/PROBLEM_STATEMENT.md) — what problem this solves
- [Technical Design](docs/TECHNICAL_DESIGN.md) — how the system works
- [Architecture](docs/ARCHITECTURE.md) — system diagrams and component overview
- [Dashboard Guide](docs/DASHBOARD_GUIDE.md) — how to use the dashboard
- [Data Dictionary](docs/DATA_DICTIONARY.md) — column definitions and data types

---

## Data Source

- **Provider:** AWS Public Blockchain Dataset
- **URL:** https://aws-public-blockchain.s3.us-east-2.amazonaws.com/index.html
- **Coverage:** January 2013 – December 2017 + daily live updates
- **Format:** Parquet
- **Size:** 8.7 million transactions
- **Updated:** Every day automatically via Airflow

---

## Known Limitations

1. **Kafka-Airflow connection** — currently fails due to Docker
   networking. Fix: migrate to Azure Event Hubs.
2. **Power BI refresh** — manual only. Automated refresh needs
   Power BI Pro subscription.
3. **Synapse session timeout** — Spark session expires after 15
   minutes idle. Re-run all cells after timeout.

---

## Future Improvements

- [ ] GitHub Actions for automated daily data pulls
- [ ] Synapse REST API trigger from Airflow
- [ ] Migrate Kafka to Azure Event Hubs
- [ ] Deploy dashboard publicly on a website
- [ ] Add data quality checks
- [ ] Add unit tests
- [ ] Power BI scheduled refresh workaround

---

## License

This project is licensed under the MIT License.

---

## Author

**Syed Abdul Rahman**
- GitHub: [@Mubashirdev09](https://github.com/Mubashirdev09)
- LinkedIn: [linkedin.com/in/rahman09](https://linkedin.com/in/rahman09)
