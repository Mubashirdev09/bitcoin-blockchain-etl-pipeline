🪙 Bitcoin Blockchain ETL Pipeline

📋 Project Overview

This project implements a full end-to-end data engineering pipeline that ingests, processes, streams, and visualizes real Bitcoin blockchain transaction data. The pipeline automatically downloads daily Bitcoin transaction data from AWS public datasets, stores it in Azure Data Lake, processes 8.7 million transactions using PySpark on Azure Synapse, streams data through Apache Kafka, orchestrates the entire workflow with Apache Airflow, and visualizes 5-year trends in a Power BI dashboard.

Architecture 



AWS Public Blockchain Dataset

&#x20;       ↓

Python (Data Download Scripts)

&#x20;       ↓

Azure Data Lake Storage Gen2 (Raw Layer)

&#x20;       ↓

Azure Synapse Analytics - PySpark (Transform Layer)

&#x20;       ↓

Azure Data Lake Storage Gen2 (Cleaned Layer)

&#x20;       ↓

Apache Kafka (Streaming Layer)

&#x20;       ↓

Apache Airflow (Orchestration Layer)

&#x20;       ↓

Power BI (Visualization Layer)



📊 Dataset

Source: AWS Public Blockchain Dataset

URL: https://aws-public-blockchain.s3.us-east-2.amazonaws.com/index.html

Format: Parquet

Coverage: 2013-2017 (5 years)

Files: 59 parquet files

Total Rows: 8.7 million Bitcoin transactions

Required Software Installations

1\. Python

download from https://www.python.org/downloads/ make sure to check "Add Python to PATH" during installation

verify installation:

bash

python --version

2\. Python Libraries

bash

pip install pandas pyarrow kafka-python azure-storage-blob

3\. Docker Desktop

download from https://www.docker.com/products/docker-desktop/

select Windows AMD64

install and restart PC

verify installation:

bash

docker --version

4\. Azure Storage Explorer

download from https://azure.microsoft.com/en-us/products/storage/storage-explorer/

used to upload files to Azure Data Lake

5\. Power BI Desktop

download from https://powerbi.microsoft.com/en-us/desktop/





Azure Setup Steps

Step 1: Create Storage Account

go to portal.azure.com

click "Create a resource" → search "Storage Account"

fill in:

Name: btcetlstorage

Region: East US

Performance: Standard

Redundancy: LRS

go to Advanced tab → enable Hierarchical namespace (ADLS Gen2)

click Review + Create → Create

Step 2: Create Container

go to your storage account

click "Containers" → "+ Container"

name: btc-transactions

click Create

Step 3: Create Synapse Workspace

search "Synapse" in Azure portal

click "+ Create"

fill in workspace name: btc-etl-synapse

link to your storage account btcetlstorage

click Review + Create → Create

Step 4: Create Spark Pool

open Synapse Studio

go to Manage → Apache Spark Pools → + New

fill in:

Name: etlsparkpool

Node size: Small

Number of nodes: 3

Auto-pause: Enabled (15 minutes)

click Review + Create → Create

📥 Section 3: Data Download



Project Folder Setup

Step 1: Download Bitcoin Parquet Files

create a file called download\_btc.py in your project folder and paste this code:

Step 3: Check for Corrupted Files

create check\_files.py:



Step 4: Upload Files to Azure Data Lake

use Azure Storage Explorer:

open Azure Storage Explorer

connect using your storage account connection string

navigate to btcetlstorage → btc-transactions

click Upload → Upload Files

select all files from data/ folder

click Upload

⚡ Section 4: Azure Synapse ETL

Overview

In this section we use PySpark on Azure Synapse to:

read all 59 parquet files from Azure Data Lake

clean and transform 8.7 million transactions

add new analytical columns

save cleaned data back to Azure

Step 2: Create Notebook

click Develop tab on left sidebar (</> icon)

click "+" → "Notebook"

at top dropdown → select etlsparkpool

make sure language is PySpark (Python)



🔄 Section 5: Apache Kafka Streaming

Overview

Apache Kafka acts as a real-time message streaming layer. The producer reads Bitcoin transactions and streams them one by one into Kafka. The consumer receives them in real time — exactly how companies like Uber and Netflix process data.

kafka\_producer.py → Kafka Topic "btc-transactions" → kafka\_consumer.py



Step 1: Start Kafka with Docker

make sure Docker Desktop is running (whale icon in taskbar) 🐳

navigate to your project folder:

bash

cd C:\\Users\\%USERNAME%\\blockchain-etl-project

create docker-compose.yml:

Step 3: Kafka Producer …  Kafka Consumer

⏰ Section 6: Apache Airflow

Overview

Apache Airflow automates the entire pipeline. Instead of manually running scripts every day, Airflow wakes up at midnight and automatically downloads new Bitcoin data, uploads it to Azure, and streams it through Kafka — all without any manual intervention.

Every day at midnight:

Airflow → download data → upload to Azure → send to Kafka

Step 1: Create Docker Compose V2

create docker-compose-v2.yml in your project folder:

Step 2: Create DAGs Folder

bash

mkdir C:\\Users\\%USERNAME%\\blockchain-etl-project\\dags



Step 3: Create DAG File

create dags\\btc\_pipeline.py:

Step 4: Start Airflow

bash

docker-compose -f docker-compose-v2.yml up -d

wait 3-4 minutes then open browser:

http://localhost:8080

login:

Username: admin

Password: admin

Step 5: Enable and Run DAG

find btc\_etl\_pipeline in DAGs list

click toggle to enable it

click ▶ play button to trigger manually

click Graph tab to see pipeline flow

expected result:

download\_btc\_data → upload\_to\_azure → send\_to\_kafka

all green ✅



Stop Airflow when done

bash

docker-compose -f docker-compose-v2.yml down



⚠️ Important Notes

always start Docker Desktop before running Airflow

Airflow runs on http://localhost:8080

DAG runs automatically every day at midnight

containers stop when PC restarts — run docker-compose -f docker-compose-v2.yml up -d to restart

📊 Section 7: Power BI Dashboard

Overview

Power BI connects to your Azure Data Lake and visualizes 5 years of Bitcoin transaction data through 4 interactive charts showing transaction volume, fee trends, value distribution and transaction size categories.

Step 2: Connect to Azure Data Lake

click "Get Data" at the top

search "Web"

paste this URL:

https://btcetlstorage.blob.core.windows.net/btc-transactions/btc\_clean.csv

click OK

click "Organizational account" → sign in with your Azure Gmail account

select the most specific URL from dropdown:

https://btcetlstorage.blob.core.windows.net/btc-transactions/btc\_clean.csv

click Connect → Load



Key Insights Discovered

📈 Transaction Volume

2013 →  35,000  transactions (Bitcoin just starting)

2014 →  45,000  transactions (growing adoption)

2015 →  95,000  transactions (recovery after Mt.Gox)

2016 → 150,000  transactions (pre-bull run)

2017 → 198,582  transactions (ALL TIME HIGH 🚀)

Bitcoin transaction volume grew 467% from 2013 to 2017!



🐳 Transaction Size Distribution

Small  (under 1 BTC)  → 69.5% → everyday users

Medium (1-10 BTC)     → 20.1% → regular investors

Large  (10-50 BTC)    → 6.9%  → big players

Whale  (over 50 BTC)  → 3.5%  → institutional investors

most Bitcoin transactions are small — showing everyday retail adoption!



💸 Fee Trends

2013-2015 → fees very low (network not busy)

2016      → fees starting to rise

2017      → fees EXPLODED 🔥

in 2017 Bitcoin network became so congested that fees went through the roof — this is visible in our data and matches real historical events!



💰 BTC Value Transferred

2013 →   719,135 BTC

2014 →   495,783 BTC (drop due to Mt.Gox hack)

2015 →   955,417 BTC (recovery)

2016 → 2,096,974 BTC (massive growth!)

2017 → 1,618,691 BTC (still huge)

2014 drop matches the famous Mt.Gox hack — the world's largest Bitcoin exchange collapsed, causing massive market panic. our data captures this real historical event! 📉



🔄 Transaction Complexity

Average complexity ratio: \~2.0

(most transactions have 1 sender → 2 receivers)

Bitcoin transactions often split payments — sender pays recipient AND sends change back to themselves, explaining the 2.0 average!

📁 Section 10: Project Structure

blockchain-etl-project/

├── data/                          # local parquet files (59 files)

├── dags/

│   └── btc\_pipeline.py           # Airflow DAG

├── docker-compose.yml             # Kafka + Zookeeper

├── docker-compose-v2.yml          # Kafka + Zookeeper + Airflow

├── download\_btc.py                # data download script

├── check\_files.py                 # corrupted file checker

├── kafka\_producer.py              # Kafka producer

├── kafka\_consumer.py              # Kafka consumer

├── test\_parquet.py                # initial data exploration

└── btc\_dashboard.pbix             # Power BI dashboard



🔑 Section 11: Important Commands Reference

Start Kafka only

bash

docker-compose up -d

docker-compose down

Start Kafka + Airflow

bash

docker-compose -f docker-compose-v2.yml up -d

docker-compose -f docker-compose-v2.yml down

Check running containers

bash

docker ps

Stream Bitcoin data

bash

\# terminal 1 - producer

python kafka\_producer.py



\# terminal 2 - consumer

python kafka\_consumer.py

Check corrupted files

bash

python check\_files.py

Airflow UI

http://localhost:8080

username: admin

password: admin

Azure Data Lake URL

abfss://btc-transactions@btcetlstorage.dfs.core.windows.net/































