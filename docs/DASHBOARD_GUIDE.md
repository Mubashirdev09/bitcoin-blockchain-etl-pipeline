# Dashboard Usage Guide — Bitcoin Blockchain ETL Pipeline

---

## What This Dashboard Is For

This dashboard was built to answer one question:

> **When were Bitcoin fees lowest — and what caused them to spike?**

If you are someone who sends Bitcoin and wants to understand fee
patterns, or a researcher studying Bitcoin network behavior —
this dashboard is for you.

It covers 5 years of real Bitcoin transaction data (2013–2017)
plus today's live data pulled automatically every day.

---

## Who Should Use This

| User | What They Get |
|------|--------------|
| Crypto investors | Understand when fees are historically low vs high |
| Traders | See how network congestion affects transaction costs |
| Researchers | Study Bitcoin adoption patterns over 5 years |
| Data engineers | Reference for blockchain ETL architecture |

---

## How to Access the Dashboard

The dashboard is a Power BI file (`btc_dashboard.pbix`).

To open it:
1. Download and install **Power BI Desktop** (free)
   → https://powerbi.microsoft.com/en-us/desktop/
2. Download `btc_dashboard.pbix` from this repository
3. Open the file in Power BI Desktop
4. Click **Refresh** to load the latest data from Azure

---

## Dashboard Pages

The dashboard has two pages. Each page tells a different part
of the Bitcoin fee story.

---

## Page 1 — Transaction Analysis

This page shows raw transaction level data from 526,000
sampled transactions across 2013–2017.

### Chart 1 — Transaction Volume Over Time

**What it shows:**
How many Bitcoin transactions happened each year.

**How to read it:**
- X axis = Year
- Y axis = Number of transactions
- A rising line means more people were using Bitcoin

**Key insight:**
Transactions grew from 568,000 in 2013 to 3.3 million in 2017 —
a 484% increase. The steepest jump happened between 2014 and 2015,
during Bitcoin's recovery from the Mt.Gox exchange collapse.

---

### Chart 2 — Transaction Size Distribution

**What it shows:**
What percentage of transactions fall into each size category.

**Categories:**
| Category | Threshold | What it means |
|----------|-----------|---------------|
| Small | Under 1 BTC | Everyday retail users |
| Medium | 1–10 BTC | Regular investors |
| Large | 10–50 BTC | Significant transfers |
| Whale | Over 50 BTC | Institutional players |

**Key insight:**
69.5% of all transactions are small — showing Bitcoin is mostly
used by everyday retail users, not just wealthy investors.

---

### Chart 3 — Total BTC Value Transferred by Year

**What it shows:**
The total amount of Bitcoin moved each year across all transactions.

**How to read it:**
- Longer bars = more BTC moved that year
- 2016 had the highest total BTC moved

**Key insight:**
2016 moved more total BTC than 2017, even though 2017 had more
transactions. This means 2016 had larger individual transactions
on average — more institutional activity before the retail boom.

---

### Chart 4 — Network Fees + Transaction Volume

**What it shows:**
A combo chart combining two things on one visual:
- Bars = transaction count per year
- Line = average fee percentage per year

**How to read it:**
When the line spikes up while bars are also high — the network
was congested and fees were expensive.

**Key insight:**
In 2017, fees jumped to 3.22% average — more than double the
previous years. This happened because transaction volume hit
its peak and the network could not keep up with demand.

---

## Page 2 — Gold Layer Insights

This page uses aggregated yearly data from the Gold layer.
Each number here represents a full year of Bitcoin activity
summarized into one row.

### Chart 1 — Average Fee % by Year

**What it shows:**
The average fee percentage paid per transaction each year.

**How to read it:**
- Higher bar = more expensive year to transact
- 2017 bar should be clearly the tallest

**Key insight:**
2013: 1.0% average fee
2017: 3.2% average fee
That is a 220% increase in fee cost over 5 years.

**What caused it:**
As more people joined the Bitcoin network, miners could charge
more because everyone was competing to get their transaction
processed faster.

---

### Chart 2 — Total BTC Moved by Year

**What it shows:**
Total Bitcoin moved across all transactions each year.

**Key insight:**
2016 peak shows institutional players moving large amounts
before the 2017 retail rush. This is often seen as a setup
phase before a bull run.

---

### Chart 3 — Total Transactions by Year

**What it shows:**
How Bitcoin adoption grew year by year.

**Key insight:**
The curve is not linear — it accelerates. This is typical of
network effect adoption. Once enough people use something,
more people join faster.

---

### Chart 4 — Avg Transaction Value by Year

**What it shows:**
The average BTC value per transaction each year.

**Key insight:**
Average transaction value DROPPED from 21 BTC in 2013 to
8 BTC in 2017. This confirms retail adoption — more people
sending smaller amounts, not fewer people sending large amounts.

---

### KPI Card — Largest Single Transaction

**What it shows:**
The biggest single Bitcoin transaction in the entire dataset.

**Value:** 92,114 BTC

**What it means:**
At Bitcoin's 2017 peak price of $20,000, this single transaction
was worth approximately $1.8 billion. These are institutional
or exchange-level movements.

---

## How to Use the Dashboard for Decision Making

### Scenario 1 — You want to send Bitcoin today
1. Look at **Chart 1 (Page 1)** — is today's volume high or low?
2. Look at **Chart 1 (Page 2)** — historically, high volume years
   had higher fees
3. If current volume is similar to 2017 levels — expect high fees
4. If volume is lower — fees are likely more manageable

### Scenario 2 — You are researching Bitcoin network health
1. Start with **Page 2** for the yearly summary
2. Use **Fee % by Year** to see network congestion history
3. Cross reference with **Transaction Volume** to confirm the pattern
4. Whale activity dropping over time confirms retail adoption

### Scenario 3 — You are presenting to stakeholders
1. Use **Page 1 Chart 4** (combo chart) — it tells the complete
   story in one visual
2. Point to the 2017 spike — fees and volume both peaked together
3. Explain the network congestion mechanic
4. Show how this pipeline captures the data automatically every day

---

## Filters and Interactions

Power BI dashboards are interactive. You can:

- **Click on a year** in any chart → all other charts filter
  to show only that year's data
- **Hover over any bar or point** → see exact numbers in tooltip
- **Click on a pie slice** → filter other charts by that category
- **Use the year filter** on Page 2 → remove 2026 (only has
  today's partial data)

---

## Refreshing the Data

The pipeline downloads new Bitcoin data every day automatically
via Apache Airflow. However, Power BI does not refresh automatically
without a Pro subscription.

To get the latest data:
1. Open the dashboard in Power BI Desktop
2. Click **Home → Refresh** at the top
3. Wait 2–3 minutes for the data to reload from Azure

---

## Known Limitations

1. **2026 data is partial** — only shows transactions downloaded
   so far this year. Filter it out for cleaner historical charts.

2. **Data covers 2013–2017** — the historical fee patterns are
   based on this period. Bitcoin fees today may differ due to
   protocol changes (SegWit, Lightning Network).

3. **Sample data** — Power BI loads a 6% random sample (526,000
   rows) for performance. The full 8.7M rows live in Azure Synapse.

4. **Manual refresh** — requires Power BI Desktop and manual
   refresh. Automated refresh requires Power BI Pro ($10/month).

---

## Data Source Reference

| Item | Detail |
|------|--------|
| Raw data | AWS Public Blockchain Dataset |
| Storage | Azure Data Lake Gen2 (btcetlstorage) |
| Processing | Azure Synapse + PySpark |
| Streaming | Apache Kafka |
| Automation | Apache Airflow |
| Coverage | January 2013 – December 2017 + daily updates |
| Total rows | 8,772,036 cleaned transactions |
