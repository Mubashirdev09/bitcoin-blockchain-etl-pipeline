# Data Dictionary — Bitcoin Blockchain ETL Pipeline

---

## What This Document Is

This document explains every column in the dataset — what it means,
where it comes from, and how it is used in the pipeline.

If you are trying to understand the data or reproduce the analysis,
start here.

---

## Source Data (Bronze Layer)

These are the original columns that come directly from the AWS
public Bitcoin blockchain dataset. Nothing is changed at this stage.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `txid` | string | Unique identifier for each transaction. Every Bitcoin transaction gets its own ID — no two are the same. | `4a5e1e4baab89f3a...` |
| `hash` | string | The transaction hash. In Bitcoin, txid and hash are usually the same value. | `4a5e1e4baab89f3a...` |
| `version` | long | Bitcoin protocol version used to create this transaction. Most transactions use version 1 or 2. | `1` |
| `size` | long | Size of the transaction in bytes. This is what actually determines the fee — not the BTC amount. | `225` |
| `virtual_size` | long | Virtual size in bytes. Used after SegWit upgrade. For older transactions this equals size. | `225` |
| `block_hash` | string | The hash of the block this transaction was included in. Links the transaction to a specific block. | `000000000019d6...` |
| `block_number` | long | The block number in the Bitcoin blockchain. Block 0 is the genesis block from January 2009. | `308684` |
| `index` | long | Position of this transaction within its block. Transaction 0 in every block is always the coinbase (mining reward). | `125` |
| `lock_time` | long | Earliest time the transaction can be added to the blockchain. Most transactions set this to 0, meaning no restriction. | `0` |
| `input_count` | long | How many inputs (senders) this transaction has. Simple transactions have 1. Complex ones can have many. | `1` |
| `output_count` | long | How many outputs (receivers) this transaction has. Usually 2 — one to the recipient, one change back to sender. | `2` |
| `is_coinbase` | boolean | Whether this is a mining reward transaction. Miners get newly created Bitcoin as a reward for each block they mine. These are filtered out in Silver layer. | `false` |
| `output_value` | double | Total BTC sent out from this transaction. Measured in BTC, not satoshis. | `0.04980851` |
| `input_value` | double | Total BTC coming into this transaction. Should always be slightly more than output_value — the difference is the fee. | `0.04990851` |
| `fee` | double | The mining fee paid. Calculated as input_value minus output_value. Goes to the miner who processes the block. | `0.0001` |
| `outputs` | array | Nested list of all output addresses and amounts. Complex structure — not used directly in analysis. | `[{address, value}]` |
| `inputs` | array | Nested list of all input addresses and previous transactions. Complex structure — not used directly in analysis. | `[{address, txid}]` |
| `block_timestamp` | timestamp | The exact date and time this transaction was confirmed on the blockchain. UTC timezone. | `2014-07-01 03:24:17` |
| `date` | date | Date partition used by AWS to organize the files. Same date as block_timestamp but date only. | `2014-07-01` |
| `last_modified` | timestamp | When the AWS dataset file was last updated. Not a blockchain field — metadata from AWS. | `2026-02-09 11:08:00` |

---

## Transformed Data (Silver Layer)

These are the columns kept after cleaning, plus three new columns
created through feature engineering.

### Columns Kept from Bronze

| Column | Why Kept |
|--------|----------|
| `txid` | Primary key — needed to identify each transaction |
| `block_timestamp` | Time dimension — needed for all time-based analysis |
| `block_number` | Links transaction to its place in the blockchain |
| `output_value` | Core metric — the BTC amount transferred |
| `input_value` | Needed to calculate fee percentage |
| `fee` | Core metric — what we are analyzing |
| `input_count` | Needed to calculate complexity ratio |
| `output_count` | Needed to calculate complexity ratio |
| `is_coinbase` | Used to filter out mining rewards |

### Columns Dropped and Why

| Column | Why Dropped |
|--------|-------------|
| `hash` | Duplicate of txid — not needed |
| `version` | Bitcoin protocol detail — not useful for analysis |
| `size` | Raw bytes — not directly meaningful for fee analysis |
| `virtual_size` | Same as size for pre-SegWit transactions |
| `block_hash` | Block level detail — not needed at transaction level |
| `index` | Position in block — not relevant to fee analysis |
| `lock_time` | Advanced Bitcoin feature — rarely used in practice |
| `outputs` | Nested complex structure — hard to work with directly |
| `inputs` | Nested complex structure — hard to work with directly |
| `date` | Covered by block_timestamp |
| `last_modified` | AWS metadata — not a blockchain field |

### New Columns Created (Feature Engineering)

| Column | Type | Formula | Description |
|--------|------|---------|-------------|
| `fee_percentage` | double | `(fee / input_value) * 100` | Fee expressed as a percentage of the total input. Makes fees comparable across transactions of different sizes. Rounded to 4 decimal places. |
| `complexity_ratio` | double | `output_count / input_count` | Ratio of outputs to inputs. A ratio of 1.0 is a simple transaction. Higher ratios indicate more complex transactions — like one sender paying many receivers. Rounded to 4 decimal places. |
| `btc_value_category` | string | See thresholds below | Groups transactions by size into four categories for easier pattern analysis. |

#### btc_value_category Thresholds

| Category | Condition | Typical User |
|----------|-----------|-------------|
| `whale` | output_value > 50 BTC | Institutions, exchanges, large investors |
| `large` | output_value > 10 BTC | Significant investors |
| `medium` | output_value > 1 BTC | Regular Bitcoin users |
| `small` | output_value <= 1 BTC | Everyday retail users |

---

## Aggregated Data (Gold Layer)

These columns represent one full year of Bitcoin activity
summarized into a single row. There are 6 rows total —
one for each year from 2013 to 2017, plus 2026.

| Column | Type | Description | Example (2017) |
|--------|------|-------------|----------------|
| `year` | integer | The year this row represents | `2017` |
| `total_transactions` | long | Total number of non-coinbase transactions that year | `3,320,902` |
| `avg_output_value` | double | Average BTC per transaction that year | `8.15` |
| `total_output_value` | double | Sum of all BTC moved that year | `27,058,792` |
| `avg_fee_percentage` | double | Average fee as % of input value that year | `3.22` |
| `avg_complexity_ratio` | double | Average outputs per input that year | `2.08` |
| `max_transaction_value` | double | Largest single transaction that year in BTC | `23,706.99` |
| `min_transaction_value` | double | Smallest transaction that year in BTC | `0.0` |

---

## Statistical Analysis Outputs

These are the results from the five statistical tests run
on the Silver layer data.

| Metric | Value | Source |
|--------|-------|--------|
| Total rows analyzed | 8,772,036 | Silver layer count |
| Mean output value | 11.13 BTC | `.describe()` |
| Max output value | 92,114 BTC | `.describe()` |
| Fee correlation with value | 0.0148 | `.stat.corr()` |
| Highest YoY growth year | 2015 (+84%) | `pct_change()` |
| Peak average fee year | 2017 (3.22%) | `groupBy().agg()` |
| Peak whale activity year | 2013 (5.54%) | `count(when())` |
| Lowest whale activity year | 2017 (2.71%) | `count(when())` |

---

## Important Notes

### On Bitcoin fees
Bitcoin fees are measured in BTC, not dollars. The same fee in BTC
can mean very different things depending on Bitcoin's price at the time.
A 0.001 BTC fee in 2013 (when Bitcoin was $100) cost about $0.10.
The same fee in 2017 (when Bitcoin was $10,000) cost $10.

### On coinbase transactions
Every block has exactly one coinbase transaction — the mining reward.
These are filtered out because they do not represent real transfers
between users. Including them would skew the analysis since they have
no input value and very high output value.

### On the sample used in Power BI
Power BI loads a 6% random sample (approximately 526,000 rows) for
performance reasons. The sample uses `seed=42` which means the same
rows are selected every time the sampling runs — making results
reproducible and consistent across refreshes.

### On data coverage
The dataset covers January 2013 to December 2017 for historical
analysis, plus daily live data from 2026 onwards pulled automatically
by Apache Airflow. Data before 2013 exists in the AWS dataset but
was not included in this project scope.
