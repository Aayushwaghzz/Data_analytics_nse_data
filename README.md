## Overview
The objective is to design, implement, and optimize a relational database capable of storing and analyzing high-volume Futures & Options (F&O) data from multiple Indian exchanges (NSE, BSE, MCX) 

The implementation uses DuckDB within a Jupyter Notebook environment to ingest a 2.5M+ row dataset, demonstrating advanced SQL querying, time-series optimization, and cross-exchange analytics

## Design Rationale & Architecture

### Normalization vs. Star Schema Avoidance
The database is structured in the 3rd Normal Form (3NF) rather than a heavily denormalized Star Schema. While Star Schemas are traditionally favored for read-heavy OLAP data warehouses, they suffer from massive write-amplification during High-Frequency Trading (HFT) ingestion. Writing repetitive string data (like exchange names and complex instrument symbols) millions of times per day creates severe disk I/O bottlenecks. 

By abstracting `Exchanges`, `Instruments`, and `Expiries` into highly normalized dimension tables , the central `Trades` fact table is kept incredibly lean, storing only numerical trading data and a single integer foreign key.

### Scalability for 10M+ Rows (HFT Ingestion)
To ensure the system scales efficiently for massive daily ingestion and time-series querying, the following optimizations were implemented:
1. **Table Partitioning:** The primary `Trades` table is designed to be partitioned by `trade_date`. This enables the query engine to utilize partition pruning, bypassing irrelevant data chunks entirely during time-bound analytical queries (e.g., the 30-day max volume query).
2.**Indexing Strategy:** Block Range Indexes (BRIN) are utilized for the `timestamp` column. For sequentially ingested time-series data, BRIN indexes store the minimum and maximum values for physical data blocks, offering significantly faster range queries with a fraction of the memory overhead compared to traditional B-Trees. B-Tree indexes are applied to `symbol` and `exchange` for rapid filtering and joins.


## Setup & Execution
1. Clone this repository.
2. Download the required "NSE Future and Options Dataset 3M" from Kaggle.
3. Place the extracted `data_analytics.csv` file in the root directory of this repository.
4. Install the required Python packages: `pip install duckdb pandas jupyter`.
5. Run all cells in `data_ingestion_analysis.ipynb` to initialize the database, ingest the data, and execute the analysis.
