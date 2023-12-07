# Build real-time view in Data Lake

This repository contains Flink jobs to process data with a help of Apache Paimon.

Pre-requisites:
1. MySQL server 8.x
2. [Ammonite](https://ammonite.io/)
3. Apache Flink cluster 1.17 or higher

## Ingestion jobs

1. Create MySQL Database and apply `mysql.sql` DDL script to it.
2. Create Flink SQL jobs using `flink-sql/ingestion.sql` code.
3. Run Flink CDC job using Ammonite: `amm runCdcAction.sc`

## Aggregation jobs

Create Flink SQL job to aggregate data using `flink-sql/aggregation.sql`.

## Visualize data view

Run Paimon table stream-reader to print current data continuously in the console using Ammonite: `amm readStream.sc`.


## Sync via Merge action

In order to sync country_sales table with customers run the Paimon Merge action via Ammonite: `amm mergeDiff.sc`.