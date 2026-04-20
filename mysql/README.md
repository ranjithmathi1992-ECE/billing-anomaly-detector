# 🏥 Hospital Billing Anomaly Detector

## Overview
Detects billing anomalies between hospital bills
and insurance claims using PySpark Big Data processing.

## Technologies
- Python 3.12
- MySQL 8.0
- Apache Hadoop 3.3.6
- Apache Hive 3.1.3
- Apache Spark 3.2.4 (PySpark)
- Apache Airflow 2.9.0

## Architecture
MySQL → Python ETL → HDFS → PySpark → Hive → Airflow

## Business Problem
- Detects overbilling cases automatically
- Flags HIGH/MEDIUM/NORMAL anomalies
- Compares hospital bills vs insurance claims
- Runs daily without manual intervention

## How to Run
1. start-dfs.sh && start-yarn.sh
2. python3 pyspark/billing_etl.py
3. spark-submit pyspark/detect_anomaly.py
4. airflow standalone
