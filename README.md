# 🏥 Hospital Billing Anomaly Detector

## 📌 Project Overview
An advanced Big Data pipeline that automatically 
detects billing anomalies between hospital bills 
and insurance claims using Apache Spark PySpark. 
Flags HIGH, MEDIUM and NORMAL anomaly cases and 
automates daily reporting using Apache Airflow.

## 🎯 Business Problem Solved
- Hospitals overbill insurance companies unknowingly
- Manual comparison of bills vs claims is time consuming
- No automated system to detect billing mismatches
- Insurance companies lose money due to unchecked bills

## ✅ Solution Built
- Extracts hospital bills and insurance claims from MySQL
- Loads data into Hadoop HDFS for big data processing
- Uses PySpark to join and compare bills vs claims
- Flags HIGH MEDIUM NORMAL anomalies automatically
- Stores results in Hive for SQL querying
- Runs daily via Apache Airflow automation

## 🛠️ Technologies Used
| Technology | Version | Purpose |
|-----------|---------|---------|
| Python | 3.12 | ETL scripting |
| MySQL | 8.0 | Source database |
| Apache Hadoop | 3.3.6 | Distributed storage |
| Apache Hive | 3.1.3 | SQL on Big Data |
| Apache Spark PySpark | 3.2.4 | Anomaly detection |
| Apache Airflow | 2.9.0 | Pipeline automation |

## 🏗️ Architecture
MySQL → Python ETL → HDFS → PySpark → Hive → Airflow DAG

## 📊 Key Results
- 20 hospital bills processed daily
- 20 insurance claims compared automatically
- HIGH anomalies detected and flagged
- MEDIUM anomalies detected and flagged
- Pipeline runs daily without manual intervention

## 📁 Project Structure
billing_anomaly/
├── README.md
├── .gitignore
├── mysql/
│   └── create_tables.sql
├── pyspark/
│   ├── billing_etl.py
│   └── detect_anomaly.py
└── airflow/
    └── billing_anomaly_dag.py

## 🚀 How to Run
Step 1 - Start Hadoop:
start-dfs.sh and start-yarn.sh

Step 2 - Create HDFS folders:
hdfs dfs -mkdir -p /billing_anomaly/bills
hdfs dfs -mkdir -p /billing_anomaly/claims
hdfs dfs -mkdir -p /billing_anomaly/results

Step 3 - Run ETL:
python3 pyspark/billing_etl.py

Step 4 - Run PySpark Anomaly Detection:
spark-submit pyspark/detect_anomaly.py

Step 5 - Start Airflow:
airflow standalone

Step 6 - Access UI:
http://localhost:8080

## 🎯 Skills Demonstrated
- PySpark Data Processing
- Anomaly Detection Logic
- Cross Domain Analysis
- Apache Hive Querying
- Airflow Pipeline Automation
- Python Programming
- Big Data ETL
- Healthcare Finance Domain
- Hadoop HDFS Storage
- MySQL Database Design
