from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import pandas as pd
from sqlalchemy import create_engine
import os

default_args = {
    'owner': 'ranjith',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'billing_anomaly_pipeline',
    default_args=default_args,
    description='Hospital Billing Anomaly Detection Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2026, 4, 20),
    catchup=False
)

def extract_and_load():
    os.environ['PATH'] = '/opt/hd/bin:/opt/hd/sbin:' + os.environ['PATH']
    engine = create_engine('mysql+pymysql://root:YOUR_PASSWORD@localhost/hospital_billing')
    bills_df = pd.read_sql("SELECT * FROM hospital_bills", engine)
    claims_df = pd.read_sql("SELECT * FROM insurance_claims", engine)
    bills_df.to_csv('/tmp/hospital_bills.csv', index=False)
    claims_df.to_csv('/tmp/insurance_claims.csv', index=False)
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/billing_anomaly/bills"])
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/billing_anomaly/claims"])
    subprocess.run(["hdfs", "dfs", "-put", "-f", "/tmp/hospital_bills.csv", "/billing_anomaly/bills/"])
    subprocess.run(["hdfs", "dfs", "-put", "-f", "/tmp/insurance_claims.csv", "/billing_anomaly/claims/"])
    print("ETL Complete!")

task1 = PythonOperator(
    task_id='extract_load_to_hdfs',
    python_callable=extract_and_load,
    dag=dag
)

task2 = BashOperator(
    task_id='run_spark_anomaly_detection',
    bash_command='source ~/.bashrc && spark-submit ~/projects/billing_anomaly/pyspark/detect_anomaly.py',
    dag=dag
)

task3 = BashOperator(
    task_id='verify_hive_results',
    bash_command='hive -e "USE billing_db; SELECT anomaly_flag, COUNT(*) FROM billing_anomalies GROUP BY anomaly_flag;"',
    dag=dag
)

task1 >> task2 >> task3
