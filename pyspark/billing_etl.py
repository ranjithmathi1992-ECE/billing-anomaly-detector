import os
os.environ['PATH'] = '/opt/hd/bin:/opt/hd/sbin:' + os.environ['PATH']
from sqlalchemy import create_engine
import pandas as pd
import subprocess

engine = create_engine('mysql+pymysql://root:root@localhost/hospital_billing')

bills_df = pd.read_sql("SELECT * FROM hospital_bills", engine)
claims_df = pd.read_sql("SELECT * FROM insurance_claims", engine)
bills_df.to_csv('/tmp/hospital_bills.csv', index=False)
claims_df.to_csv('/tmp/insurance_claims.csv', index=False)
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/billing_anomaly/bills"])
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/billing_anomaly/claims"])
subprocess.run(["hdfs", "dfs", "-put", "-f", "/tmp/hospital_bills.csv", "/billing_anomaly/bills/"])
subprocess.run(["hdfs", "dfs", "-put", "-f", "/tmp/insurance_claims.csv", "/billing_anomaly/claims/"])

print("ETL Complete! Data loaded to HDFS!")
