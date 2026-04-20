import os
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'
os.environ['SPARK_HOME'] = '/opt/spark'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs, round, when

# Create Spark Session
# Think of SparkSession as opening a connection to Spark engine
# appName gives your application a name
spark = SparkSession.builder \
    .appName("BillingAnomalyDetector") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to ERROR only - reduces noise in output
spark.sparkContext.setLogLevel("ERROR")

print("Spark Session Created!")

# Read bills data from HDFS into Spark DataFrame
# A Spark DataFrame is like pandas DataFrame but distributed across cluster
bills = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/billing_anomaly/bills/hospital_bills.csv")

# Read claims data from HDFS
claims = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/billing_anomaly/claims/insurance_claims.csv")

print("Data loaded from HDFS!")
print(f"Total Bills: {bills.count()}")
print(f"Total Claims: {claims.count()}")

# JOIN bills and claims on bill_id
# This combines both tables into one - like SQL JOIN
# We use "inner" join - only matching records from both tables
joined = bills.join(claims, "bill_id", "inner")

print("Tables joined successfully!")

# Calculate anomaly score
# difference = how much the bill differs from claimed amount
# percentage_diff = difference as percentage of bill amount
anomaly_df = joined.withColumn(
    "difference",
    round(col("bill_amount") - col("claimed_amount"), 2)
).withColumn(
    "percentage_diff",
    round((abs(col("bill_amount") - col("claimed_amount")) / col("bill_amount")) * 100, 2)
).withColumn(
    "anomaly_flag",
    when(col("percentage_diff") > 20, "HIGH ANOMALY")
    .when(col("percentage_diff") > 10, "MEDIUM ANOMALY")
    .otherwise("NORMAL")
)

# Show results
print("\n=== ANOMALY DETECTION RESULTS ===")
anomaly_df.select(
    "bill_id",
    "patient_name",
    "treatment",
    "bill_amount",
    "claimed_amount",
    "difference",
    "percentage_diff",
    "anomaly_flag"
).show(20, truncate=False)

# Show only HIGH anomalies
print("\n=== HIGH ANOMALY CASES ===")
anomaly_df.filter(
    col("anomaly_flag") == "HIGH ANOMALY"
).select(
    "patient_name",
    "treatment",
    "bill_amount",
    "claimed_amount",
    "percentage_diff",
    "insurance_company"
).show(truncate=False)

# Summary statistics by department
print("\n=== ANOMALY SUMMARY BY DEPARTMENT ===")
anomaly_df.groupBy("department", "anomaly_flag") \
    .count() \
    .orderBy("department") \
    .show()

# Save results back to HDFS as CSV
print("Saving results to HDFS...")
anomaly_df.select(
    "bill_id",
    "patient_name",
    "treatment",
    "department",
    "bill_amount",
    "claimed_amount",
    "difference",
    "percentage_diff",
    "anomaly_flag",
    "insurance_company",
    "branch"
).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs://localhost:9000/billing_anomaly/results/")

print("Results saved to HDFS!")
print("Anomaly Detection Complete!")

# Stop Spark Session
spark.stop()
