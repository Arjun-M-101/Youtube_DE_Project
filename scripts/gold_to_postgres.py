from pyspark.sql import SparkSession
import os

# Start Spark with JDBC driver
spark = SparkSession.builder \
    .appName("Load Gold to Postgres") \
    .config("spark.jars", "/home/arjun/youtube_de_project/postgresql-42.7.4.jar") \
    .getOrCreate()

# Path to unified Gold dataset
gold_path = "file:///home/arjun/youtube_de_project/gold"

# Read Parquet dataset (Spark infers schema automatically)
df = spark.read.parquet(gold_path)

user = os.getenv("PGUSER")
password = os.getenv("PGPASSWORD")

# JDBC connection details
jdbc_url = "jdbc:postgresql://localhost:5432/youtube_gold"
table_name = "videos_gold"
db_props = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

# Write to Postgres (auto-creates table with inferred schema)
df.write.jdbc(
    url=jdbc_url,
    table=table_name,
    mode="overwrite",   # replace table each run (use "append" if you want accumulation)
    properties=db_props
)

print("✅ Gold dataset loaded into Postgres via Spark")

spark.stop()