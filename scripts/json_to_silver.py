from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, explode
import os

spark = SparkSession.builder.appName("JSON to Silver").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Dynamic project root
home = os.path.expanduser("~")
project_root = os.path.join(home, "youtube_de_project")

bronze_path = os.path.join(project_root, "bronze", "raw_statistics_reference")
silver_path = os.path.join(project_root, "silver", "categories")
os.makedirs(silver_path, exist_ok=True)

for file in os.listdir(bronze_path):
    if file.endswith(".json"):
        region = file.split("_")[0]
        path = f"file://{os.path.join(bronze_path, file)}"
        df = spark.read.option("multiLine", True).json(path)

        # Explode items array
        df_flat = df.select(explode(col("items")).alias("item"))

        # Flatten id and snippet.title as category_name, and add region
        df_flat = df_flat.select(
            col("item.id").alias("id"),
            col("item.snippet.title").alias("category_name")
        ).withColumn("region", lit(region))

        out_file = f"file://{os.path.join(silver_path, f'{region}_categories.parquet')}"
        df_flat.write.mode("overwrite").parquet(out_file)
        print(f"✅ Flattened {file} → {out_file}")

spark.stop()