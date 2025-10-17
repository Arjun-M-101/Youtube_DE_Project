from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, to_date, to_timestamp
import os

spark = SparkSession.builder.appName("CSV to Silver").getOrCreate()

home = os.path.expanduser("~")
project_root = os.path.join(home, "youtube_de_project")

bronze_path = os.path.join(project_root, "bronze", "raw_statistics")
silver_path = os.path.join(project_root, "silver", "videos")

os.makedirs(silver_path, exist_ok=True)

for file in os.listdir(bronze_path):
    if file.endswith(".csv"):
        region = file[:2]
        path = f"file://{os.path.join(bronze_path, file)}"
        df = spark.read.option("header", True).csv(path)

        # Cast numeric and boolean columns properly
        df = df.withColumn("category_id", col("category_id").cast("int")) \
               .withColumn("views", col("views").cast("int")) \
               .withColumn("likes", col("likes").cast("int")) \
               .withColumn("dislikes", col("dislikes").cast("int")) \
               .withColumn("comments_disabled", col("comments_disabled").cast("boolean")) \
               .withColumn("comment_count", col("comment_count").cast("int")) \
               .withColumn("region", lit(region)) \
               .withColumn("ratings_disabled", col("ratings_disabled").cast("boolean")) \
               .withColumn("video_error_or_removed", col("video_error_or_removed").cast("boolean")) \
               .withColumn("publish_time", to_timestamp(col("publish_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
               .withColumn("trending_date", to_date(col("trending_date"), "yy.dd.MM"))

        out_file = f"file://{os.path.join(silver_path, f'{region}_videos.parquet')}"
        df.write.mode("overwrite").parquet(out_file)
        print(f"✅ Cleaned {file} → {out_file}")

spark.stop()