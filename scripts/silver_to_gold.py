from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# ----------------------------
# Initialize Spark
# ----------------------------
spark = SparkSession.builder \
    .appName("Silver to Gold - Unified") \
    .getOrCreate()

# ----------------------------
# Config paths
# ----------------------------
regions = ["CA", "DE", "KR", "FR", "RU", "JP", "MX", "IN", "US", "GB"]

silver_videos_path = "file:///home/arjun/youtube_de_project/silver/videos/"
silver_categories_path = "file:///home/arjun/youtube_de_project/silver/categories/"
gold_path = "file:///home/arjun/youtube_de_project/gold/"

# ----------------------------
# Collect all regional Gold DataFrames
# ----------------------------
df_all = None

for region in regions:
    videos_file = f"{silver_videos_path}{region}_videos.parquet"
    categories_file = f"{silver_categories_path}{region}_categories.parquet"

    # Load silver tables
    df_videos = spark.read.parquet(videos_file)
    df_categories = spark.read.parquet(categories_file)

    # Register as temp views for Spark SQL
    df_videos.createOrReplaceTempView(f"{region}_videos")
    df_categories.createOrReplaceTempView(f"{region}_categories")

    # Join using Spark SQL
    df_gold = spark.sql(f"""
        SELECT v.*,
               c.category_name,
               (v.likes + v.comment_count) / CASE WHEN v.views = 0 THEN 1 ELSE v.views END AS engagement_ratio
        FROM {region}_videos v
        LEFT JOIN {region}_categories c
        ON v.category_id = c.id
    """)

    # Add region column
    df_gold = df_gold.withColumn("region", lit(region))

    # Union into master DataFrame
    if df_all is None:
        df_all = df_gold
    else:
        df_all = df_all.unionByName(df_gold)

    print(f"✅ Processed {region}")

# ----------------------------
# Write unified Gold dataset
# ----------------------------
if df_all:
    # Partition by region for efficiency
    df_all.write.mode("overwrite").partitionBy("region").parquet(gold_path)
    print(f"🎯 Unified Gold dataset written → {gold_path}")

# ----------------------------
# Done
# ----------------------------
spark.stop()