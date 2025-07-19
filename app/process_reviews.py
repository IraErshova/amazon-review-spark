from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, avg, month, year

spark = SparkSession.builder \
    .appName("AmazonReviews") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/amazon_reviews_db") \
    .config("spark.mongodb.read.connection.uri", "mongodb://mongodb:27017/amazon_reviews_db") \
    .getOrCreate()

# Load data
df = spark.read.csv("/data/amazon_reviews.csv", header=True, inferSchema=True)

# Data Cleaning
cols_to_clean = ["review_id", "product_id", "star_rating", "review_date"]
df_clean = df.dropna(subset=cols_to_clean)
df_clean = df_clean.withColumn("review_date", to_date(col("review_date"), "yyyy-MM-dd"))
df_clean = df_clean.filter(col("verified_purchase") == "1")

# Total reviews and average star rating per product
product_agg = df_clean.groupBy("product_id").agg(
    count("*").alias("total_reviews"),
    avg("star_rating").alias("avg_rating")
)

# Total verified reviews per customer
customer_agg = df_clean.groupBy("customer_id").agg(
    count("*").alias("verified_review_count")
)

# Monthly reviews per product
monthly_reviews = df_clean.withColumn("year", year("review_date")) \
                          .withColumn("month", month("review_date")) \
                          .groupBy("product_id", "year", "month") \
                          .agg(count("*").alias("monthly_review_count"))

# write data to mongoDB
product_agg.write.format("mongodb").mode("overwrite").option("collection", "product_reviews").save()
customer_agg.write.format("mongodb").mode("overwrite").option("collection", "customer_reviews").save()
monthly_reviews.write.format("mongodb").mode("overwrite").option("collection", "monthly_product_trends").save()

spark.stop()
