from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import IntegerType, BooleanType, DateType

CASSANDRA_KEYSPACE = "amazon_reviews"
CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042

def run_cassandra_etl():
    def parse_date(date_str):
        if date_str is None:
            return None
        date_str = date_str.strip()
        for fmt in ("%Y-%m-%d", "%m-%d-%y", "%m/%d/%y", "%Y/%m/%d", "%d-%m-%y", "%d/%m/%y"):
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        return None

    parse_date_udf = udf(parse_date, DateType())

    spark = SparkSession.builder \
        .appName("AmazonReviewsCassandra") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .getOrCreate()

    # Load data
    df = spark.read.csv("/data/amazon_reviews.csv", header=True, inferSchema=True)

    # Type conversions
    df_clean = df.withColumn("star_rating", col("star_rating").cast(IntegerType())) \
        .withColumn("customer_id", col("customer_id").cast(IntegerType())) \
        .withColumn("verified_purchase", when(col("verified_purchase") == "1", True).otherwise(False).cast(BooleanType())) \
        .withColumn("review_date", parse_date_udf(col("review_date")))

    # Data Cleaning
    cols_to_clean = ["review_id", "product_id", "star_rating", "review_date"]
    df_clean = df_clean.dropna(subset=cols_to_clean)

    # Write to reviews_by_product
    df_clean.select(
        "product_id", "star_rating", "review_id", "review_date", "customer_id", "review_body", "verified_purchase"
    ).write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="reviews_by_product", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    # Write to reviews_by_customer
    df_clean.select(
        "customer_id", "review_date", "review_id", "product_id", "star_rating", "review_body", "verified_purchase"
    ).write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="reviews_by_customer", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    # Aggregate for product_review_counts_by_date
    product_counts = df_clean.groupBy("review_date", "product_id").count().withColumnRenamed("count", "review_count")
    product_counts.select(
        "review_date", "product_id", "review_count"
    ).write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="product_review_counts_by_date", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    # Aggregate for customer_verified_review_counts_by_date
    verified_counts = df_clean.filter(col("verified_purchase") == True) \
        .groupBy("review_date", "customer_id").count().withColumnRenamed("count", "review_count")
    verified_counts.select(
        "review_date", "customer_id", "review_count"
    ).write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="customer_verified_review_counts_by_date", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    # Aggregate for customer_haters_by_date
    haters = df_clean.filter(col("star_rating").isin([1, 2])) \
        .groupBy("review_date", "customer_id").count().withColumnRenamed("count", "review_count")
    haters.select(
        "review_date", "customer_id", "review_count"
    ).write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="customer_haters_by_date", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    # Aggregate for customer_backers_by_date
    backers = df_clean.filter(col("star_rating").isin([4, 5])) \
        .groupBy("review_date", "customer_id").count().withColumnRenamed("count", "review_count")
    backers.select(
        "review_date", "customer_id", "review_count"
    ).write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="customer_backers_by_date", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    spark.stop()

if __name__ == "__main__":
    run_cassandra_etl()
