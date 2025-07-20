from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, BooleanType, DateType

CASSANDRA_KEYSPACE = "amazon_reviews"
CASSANDRA_HOST = "localhost"
CSV_PATH = "../data/amazon_reviews.csv"

def main():
    spark = SparkSession.builder \
        .appName("AmazonReviewsToCassandra") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .getOrCreate()

    # Read CSV
    df = spark.read.option("header", True).csv(CSV_PATH)

    # Type conversions
    df = df.withColumn("star_rating", col("star_rating").cast(IntegerType())) \
           .withColumn("customer_id", col("customer_id").cast(IntegerType())) \
           .withColumn("verified_purchase", when(col("verified_purchase") == "Y", True).otherwise(False).cast(BooleanType())) \
           .withColumn("review_date", col("review_date").cast(DateType()))

    # Write to reviews_by_product
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="reviews_by_product", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    # Write to reviews_by_customer
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="reviews_by_customer", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    # Aggregate for product_review_counts_by_date
    product_counts = df.groupBy("review_date", "product_id").count().withColumnRenamed("count", "review_count")
    product_counts.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="product_review_counts_by_date", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    # Aggregate for customer_verified_review_counts_by_date
    verified_counts = df.filter(col("verified_purchase") == True) \
        .groupBy("review_date", "customer_id").count().withColumnRenamed("count", "review_count")
    verified_counts.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="customer_verified_review_counts_by_date", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    # Aggregate for customer_haters_by_date
    haters = df.filter(col("star_rating").isin([1, 2])) \
        .groupBy("review_date", "customer_id").count().withColumnRenamed("count", "review_count")
    haters.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="customer_haters_by_date", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    # Aggregate for customer_backers_by_date
    backers = df.filter(col("star_rating").isin([4, 5])) \
        .groupBy("review_date", "customer_id").count().withColumnRenamed("count", "review_count")
    backers.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="customer_backers_by_date", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    spark.stop()

if __name__ == "__main__":
    main() 