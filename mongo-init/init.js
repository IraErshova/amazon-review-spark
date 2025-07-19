db = db.getSiblingDB("amazon_reviews_db");

db.createCollection("product_reviews");
db.product_stats.createIndex({ product_id: 1 });

db.createCollection("customer_reviews");
db.customer_stats.createIndex({ customer_id: 1 });

db.createCollection("monthly_product_trends");
db.monthly_product_trends.createIndex({ product_id: 1, year: 1, month: 1 });
