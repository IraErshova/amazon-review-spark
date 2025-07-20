-- Return all reviews for specified product_id or for specified product_id with given star_rating
CREATE TABLE IF NOT EXISTS reviews_by_product (
    product_id TEXT,
    star_rating INT,
    review_id TEXT,
    review_date DATE,
    customer_id INT,
    review_body TEXT,
    verified_purchase BOOLEAN,
    PRIMARY KEY ((product_id), star_rating, review_id)
) WITH CLUSTERING ORDER BY (star_rating ASC, review_id DESC);

-- Return all reviews for specified customer_id
CREATE TABLE IF NOT EXISTS reviews_by_customer (
    customer_id INT,
    review_date DATE,
    review_id TEXT,
    product_id TEXT,
    star_rating INT,
    review_body TEXT,
    verified_purchase BOOLEAN,
    PRIMARY KEY ((customer_id), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id DESC);

-- Return N most reviewed items (by # of reviews) for a given period of time.
CREATE TABLE IF NOT EXISTS product_review_counts_by_date (
    review_date DATE,
    product_id TEXT,
    review_count INT,
    PRIMARY KEY ((review_date), review_count, product_id)
) WITH CLUSTERING ORDER BY (review_count DESC);

-- Return N most productive customers (by # of reviews written for verified purchases) for a given period.
CREATE TABLE IF NOT EXISTS customer_verified_review_counts_by_date (
    review_date DATE,
    customer_id INT,
    review_count INT,
    PRIMARY KEY ((review_date), review_count, customer_id)
) WITH CLUSTERING ORDER BY (review_count DESC);

-- Return N most productive “haters” (by # of 1- or 2-star reviews) for a given period.
CREATE TABLE IF NOT EXISTS customer_haters_by_date (
    review_date DATE,
    customer_id INT,
    review_count INT,
    PRIMARY KEY ((review_date), review_count, customer_id)
) WITH CLUSTERING ORDER BY (review_count DESC);

-- Return N most productive “backers” (by # of 4- or 5-star reviews) for a given period.
CREATE TABLE IF NOT EXISTS customer_backers_by_date (
    review_date DATE,
    customer_id INT,
    review_count INT,
    PRIMARY KEY ((review_date), review_count, customer_id)
) WITH CLUSTERING ORDER BY (review_count DESC);
