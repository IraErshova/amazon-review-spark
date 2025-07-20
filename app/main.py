import json
import os
import redis
from fastapi import FastAPI, HTTPException
from cassandra.cluster import Cluster
from contextlib import asynccontextmanager

# TTL settings in seconds
TTL = 300  # 5 minutes

@asynccontextmanager
async def lifespan(app: FastAPI):
    redis_client = redis.Redis(
        host=os.environ.get("REDIS_HOST", "redis"),
        port=int(os.environ.get("REDIS_PORT", 6379)),
        decode_responses=True
    )
    cassandra_cluster = Cluster([os.environ.get("CASSANDRA_HOST", "cassandra")])
    cassandra_session = cassandra_cluster.connect(os.environ.get("CASSANDRA_KEYSPACE", "amazon_reviews"))
    app.state.redis_client = redis_client
    app.state.cassandra_cluster = cassandra_cluster
    app.state.cassandra_session = cassandra_session
    try:
        yield
    finally:
        try:
            cassandra_session.shutdown()
        except Exception as e:
            print(f"Error shutting down Cassandra session: {e}")
        try:
            cassandra_cluster.shutdown()
        except Exception as e:
            print(f"Error shutting down Cassandra cluster: {e}")
        try:
            redis_client.close()
        except Exception as e:
            print(f"Error closing Redis connection: {e}")

app = FastAPI(title="My API", lifespan=lifespan)

# Helper functions

def get_redis_client():
    return app.state.redis_client

def get_cassandra_session():
    return app.state.cassandra_session

def get_from_cache(key: str):
    try:
        cached_data = get_redis_client().get(key)
        if cached_data:
            return json.loads(cached_data)
        return None
    except Exception as e:
        print(f"Cache read error: {e}")
        return None

def set_in_cache(key: str, data, ttl: int):
    try:
        get_redis_client().setex(key, ttl, json.dumps(data, default=str))
        return True
    except Exception as e:
        print(f"Cache write error: {e}")
        return False

def cassandra_rows_to_dicts(rows):
    # convert Cassandra result rows to list of dicts
    return [dict(row._asdict()) for row in rows]

def get_product_reviews_from_db(product_id: str):
    query = "SELECT * FROM reviews_by_product WHERE product_id=%s"
    rows = get_cassandra_session().execute(query, (product_id,))
    return cassandra_rows_to_dicts(rows)

def get_product_reviews_by_star_from_db(product_id: str, star_rating: int):
    query = "SELECT * FROM reviews_by_product WHERE product_id=%s AND star_rating=%s"
    rows = get_cassandra_session().execute(query, (product_id, star_rating))
    return cassandra_rows_to_dicts(rows)

def get_reviews_by_customer_from_db(customer_id: int):
    query = "SELECT * FROM reviews_by_customer WHERE customer_id=%s"
    rows = get_cassandra_session().execute(query, (customer_id,))
    return cassandra_rows_to_dicts(rows)

def get_most_reviewed_items_by_date_from_db(review_date: str, limit: int):
    query = "SELECT * FROM product_review_counts_by_date WHERE review_date=%s LIMIT %s"
    rows = get_cassandra_session().execute(query, (review_date, limit))
    return cassandra_rows_to_dicts(rows)

def get_most_productive_customers_by_date_from_db(review_date: str, limit: int):
    query = "SELECT * FROM customer_verified_review_counts_by_date WHERE review_date=%s LIMIT %s"
    rows = get_cassandra_session().execute(query, (review_date, limit))
    return cassandra_rows_to_dicts(rows)

def get_most_productive_haters_by_date_from_db(review_date: str, limit: int):
    query = "SELECT * FROM customer_haters_by_date WHERE review_date=%s LIMIT %s"
    rows = get_cassandra_session().execute(query, (review_date, limit))
    return cassandra_rows_to_dicts(rows)

def get_most_productive_backers_by_date_from_db(review_date: str, limit: int):
    query = "SELECT * FROM customer_backers_by_date WHERE review_date=%s LIMIT %s"
    rows = get_cassandra_session().execute(query, (review_date, limit))
    return cassandra_rows_to_dicts(rows)

@app.get("/")
def read_root():
    return {
        "message": "Amazon Reviews API",
    }

# API Endpoints
@app.get("/product/{product_id}/reviews")
def get_product_reviews(product_id: str, no_cache: bool = False):
    cache_key = f"product_reviews:{product_id}"
    if not no_cache:
        cached_data = get_from_cache(cache_key)
        if cached_data:
            return cached_data
    result = get_product_reviews_from_db(product_id)
    if result is None:
        return {"error": "Data not found"}
    if not no_cache:
        set_in_cache(cache_key, result, TTL)
    return result

@app.get("/product/{product_id}/reviews/{star_rating}")
def get_product_reviews_by_star(product_id: str, star_rating: int, no_cache: bool = False):
    cache_key = f"product_reviews:{product_id}:star:{star_rating}"
    if not no_cache:
        cached_data = get_from_cache(cache_key)
        if cached_data:
            return cached_data
    result = get_product_reviews_by_star_from_db(product_id, star_rating)
    if result is None:
        return {"error": "Data not found"}
    if not no_cache:
        set_in_cache(cache_key, result, TTL)
    return result

@app.get("/customer/{customer_id}/reviews")
def get_reviews_by_customer(customer_id: int, no_cache: bool = False):
    cache_key = f"customer_reviews:{customer_id}"
    if not no_cache:
        cached_data = get_from_cache(cache_key)
        if cached_data:
            return cached_data
    result = get_reviews_by_customer_from_db(customer_id)
    if result is None:
        return {"error": "Data not found"}
    if not no_cache:
        set_in_cache(cache_key, result, TTL)
    return result

@app.get("/reviews/most_reviewed")
def get_most_reviewed_items_by_date(review_date: str, limit: int = 10, no_cache: bool = False):
    cache_key = f"most_reviewed:{review_date}:{limit}"
    if not no_cache:
        cached_data = get_from_cache(cache_key)
        if cached_data:
            return cached_data
    result = get_most_reviewed_items_by_date_from_db(review_date, limit)
    if result is None:
        return {"error": "Data not found"}
    if not no_cache:
        set_in_cache(cache_key, result, TTL)
    return result

@app.get("/customers/most_productive")
def get_most_productive_customers_by_date(review_date: str, limit: int = 10, no_cache: bool = False):
    cache_key = f"most_productive_customers:{review_date}:{limit}"
    if not no_cache:
        cached_data = get_from_cache(cache_key)
        if cached_data:
            return cached_data
    result = get_most_productive_customers_by_date_from_db(review_date, limit)
    if result is None:
        return {"error": "Data not found"}
    if not no_cache:
        set_in_cache(cache_key, result, TTL)
    return result

@app.get("/customers/most_haters")
def get_most_productive_haters_by_date(review_date: str, limit: int = 10, no_cache: bool = False):
    cache_key = f"most_haters:{review_date}:{limit}"
    if not no_cache:
        cached_data = get_from_cache(cache_key)
        if cached_data:
            return cached_data
    result = get_most_productive_haters_by_date_from_db(review_date, limit)
    if result is None:
        return {"error": "Data not found"}
    if not no_cache:
        set_in_cache(cache_key, result, TTL)
    return result

@app.get("/customers/most_backers")
def get_most_productive_backers_by_date(review_date: str, limit: int = 10, no_cache: bool = False):
    cache_key = f"most_backers:{review_date}:{limit}"
    if not no_cache:
        cached_data = get_from_cache(cache_key)
        if cached_data:
            return cached_data
    result = get_most_productive_backers_by_date_from_db(review_date, limit)
    if result is None:
        return {"error": "Data not found"}
    if not no_cache:
        set_in_cache(cache_key, result, TTL)
    return result

@app.get("/cache/clear")
async def clear_cache():
    try:
        await get_redis_client().flushdb()
        return {"message": "Cache cleared successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to clear cache: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
