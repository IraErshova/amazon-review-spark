# ETL.py

from ETL_mongo import run_mongo_etl
from ETL_cassandra import run_cassandra_etl

if __name__ == "__main__":
    run_mongo_etl()
    run_cassandra_etl()
