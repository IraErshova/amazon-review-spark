import os
from cassandra.cluster import Cluster

CQL_FILE = os.path.join(os.path.dirname(__file__), 'cql_scripts/create_schemas.cql')
# cassandra settings
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "amazon_reviews")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")

def create_keyspace_and_tables():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect()

    # Create keyspace if not exists
    session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};
    """)
    session.set_keyspace(CASSANDRA_KEYSPACE)

    # read and execute CQL file
    with open(CQL_FILE, 'r') as f:
        cql_commands = f.read().split(';')
        for command in cql_commands:
            cmd = command.strip()
            if cmd:
                session.execute(cmd)
    print("Keyspace and tables created successfully.")
    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    create_keyspace_and_tables()
