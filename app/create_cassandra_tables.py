import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

CASSANDRA_KEYSPACE = "amazon_reviews"
CQL_FILE = os.path.join(os.path.dirname(__file__), '../cql_scripts/create_schemas.cql')

# Cassandra connection settings (customize as needed)
CASSANDRA_HOSTS = ["localhost"]
CASSANDRA_PORT = 9042
CASSANDRA_USER = os.environ.get("CASSANDRA_USER", "cassandra")
CASSANDRA_PASSWORD = os.environ.get("CASSANDRA_PASSWORD", "cassandra")

def create_keyspace_and_tables():
    auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
    cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT, auth_provider=auth_provider)
    session = cluster.connect()

    # Create keyspace if not exists
    session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};
    """)
    session.set_keyspace(CASSANDRA_KEYSPACE)

    # Read and execute CQL file
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
