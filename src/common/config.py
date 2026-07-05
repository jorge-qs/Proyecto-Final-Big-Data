import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:root@localhost:27017/")
CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "yelp_analytics")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4j_password")
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "data/raw")
STAGING_PATH = os.getenv("STAGING_PATH", "data/staging")
