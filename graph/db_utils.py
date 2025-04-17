# graph/db_utils.py

import os
import sys
import time

import psycopg2
from dotenv import load_dotenv
from neo4j import GraphDatabase

# --- Configuration Loading ---
load_dotenv() # Load environment variables from .env file

# Neo4j connection details
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "dev_password")

# PostgreSQL connection details
# Default DATABASE_URL for running script OUTSIDE Docker (connecting to exposed port)
DEFAULT_PG_HOST = os.getenv("PG_HOST_FROM_HOST", "localhost") # Host accessible hostname
DEFAULT_PG_PORT = os.getenv("PG_PORT_FROM_HOST", "5432")      # Host accessible port (CORRECTED based on profiles.yml)
DEFAULT_PG_USER = os.getenv("PG_USER", "postgres")          # User determined during testing
DEFAULT_PG_DB = os.getenv("PG_DATABASE", "iati")          # DB determined during testing
DEFAULT_PG_PASSWORD = os.getenv("PGPASSWORD", "dev_password")   # Default password

DEFAULT_DATABASE_URL = f"postgresql://{DEFAULT_PG_USER}:{DEFAULT_PG_PASSWORD}@{DEFAULT_PG_HOST}:{DEFAULT_PG_PORT}/{DEFAULT_PG_DB}"

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    DEFAULT_DATABASE_URL # Use the host-accessible default if env var not set
)

print(f"Using PostgreSQL Connection URL: {DATABASE_URL}") # Log the URL being used
print(f"Using Neo4j Connection URL: {NEO4J_URI}")

# --- Database Connection Functions ---

def get_neo4j_driver():
    """Establishes connection to Neo4j."""
    for attempt in range(5): # Retry mechanism
        try:
            # Ensure driver uses appropriate encryption settings if needed (e.g., encrypted=True for Aura)
            # For local testing, defaults are usually fine.
            driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            driver.verify_connectivity()
            print(f"Successfully connected to Neo4j at {NEO4J_URI}.")
            return driver
        except Exception as e:
            print(f"Attempt {attempt+1}/5: Error connecting to Neo4j at {NEO4J_URI}: {e}", file=sys.stderr)
            if "Unable to retrieve routing information" in str(e):
                print(
                    "Hint: Ensure Neo4j is running and accessible. For single instances, "
                    "try using the 'bolt://' scheme. Check firewall rules and docker network.",
                    file=sys.stderr,
                )
            elif "authentication failed" in str(e).lower():
                 print("Hint: Check Neo4j username/password (NEO4J_USER, NEO4J_PASSWORD in .env).", file=sys.stderr)
            elif "connection refused" in str(e).lower():
                 print(f"Hint: Ensure Neo4j is running and reachable at {NEO4J_URI}. Check docker logs and port mappings.", file=sys.stderr)

            if attempt < 4:
                wait_time = 2**(attempt + 1) # Exponential backoff
                print(f"Retrying connection in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                 print("Max connection attempts reached for Neo4j. Exiting.", file=sys.stderr)
                 sys.exit(1)


def get_postgres_connection():
    """Establishes connection to PostgreSQL."""
    try:
        # Use the potentially corrected DATABASE_URL
        conn = psycopg2.connect(DATABASE_URL)
        print(f"Successfully connected to PostgreSQL (Database: {conn.info.dbname}, User: {conn.info.user}).")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL using URL {DATABASE_URL}: {e}", file=sys.stderr)
        # Provide specific hints based on common errors
        if "password authentication failed" in str(e):
            print("Hint: Check PGPASSWORD or the password/user in DATABASE_URL matches DB settings.", file=sys.stderr)
        elif "database" in str(e) and "does not exist" in str(e):
             print("Hint: Ensure the database name in DATABASE_URL is correct and the DB exists.", file=sys.stderr)
        elif "connection refused" in str(e) or "server closed the connection unexpectedly" in str(e) or "could not translate host name" in str(e):
             print(f"Hint: Ensure the PostgreSQL server is running and accessible at the host/port in DATABASE_URL ({DEFAULT_PG_HOST}:{DEFAULT_PG_PORT} if DATABASE_URL == DEFAULT_DATABASE_URL else 'from env'). Check Docker container status, logs, and network.", file=sys.stderr)
        sys.exit(1) 