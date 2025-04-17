# graph/load_published_activities.py

import argparse
import os
import sys
import time

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from neo4j import GraphDatabase
from tqdm import tqdm

# --- Configuration Loading ---
load_dotenv() # Load environment variables from .env file

# Neo4j connection details
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "dev_password")

# PostgreSQL connection details - **Adjust based on verified credentials**
# We connected using user='postgres', password='dev_password', database='iati', host='postgres' (service name)
# Construct the DATABASE_URL accordingly if it's not already set this way in .env
# Defaulting to the one that worked in the terminal, but using .env is preferred
# ---
# Default DATABASE_URL for running script OUTSIDE Docker (connecting to exposed port)
DEFAULT_PG_HOST = os.getenv("PG_HOST_FROM_HOST", "localhost") # Host accessible hostname
DEFAULT_PG_PORT = os.getenv("PG_PORT_FROM_HOST", "5432")      # Host accessible port (CORRECTED)
DEFAULT_PG_USER = os.getenv("PG_USER", "postgres")          # User determined during testing
DEFAULT_PG_DB = os.getenv("PG_DATABASE", "iati")          # DB determined during testing
DEFAULT_PG_PASSWORD = os.getenv("PGPASSWORD", "dev_password")   # Default password

DEFAULT_DATABASE_URL = f"postgresql://{DEFAULT_PG_USER}:{DEFAULT_PG_PASSWORD}@{DEFAULT_PG_HOST}:{DEFAULT_PG_PORT}/{DEFAULT_PG_DB}"

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    DEFAULT_DATABASE_URL # Use the host-accessible default if env var not set
)

print(f"Using PostgreSQL Connection URL: {DATABASE_URL}") # Log the URL being used

# Target Schema and Table
DBT_TARGET_SCHEMA = "iati_graph"
SOURCE_TABLE = "published_activities"
NEO4J_NODE_LABEL = "PublishedActivity"
NEO4J_ID_PROPERTY = "iatiidentifier"
NEO4J_TITLE_PROPERTY = "title"
SOURCE_TITLE_COLUMN = "title_narrative"

# Processing Batch Size
DEFAULT_BATCH_SIZE = 1000

# --- Database Connection Functions ---

def get_neo4j_driver():
    """Establishes connection to Neo4j."""
    for attempt in range(5): # Retry mechanism
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            driver.verify_connectivity()
            print(f"Successfully connected to Neo4j at {NEO4J_URI}.")
            return driver
        except Exception as e:
            print(f"Attempt {attempt+1}/5: Error connecting to Neo4j at {NEO4J_URI}: {e}", file=sys.stderr)
            if "Unable to retrieve routing information" in str(e):
                print(
                    "Hint: Ensure Neo4j is running and accessible. For single instances, "
                    "try using the 'bolt://' scheme. Check firewall rules.",
                    file=sys.stderr,
                )
            if attempt < 4:
                wait_time = 2**(attempt + 1) # Exponential backoff
                print(f"Retrying connection in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                 print("Max connection attempts reached. Exiting.", file=sys.stderr)
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
            print("Hint: Check PGPASSWORD or the password in DATABASE_URL.", file=sys.stderr)
        elif "database" in str(e) and "does not exist" in str(e):
             print("Hint: Ensure the database name in DATABASE_URL is correct.", file=sys.stderr)
        elif "connection refused" in str(e) or "server closed the connection unexpectedly" in str(e):
             print("Hint: Ensure the PostgreSQL server is running and accessible at the host/port in DATABASE_URL. Check Docker container status and network.", file=sys.stderr)
        sys.exit(1)

# --- Helper Functions ---

def get_pg_count(pg_conn, schema, table):
    """Gets the total row count from a PostgreSQL table."""
    with pg_conn.cursor() as cursor:
        try:
            cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}";')
            count = cursor.fetchone()[0]
            print(f"Expected node count from {schema}.{table}: {count}")
            return count
        except psycopg2.Error as e:
            print(f"Error getting count from {schema}.{table}: {e}", file=sys.stderr)
            # Attempt to provide more context if the relation doesn't exist
            if "relation" in str(e) and "does not exist" in str(e):
                 print(f"Hint: Ensure schema '{schema}' and table '{table}' exist in database '{pg_conn.info.dbname}'. Check dbt run completion.", file=sys.stderr)
            return None # Return None to indicate failure


def get_neo4j_node_count(neo4j_driver, label):
    """Gets the count of nodes with a specific label in Neo4j."""
    cypher = f"MATCH (n:{label}) RETURN count(n) AS count"
    try:
        with neo4j_driver.session() as session:
            result = session.run(cypher).single()
            count = result["count"] if result else 0
            print(f"Current node count for :{label} in Neo4j: {count}")
            return count
    except Exception as e:
        print(f"Error getting Neo4j node count for :{label}: {e}", file=sys.stderr)
        return None # Return None to indicate failure


def create_neo4j_constraint(neo4j_driver, label, property_key):
    """Creates a uniqueness constraint in Neo4j."""
    constraint_name = f"constraint_unique_{label}_{property_key}"
    cypher = f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS FOR (n:{label}) REQUIRE n.{property_key} IS UNIQUE"
    print(f"Applying Neo4j constraint: {constraint_name}...")
    try:
        with neo4j_driver.session() as session:
            session.run(cypher)
        print("Constraint applied successfully or already exists.")
        return True
    except Exception as e:
        print(f"Warning: Could not apply constraint '{constraint_name}'. Reason: {e}", file=sys.stderr)
        # Provide hint for specific errors if possible (e.g., Enterprise feature needed for composite keys, etc.)
        if "SyntaxError" in str(e):
             print("Hint: Check the syntax of the constraint, label, or property name.", file=sys.stderr)
        return False # Indicate failure


# --- Data Loading Function ---

def load_published_activity_nodes(pg_conn, neo4j_driver, batch_size):
    """Loads PublishedActivity nodes from PostgreSQL to Neo4j."""
    print(f"--- Loading Nodes: {DBT_TARGET_SCHEMA}.{SOURCE_TABLE} -> :{NEO4J_NODE_LABEL} ---")

    # 1. Get expected count from PostgreSQL
    expected_count = get_pg_count(pg_conn, DBT_TARGET_SCHEMA, SOURCE_TABLE)
    if expected_count is None:
        print("Cannot proceed without expected count.", file=sys.stderr)
        return False # Indicate failure

    if expected_count == 0:
        print(f"Skipping node loading - no rows found in {DBT_TARGET_SCHEMA}.{SOURCE_TABLE}.")
        return True # Indicate success (nothing to do)

    # 2. Get current count from Neo4j (before loading)
    count_before = get_neo4j_node_count(neo4j_driver, NEO4J_NODE_LABEL)
    if count_before is None:
        print("Cannot verify counts before loading.", file=sys.stderr)
        # Optionally decide whether to proceed or not
        # return False

    # 3. Create Constraint
    if not create_neo4j_constraint(neo4j_driver, NEO4J_NODE_LABEL, NEO4J_ID_PROPERTY):
         # Decide if constraint failure is critical
         print("Continuing without guaranteed constraint.", file=sys.stderr)


    # 4. Prepare PostgreSQL Cursor and Neo4j Session
    # Use server-side cursor for potentially large tables
    pg_cursor = pg_conn.cursor(name='fetch_activities', cursor_factory=psycopg2.extras.DictCursor)
    pg_cursor.itersize = batch_size # Fetch rows in chunks matching batch size

    # 5. Prepare SELECT Query
    select_cols = f'"{NEO4J_ID_PROPERTY}", "{SOURCE_TITLE_COLUMN}"' # Only select needed columns
    select_query = f'SELECT {select_cols} FROM "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}";'

    # 6. Prepare Cypher Query for Batch Loading
    # Use MERGE for idempotency based on the unique ID property
    # Set the title property
    cypher_query = f"""
    UNWIND $batch as row
    MERGE (n:{NEO4J_NODE_LABEL} {{{NEO4J_ID_PROPERTY}: row.{NEO4J_ID_PROPERTY}}})
    ON CREATE SET n.{NEO4J_TITLE_PROPERTY} = row.{SOURCE_TITLE_COLUMN}
    ON MATCH SET n.{NEO4J_TITLE_PROPERTY} = row.{SOURCE_TITLE_COLUMN} // Update title if node already exists
    """

    # 7. Execute Loading in Batches
    print(f"Executing SELECT query: {select_query}")
    try:
        pg_cursor.execute(select_query)
    except psycopg2.Error as e:
         print(f"Error executing SELECT query: {e}", file=sys.stderr)
         if "relation" in str(e) and "does not exist" in str(e):
             print(f"Hint: Ensure schema '{DBT_TARGET_SCHEMA}' and table '{SOURCE_TABLE}' exist and are accessible by user '{pg_conn.info.user}'.", file=sys.stderr)
         pg_cursor.close()
         return False


    processed_count = 0
    print(f"Starting batch load (batch size: {batch_size})...")
    with tqdm(total=expected_count, desc=f"Nodes :{NEO4J_NODE_LABEL}", unit=" nodes") as pbar:
         while True:
            try:
                batch_data = pg_cursor.fetchmany(batch_size)
            except psycopg2.Error as e:
                 print(f"Error fetching batch from PostgreSQL: {e}", file=sys.stderr)
                 # Consider breaking or attempting to recover depending on the error
                 break

            if not batch_data:
                break # End of data

            # Convert Row objects to dictionaries for Neo4j driver
            # Only include non-null identifiers
            batch_list = [
                dict(row) for row in batch_data if row[NEO4J_ID_PROPERTY] is not None
            ]

            skipped_count = len(batch_data) - len(batch_list)
            if skipped_count > 0:
                print(f"Skipped {skipped_count} rows in batch due to null '{NEO4J_ID_PROPERTY}'.")

            if not batch_list:
                pbar.update(len(batch_data)) # Update progress bar even if all skipped
                continue

            try:
                with neo4j_driver.session(database="neo4j") as session: # Explicitly use default db
                    session.execute_write(
                        lambda tx: tx.run(cypher_query, batch=batch_list)
                    )
                processed_count += len(batch_list)
                pbar.update(len(batch_data)) # Update progress based on original fetch size
            except Exception as e:
                print(f"Error processing batch in Neo4j: {e}", file=sys.stderr)
                print(f"Failed Cypher: {cypher_query}", file=sys.stderr)
                # Consider logging failed batch data: print(f"Failed batch sample: {batch_list[:2]}", file=sys.stderr)
                # Decide whether to stop or continue (e.g., skip batch and continue)
                # For now, we stop on Neo4j errors
                pg_cursor.close()
                return False


    pg_cursor.close()
    print(f"Finished batch loading. Processed {processed_count} nodes.")

    # 8. Get final count from Neo4j (after loading)
    count_after = get_neo4j_node_count(neo4j_driver, NEO4J_NODE_LABEL)
    if count_after is None:
        print("Could not verify counts after loading.", file=sys.stderr)
    else:
        print(f"Expected Count: {expected_count}")
        print(f"Count Before Load: {count_before if count_before is not None else 'N/A'}")
        print(f"Count After Load: {count_after}")
        # Check if the count matches expectation (can be tricky with MERGE if nodes existed)
        # A simple check: did the count increase by the number of *new* nodes?
        if count_before is not None and count_after is not None:
             delta = count_after - count_before
             print(f"Nodes added/updated in this run: ~{processed_count} (Neo4j count delta: {delta})")
             # Note: MERGE might update existing nodes, so delta might not exactly match processed_count if nodes already existed.
             # A more robust check might involve comparing the set of IDs from PG with IDs in Neo4j.
        if count_after != expected_count:
             print(f"WARNING: Final count ({count_after}) does not match expected count ({expected_count}). This might be due to pre-existing nodes, null IDs, or loading errors.", file=sys.stderr)


    return True # Indicate success


# --- Main Execution ---

def main():
    parser = argparse.ArgumentParser(
        description=f"Load {NEO4J_NODE_LABEL} nodes from PostgreSQL ({DBT_TARGET_SCHEMA}.{SOURCE_TABLE}) to Neo4j."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Number of records to process per batch.",
    )
    parser.add_argument(
        "--skip-constraints",
        action="store_true",
        help="Skip creating Neo4j constraints (not recommended).",
    )

    args = parser.parse_args()
    batch_size = args.batch_size

    # Get database connections
    neo4j_driver = None
    pg_conn = None
    success = False
    try:
        print("--- Starting Published Activity Load ---")
        # Establish connections
        neo4j_driver = get_neo4j_driver()
        pg_conn = get_postgres_connection() # Uses potentially corrected DATABASE_URL

        # Perform loading
        success = load_published_activity_nodes(pg_conn, neo4j_driver, batch_size)

    except Exception as e:
        print(f"An unexpected error occurred during the loading process: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc() # Print stack trace for debugging
        success = False
    finally:
        # Ensure connections are closed
        if neo4j_driver:
            neo4j_driver.close()
            print("Neo4j connection closed.")
        if pg_conn:
            pg_conn.close()
            print("PostgreSQL connection closed.")

        if success:
            print("Published Activity loading process finished successfully.")
        else:
            print("Published Activity loading process finished with errors.", file=sys.stderr)
            sys.exit(1) # Exit with error code


if __name__ == "__main__":
    main() 