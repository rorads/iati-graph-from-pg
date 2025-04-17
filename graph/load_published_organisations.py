# graph/load_published_organisations.py

import argparse
import os
import sys
import time
from decimal import Decimal

import psycopg2
import psycopg2.extras
from tqdm import tqdm

# Import shared database functions and configuration
from db_utils import get_neo4j_driver, get_postgres_connection

# --- Configuration ---

# Target Schema and Table
DBT_TARGET_SCHEMA = "iati_graph"
SOURCE_TABLE = "published_organisations"
NEO4J_NODE_LABEL = "PublishedOrganisation"
NEO4J_ID_PROPERTY = "organisationidentifier"
NEO4J_NAME_PROPERTY = "name" # Explicit name for the organisation name

# Columns to load from PostgreSQL
SOURCE_COLUMNS = [
    "organisationidentifier",
    "name_narrative",
    "hierarchy",
    "reportingorg_ref",
    "dportal_link",
]

# Processing Batch Size
DEFAULT_BATCH_SIZE = 1000

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
            if "relation" in str(e) and "does not exist" in str(e):
                 print(f"Hint: Ensure schema '{schema}' and table '{table}' exist in database '{pg_conn.info.dbname}'. Check dbt run completion.", file=sys.stderr)
            return None # Return None to indicate failure


def get_neo4j_node_count(neo4j_driver, label):
    """Gets the count of nodes with a specific label in Neo4j."""
    cypher = f"MATCH (n:{label}) RETURN count(n) AS count"
    try:
        with neo4j_driver.session() as session:
            # Use execute_read for read-only queries
            result = session.execute_read(lambda tx: tx.run(cypher).single())
            count = result["count"] if result else 0
            print(f"Current node count for :{label} in Neo4j: {count}")
            return count
    except Exception as e:
        print(f"Error getting Neo4j node count for :{label}: {e}", file=sys.stderr)
        return None # Return None to indicate failure

def create_neo4j_constraint(neo4j_driver, label, property_key):
    """Creates a uniqueness constraint in Neo4j."""
    cypher = f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.{property_key} IS UNIQUE"
    print(f"Applying Neo4j constraint on :{label}({property_key})...")
    try:
        with neo4j_driver.session() as session:
            session.run(cypher)
        print("Constraint application attempted successfully (or constraint already exists).")
        return True
    except Exception as e:
        print(f"Warning: Could not apply constraint on :{label}({property_key}). Reason: {e}", file=sys.stderr)
        if "SyntaxError" in str(e):
             print("Hint: Check the syntax of the constraint, label, or property name.", file=sys.stderr)
        return False # For now, treat exceptions during creation as potential issues


# --- Data Loading Function ---

def load_published_organisation_nodes(pg_conn, neo4j_driver, batch_size):
    """Loads PublishedOrganisation nodes from PostgreSQL to Neo4j, including all specified columns."""
    print(f"--- Loading Nodes: {DBT_TARGET_SCHEMA}.{SOURCE_TABLE} -> :{NEO4J_NODE_LABEL} ---")

    # 1. Get expected count from PostgreSQL
    expected_count = get_pg_count(pg_conn, DBT_TARGET_SCHEMA, SOURCE_TABLE)
    if expected_count is None: return False
    if expected_count == 0:
        print(f"Skipping node loading - no rows found in {DBT_TARGET_SCHEMA}.{SOURCE_TABLE}.")
        return True

    # 2. Get current count from Neo4j (before loading)
    count_before = get_neo4j_node_count(neo4j_driver, NEO4J_NODE_LABEL)
    # Don't exit if count fails, just note it

    # 3. Create Constraint
    create_neo4j_constraint(neo4j_driver, NEO4J_NODE_LABEL, NEO4J_ID_PROPERTY)
    # Constraint failure might not be critical depending on use case, continue loading

    # 4. Prepare PostgreSQL Cursor
    pg_cursor = pg_conn.cursor(name='fetch_organisations', cursor_factory=psycopg2.extras.DictCursor)
    pg_cursor.itersize = batch_size

    # 5. Prepare SELECT Query for all desired columns
    select_cols_str = ", ".join([f'"{c}"' for c in SOURCE_COLUMNS])
    select_query = f'SELECT {select_cols_str} FROM "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}";'

    # 6. Prepare Cypher Query for Batch Loading with ALL columns
    # Build SET clauses dynamically for all columns except the ID property
    set_clauses = []
    for col in SOURCE_COLUMNS:
        # Use the special name property for name_narrative
        if col == "name_narrative":
            prop_name = NEO4J_NAME_PROPERTY
        else:
            # Basic sanitisation for property names (replace hyphens)
            prop_name = col.replace("-", "_")

        # Skip setting the ID property in the SET clause (it's used in MERGE)
        if col != NEO4J_ID_PROPERTY:
            # Use row[col] for accessing data in the batch map
            set_clauses.append(f"n.{prop_name} = row.{prop_name}") # Use sanitised prop_name here too

    set_clause_str = ", ".join(set_clauses)

    # Use MERGE for idempotency based on the unique ID property
    # Update all properties on both CREATE and MATCH
    cypher_query = f"""
    UNWIND $batch as row
    MERGE (n:{NEO4J_NODE_LABEL} {{{NEO4J_ID_PROPERTY}: row.{NEO4J_ID_PROPERTY}}})
    ON CREATE SET {set_clause_str}
    ON MATCH SET {set_clause_str}
    """

    # 7. Execute Loading in Batches
    print(f"Executing SELECT query: {select_query}")
    try:
        pg_cursor.execute(select_query)
    except psycopg2.Error as e:
         print(f"Error executing SELECT query: {e}", file=sys.stderr)
         if "relation" in str(e) and "does not exist" in str(e):
             print(f"Hint: Ensure schema '{DBT_TARGET_SCHEMA}' and table '{SOURCE_TABLE}' exist and are accessible by user '{pg_conn.info.user}'.", file=sys.stderr)
         elif "column" in str(e) and "does not exist" in str(e):
             print(f"Hint: A column in SOURCE_COLUMNS ({SOURCE_COLUMNS}) does not exist in '{DBT_TARGET_SCHEMA}.{SOURCE_TABLE}'. Verify SOURCE_COLUMNS.", file=sys.stderr)
         pg_cursor.close()
         return False

    processed_count = 0
    skipped_null_id_count = 0
    print(f"Starting batch load (batch size: {batch_size})...")
    print(f"Cypher Query Template:\n{cypher_query}") # Print the template for debugging

    with tqdm(total=expected_count, desc=f"Nodes :{NEO4J_NODE_LABEL}", unit=" nodes") as pbar:
         while True:
            try:
                batch_data = pg_cursor.fetchmany(batch_size)
            except psycopg2.Error as e:
                 print(f"\nError fetching batch from PostgreSQL: {e}", file=sys.stderr)
                 break

            if not batch_data: break # End of data

            # Convert Row objects to dictionaries and sanitise keys for Neo4j parameters
            batch_list = []
            current_skipped = 0
            for row_dict in [dict(row) for row in batch_data]:
                if row_dict.get(NEO4J_ID_PROPERTY) is None:
                    current_skipped += 1
                    continue

                # Sanitise keys in the dictionary for the Cypher query parameter map
                sanitised_item = {}
                for col in SOURCE_COLUMNS:
                    # Determine the correct property name for Neo4j
                    if col == "name_narrative":
                        prop_name = NEO4J_NAME_PROPERTY # Use "name" instead of "name_narrative"
                    else:
                        prop_name = col.replace("-", "_") # Standard sanitisation for others

                    value = row_dict.get(col)
                    # Convert Decimal to float for Neo4j compatibility
                    if isinstance(value, Decimal):
                        value = float(value)
                    # Add the value to the dictionary using the determined property name
                    sanitised_item[prop_name] = value
                batch_list.append(sanitised_item)

            if current_skipped > 0:
                 skipped_null_id_count += current_skipped

            if not batch_list: # If all rows in batch had null ID
                pbar.update(len(batch_data))
                continue

            try:
                with neo4j_driver.session(database="neo4j") as session:
                    # Use execute_write for write operations
                    session.execute_write(
                        lambda tx: tx.run(cypher_query, batch=batch_list)
                    )
                processed_count += len(batch_list)
                pbar.update(len(batch_data))
            except Exception as e:
                print(f"\nError processing batch in Neo4j: {e}", file=sys.stderr)
                print(f"Failed Cypher: {cypher_query}", file=sys.stderr)
                pg_cursor.close()
                return False # Stop on Neo4j errors

    pg_cursor.close()
    if skipped_null_id_count > 0:
        print(f"\nTotal rows skipped due to null '{NEO4J_ID_PROPERTY}': {skipped_null_id_count}")
    print(f"\nFinished batch loading. Processed {processed_count} nodes ({expected_count - skipped_null_id_count} expected based on non-null IDs)." if skipped_null_id_count > 0 else f"\nFinished batch loading. Processed {processed_count} nodes.")

    # 8. Get final count from Neo4j (after loading)
    count_after = get_neo4j_node_count(neo4j_driver, NEO4J_NODE_LABEL)
    if count_after is not None:
        print(f"\n--- Count Summary ---")
        print(f"Expected Count (from PG table): {expected_count}")
        print(f"Skipped Rows (null ID):         {skipped_null_id_count}")
        print(f"Net Expected Nodes:             {expected_count - skipped_null_id_count}")
        print(f"Count Before Load:              {count_before if count_before is not None else 'N/A'}")
        print(f"Count After Load (Neo4j):       {count_after}")

        if count_after != (expected_count - skipped_null_id_count):
             print(f"WARNING: Final Neo4j count ({count_after}) does not match net expected count ({expected_count - skipped_null_id_count}). Check for pre-existing nodes or loading discrepancies.", file=sys.stderr)
    else:
        print("Could not verify final counts after loading.", file=sys.stderr)

    return True # Indicate success


# --- Main Execution ---

def main():
    parser = argparse.ArgumentParser(
        description=f"Load {NEO4J_NODE_LABEL} nodes from PostgreSQL ({DBT_TARGET_SCHEMA}.{SOURCE_TABLE}) to Neo4j."
    )
    parser.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
        help=f"Number of records per batch (default: {DEFAULT_BATCH_SIZE})."
    )

    args = parser.parse_args()
    batch_size = args.batch_size

    neo4j_driver = None
    pg_conn = None
    success = False
    start_time = time.time()
    try:
        print("--- Starting Published Organisation Load ---")
        neo4j_driver = get_neo4j_driver()
        pg_conn = get_postgres_connection()

        success = load_published_organisation_nodes(pg_conn, neo4j_driver, batch_size)

    except KeyboardInterrupt:
        print("\nProcess interrupted by user.", file=sys.stderr)
        success = False
    except Exception as e:
        print(f"\nAn unexpected error occurred during the loading process: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        success = False
    finally:
        if neo4j_driver: neo4j_driver.close(); print("Neo4j connection closed.")
        if pg_conn: pg_conn.close(); print("PostgreSQL connection closed.")

        end_time = time.time()
        print(f"\nTotal execution time: {end_time - start_time:.2f} seconds.")

        if success:
            print("\nPublished Organisation loading process finished successfully.")
        else:
            print("\nPublished Organisation loading process finished with errors or was interrupted.", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main() 