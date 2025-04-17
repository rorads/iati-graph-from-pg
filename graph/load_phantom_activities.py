# graph/load_phantom_activities.py

import argparse
import os
import sys
import time
from decimal import Decimal
from collections import defaultdict

import psycopg2
import psycopg2.extras
from tqdm import tqdm

# Import shared database functions and configuration
from db_utils import get_neo4j_driver, get_postgres_connection

# --- Configuration ---

# Target Schema and Table
DBT_TARGET_SCHEMA = "iati_graph"
SOURCE_TABLE = "phantom_activities"
NEO4J_NODE_LABEL = "PhantomActivity" 
NEO4J_ID_PROPERTY = "phantom_activity_identifier"
NEO4J_TITLE_PROPERTY = "title"  # For consistency with published activities

# Columns to load from PostgreSQL - based on the SQL model
SOURCE_COLUMNS = [
    "phantom_activity_identifier",  # The identifier that was referenced but not found
    "source_column",               # The table.column where the phantom reference was found
    "source_activity_id"           # The activity ID that made the reference
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
            print(f"Expected row count from {schema}.{table}: {count}")
            return count
        except psycopg2.Error as e:
            print(f"Error getting count from {schema}.{table}: {e}", file=sys.stderr)
            if "relation" in str(e) and "does not exist" in str(e):
                 print(f"Hint: Ensure schema '{schema}' and table '{table}' exist in database '{pg_conn.info.dbname}'. Check dbt run completion.", file=sys.stderr)
            return None # Return None to indicate failure

def get_pg_unique_count(pg_conn, schema, table, id_column):
    """Gets the count of unique IDs in a PostgreSQL table."""
    with pg_conn.cursor() as cursor:
        try:
            cursor.execute(f'SELECT COUNT(DISTINCT "{id_column}") FROM "{schema}"."{table}";')
            count = cursor.fetchone()[0]
            print(f"Expected unique {id_column} count from {schema}.{table}: {count}")
            return count
        except psycopg2.Error as e:
            print(f"Error getting unique count from {schema}.{table}: {e}", file=sys.stderr)
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

def load_phantom_activity_nodes(pg_conn, neo4j_driver, batch_size):
    """Loads PhantomActivity nodes from PostgreSQL to Neo4j with grouped references."""
    print(f"--- Loading Nodes: {DBT_TARGET_SCHEMA}.{SOURCE_TABLE} -> :{NEO4J_NODE_LABEL} ---")

    # 1. Get expected counts from PostgreSQL
    row_count = get_pg_count(pg_conn, DBT_TARGET_SCHEMA, SOURCE_TABLE)
    unique_id_count = get_pg_unique_count(pg_conn, DBT_TARGET_SCHEMA, SOURCE_TABLE, NEO4J_ID_PROPERTY)
    
    if row_count is None or unique_id_count is None: return False
    if row_count == 0:
        print(f"Skipping node loading - no rows found in {DBT_TARGET_SCHEMA}.{SOURCE_TABLE}.")
        return True
    
    # Print info about potential duplicates
    if row_count > unique_id_count:
        print(f"Note: Found {row_count} total references to {unique_id_count} unique phantom activities.")
        print(f"      Multiple references to the same phantom activity will be combined.")

    # 2. Get current count from Neo4j (before loading)
    count_before = get_neo4j_node_count(neo4j_driver, NEO4J_NODE_LABEL)
    # Don't exit if count fails, just note it

    # 3. Create Constraint
    create_neo4j_constraint(neo4j_driver, NEO4J_NODE_LABEL, NEO4J_ID_PROPERTY)
    # Constraint failure might not be critical depending on use case, continue loading

    # 4. Prepare PostgreSQL Cursor
    pg_cursor = pg_conn.cursor(name='fetch_phantom_activities', cursor_factory=psycopg2.extras.DictCursor)
    pg_cursor.itersize = batch_size

    # 5. Prepare SELECT Query for all desired columns
    select_cols_str = ", ".join([f'"{c}"' for c in SOURCE_COLUMNS])
    select_query = f'SELECT {select_cols_str} FROM "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}" ORDER BY "{NEO4J_ID_PROPERTY}";'

    # 6. Prepare Cypher Query for Batch Loading with arrays for references
    cypher_query = f"""
    UNWIND $batch as row
    MERGE (n:{NEO4J_NODE_LABEL} {{{NEO4J_ID_PROPERTY}: row.{NEO4J_ID_PROPERTY}}})
    ON CREATE SET 
        n.source_columns = row.source_columns,
        n.source_activity_ids = row.source_activity_ids,
        n.{NEO4J_TITLE_PROPERTY} = 'Phantom Activity: ' + row.{NEO4J_ID_PROPERTY},
        n.reference_count = row.reference_count
    ON MATCH SET 
        n.source_columns = row.source_columns,
        n.source_activity_ids = row.source_activity_ids,
        n.{NEO4J_TITLE_PROPERTY} = 'Phantom Activity: ' + row.{NEO4J_ID_PROPERTY},
        n.reference_count = row.reference_count
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

    # Group by phantom activity identifier to combine multiple references
    with tqdm(total=unique_id_count, desc=f"Nodes :{NEO4J_NODE_LABEL}", unit=" nodes") as pbar:
        current_batch = []
        current_batch_size = 0
        grouped_activities = defaultdict(lambda: {"source_columns": [], "source_activity_ids": []})
        
        while True:
            try:
                batch_data = pg_cursor.fetchmany(batch_size)
            except psycopg2.Error as e:
                 print(f"\nError fetching batch from PostgreSQL: {e}", file=sys.stderr)
                 break

            if not batch_data: break # End of data

            # Group data by phantom_activity_identifier
            for row_dict in [dict(row) for row in batch_data]:
                id_val = row_dict.get(NEO4J_ID_PROPERTY)
                if id_val is None:
                    skipped_null_id_count += 1
                    continue
                
                # Add source column and activity id to the grouped data
                source_col = row_dict.get("source_column")
                source_act_id = row_dict.get("source_activity_id")
                
                if source_col and source_col not in grouped_activities[id_val]["source_columns"]:
                    grouped_activities[id_val]["source_columns"].append(source_col)
                
                if source_act_id and source_act_id not in grouped_activities[id_val]["source_activity_ids"]:
                    grouped_activities[id_val]["source_activity_ids"].append(source_act_id)
            
            # Check if we need to process the grouped data
            # Either when we've accumulated enough or when we're at the end
            if len(grouped_activities) >= batch_size or not batch_data:
                # Convert grouped data to batch list
                batch_list = []
                for id_val, data in grouped_activities.items():
                    sanitised_item = {
                        NEO4J_ID_PROPERTY: id_val,
                        "source_columns": data["source_columns"],
                        "source_activity_ids": data["source_activity_ids"],
                        "reference_count": len(data["source_activity_ids"])
                    }
                    batch_list.append(sanitised_item)
                
                # Process batch
                if batch_list:
                    try:
                        with neo4j_driver.session(database="neo4j") as session:
                            # Use execute_write for write operations
                            session.execute_write(
                                lambda tx: tx.run(cypher_query, batch=batch_list)
                            )
                        processed_count += len(batch_list)
                        pbar.update(len(batch_list))
                    except Exception as e:
                        print(f"\nError processing batch in Neo4j: {e}", file=sys.stderr)
                        print(f"Failed Cypher: {cypher_query}", file=sys.stderr)
                        pg_cursor.close()
                        return False # Stop on Neo4j errors
                
                # Reset grouped activities for next batch
                grouped_activities.clear()

    pg_cursor.close()
    if skipped_null_id_count > 0:
        print(f"\nTotal rows skipped due to null '{NEO4J_ID_PROPERTY}': {skipped_null_id_count}")
    print(f"\nFinished batch loading. Processed {processed_count} unique phantom activity nodes.")

    # 8. Get final count from Neo4j (after loading)
    count_after = get_neo4j_node_count(neo4j_driver, NEO4J_NODE_LABEL)
    if count_after is not None:
        print(f"\n--- Count Summary ---")
        print(f"Total Row Count (from PG table):   {row_count}")
        print(f"Unique IDs Count (from PG table):  {unique_id_count}")
        print(f"Skipped Rows (null ID):            {skipped_null_id_count}")
        print(f"Net Expected Nodes:                {unique_id_count - skipped_null_id_count}")
        print(f"Count Before Load:                 {count_before if count_before is not None else 'N/A'}")
        print(f"Count After Load (Neo4j):          {count_after}")

        if count_after != (unique_id_count - skipped_null_id_count):
             print(f"WARNING: Final Neo4j count ({count_after}) does not match net expected count ({unique_id_count - skipped_null_id_count}). Check for pre-existing nodes or loading discrepancies.", file=sys.stderr)
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
        print("--- Starting Phantom Activity Load ---")
        neo4j_driver = get_neo4j_driver()
        pg_conn = get_postgres_connection()

        success = load_phantom_activity_nodes(pg_conn, neo4j_driver, batch_size)

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
            print("\nPhantom Activity loading process finished successfully.")
        else:
            print("\nPhantom Activity loading process finished with errors or was interrupted.", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main() 