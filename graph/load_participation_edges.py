# graph/load_participation_edges.py

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
SOURCE_TABLE = "participation_links"
NEO4J_EDGE_TYPE = "PARTICIPATES_IN"  # Edge type/relationship name (Neo4j convention: uppercase with underscores)

# Node labels for source and target - updated to match actual labels used in node loader scripts
ORGANISATION_LABEL = "PublishedOrganisation"  # For published organisations
PHANTOM_ORG_LABEL = "PhantomOrganisation"     # For phantom organisations
ACTIVITY_LABEL = "PublishedActivity"          # For published activities
PHANTOM_ACTIVITY_LABEL = "PhantomActivity"    # For phantom activities

# Column mappings
SOURCE_NODE_ID = "organisation_id"    # Organisation ID (source node of the relationship)
TARGET_NODE_ID = "activity_id"        # Activity ID (target node of the relationship)

# Columns to load from PostgreSQL - based on the SQL model
SOURCE_COLUMNS = [
    "activity_id",      # The IATI identifier of the activity
    "organisation_id",  # The IATI identifier of the participating organisation
    "role_code",        # The IATI code for the organisation's role
    "role_name"         # The human-readable name of the organisation's role
]

# Edge property columns (these become properties on the relationship)
EDGE_PROPERTY_COLUMNS = [
    "role_code",
    "role_name"
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
            print(f"Expected edge count from {schema}.{table}: {count}")
            return count
        except psycopg2.Error as e:
            print(f"Error getting count from {schema}.{table}: {e}", file=sys.stderr)
            if "relation" in str(e) and "does not exist" in str(e):
                 print(f"Hint: Ensure schema '{schema}' and table '{table}' exist in database '{pg_conn.info.dbname}'. Check dbt run completion.", file=sys.stderr)
            return None # Return None to indicate failure


def get_neo4j_edge_count(neo4j_driver, edge_type):
    """Gets the count of edges with a specific type in Neo4j."""
    cypher = f"MATCH ()-[r:{edge_type}]->() RETURN count(r) AS count"
    try:
        with neo4j_driver.session() as session:
            # Use execute_read for read-only queries
            result = session.execute_read(lambda tx: tx.run(cypher).single())
            count = result["count"] if result else 0
            print(f"Current edge count for :{edge_type} in Neo4j: {count}")
            return count
    except Exception as e:
        print(f"Error getting Neo4j edge count for :{edge_type}: {e}", file=sys.stderr)
        return None # Return None to indicate failure


def check_node_existence(neo4j_driver, pg_conn, batch_size=100):
    """Samples and checks node existence to help debug missing nodes."""
    print("\n--- Node Existence Check (Debugging) ---")
    
    # Get node type counts
    node_types = [
        {"label": ORGANISATION_LABEL, "id_prop": "organisationidentifier"},
        {"label": PHANTOM_ORG_LABEL, "id_prop": "reference"},
        {"label": ACTIVITY_LABEL, "id_prop": "iatiidentifier"},
        {"label": PHANTOM_ACTIVITY_LABEL, "id_prop": "phantom_activity_identifier"}
    ]
    
    # Log node counts first
    for node_type in node_types:
        cypher = f"MATCH (n:{node_type['label']}) RETURN count(n) AS count"
        try:
            with neo4j_driver.session() as session:
                result = session.execute_read(lambda tx: tx.run(cypher).single())
                count = result["count"] if result else 0
                print(f"  Node count for :{node_type['label']}: {count}")
        except Exception as e:
            print(f"  Error getting count for {node_type['label']}: {e}")
    
    # Sample some activities and organisations from the links table
    print("\n  Sampling IDs from participation_links table:")
    try:
        with pg_conn.cursor() as cursor:
            # Sample organization IDs
            cursor.execute(f"""
                SELECT DISTINCT organisation_id 
                FROM "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}" 
                LIMIT 5
            """)
            org_ids = [row[0] for row in cursor.fetchall()]
            print(f"  Sample organisation_ids: {org_ids}")
            
            # Sample activity IDs
            cursor.execute(f"""
                SELECT DISTINCT activity_id 
                FROM "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}" 
                LIMIT 5
            """)
            act_ids = [row[0] for row in cursor.fetchall()]
            print(f"  Sample activity_ids: {act_ids}")
            
            # Check if these IDs exist in Neo4j
            print("\n  Checking if sampled IDs exist in Neo4j:")
            
            # Check organisation IDs
            for org_id in org_ids:
                with neo4j_driver.session() as session:
                    # Check published organisations
                    result = session.execute_read(
                        lambda tx: tx.run(
                            f"MATCH (n:{ORGANISATION_LABEL}) WHERE n.organisationidentifier = $id RETURN COUNT(n) as count",
                            id=org_id
                        ).single()
                    )
                    pub_count = result["count"] if result else 0
                    
                    # Check phantom organisations
                    result = session.execute_read(
                        lambda tx: tx.run(
                            f"MATCH (n:{PHANTOM_ORG_LABEL}) WHERE n.reference = $id RETURN COUNT(n) as count",
                            id=org_id
                        ).single()
                    )
                    phantom_count = result["count"] if result else 0
                    
                    print(f"  Org ID {org_id}: {pub_count} published, {phantom_count} phantom")
            
            # Check activity IDs
            for act_id in act_ids:
                with neo4j_driver.session() as session:
                    # Check published activities
                    result = session.execute_read(
                        lambda tx: tx.run(
                            f"MATCH (n:{ACTIVITY_LABEL}) WHERE n.iatiidentifier = $id RETURN COUNT(n) as count",
                            id=act_id
                        ).single()
                    )
                    pub_count = result["count"] if result else 0
                    
                    # Check phantom activities
                    result = session.execute_read(
                        lambda tx: tx.run(
                            f"MATCH (n:{PHANTOM_ACTIVITY_LABEL}) WHERE n.phantom_activity_identifier = $id RETURN COUNT(n) as count",
                            id=act_id
                        ).single()
                    )
                    phantom_count = result["count"] if result else 0
                    
                    print(f"  Activity ID {act_id}: {pub_count} published, {phantom_count} phantom")
    
    except Exception as e:
        print(f"  Error during sampling check: {e}")
    
    # Check for overall mismatch counts (approximate)
    try:
        with pg_conn.cursor() as cursor:
            # Count distinct organisations in links that don't exist in Neo4j
            cypher = f"""
            WITH $org_ids as orgIds
            UNWIND orgIds as id
            OPTIONAL MATCH (org:PublishedOrganisation) WHERE org.organisationidentifier = id
            OPTIONAL MATCH (phantomOrg:PhantomOrganisation) WHERE phantomOrg.reference = id 
            WITH id, org, phantomOrg
            WHERE org IS NULL AND phantomOrg IS NULL
            RETURN count(id) as missingCount
            """
            
            # Get sample of org IDs (limit to avoid performance issues)
            cursor.execute(f"""
                SELECT DISTINCT organisation_id 
                FROM "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}" 
                LIMIT 1000
            """)
            sample_org_ids = [row[0] for row in cursor.fetchall()]
            
            if sample_org_ids:
                with neo4j_driver.session() as session:
                    result = session.execute_read(
                        lambda tx: tx.run(cypher, org_ids=sample_org_ids).single()
                    )
                    missing_orgs = result["missingCount"] if result else 0
                    print(f"\n  ~{missing_orgs} of {len(sample_org_ids)} sampled org IDs are missing from Neo4j")
    
    except Exception as e:
        print(f"  Error during mismatch count check: {e}")
    
    print("\n--- End of Node Existence Check ---\n")


# --- Data Loading Function ---

def load_participation_edges(pg_conn, neo4j_driver, batch_size):
    """Loads participation edges from PostgreSQL to Neo4j."""
    print(f"--- Loading Edges: {DBT_TARGET_SCHEMA}.{SOURCE_TABLE} -> :{NEO4J_EDGE_TYPE} ---")

    # Add this near the beginning with pg_conn parameter
    check_node_existence(neo4j_driver, pg_conn)
    
    # 1. Get expected count from PostgreSQL
    expected_count = get_pg_count(pg_conn, DBT_TARGET_SCHEMA, SOURCE_TABLE)
    if expected_count is None: return False
    if expected_count == 0:
        print(f"Skipping edge loading - no rows found in {DBT_TARGET_SCHEMA}.{SOURCE_TABLE}.")
        return True

    # 2. Get current count from Neo4j (before loading)
    count_before = get_neo4j_edge_count(neo4j_driver, NEO4J_EDGE_TYPE)
    # Don't exit if count fails, just note it

    # 3. Prepare PostgreSQL Cursor
    pg_cursor = pg_conn.cursor(name='fetch_participation_links', cursor_factory=psycopg2.extras.DictCursor)
    pg_cursor.itersize = batch_size

    # 4. Prepare SELECT Query for all desired columns
    select_cols_str = ", ".join([f'"{c}"' for c in SOURCE_COLUMNS])
    select_query = f'SELECT {select_cols_str} FROM "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}";'

    # 5. Prepare Cypher Query for Batch Loading
    # Build SET clauses dynamically for edge properties
    set_clauses = []
    for col in EDGE_PROPERTY_COLUMNS:
        # Basic sanitisation for property names (replace hyphens)
        prop_name = col.replace("-", "_")
        # Use row[col] for accessing data in the batch map
        set_clauses.append(f"r.{prop_name} = row.{prop_name}")

    set_clause_str = ", ".join(set_clauses)

    # Use MERGE for idempotency - first try published nodes, then phantom nodes if not found
    cypher_query = f"""
    UNWIND $batch as row
    // First try to match with published organisation
    OPTIONAL MATCH (org:{ORGANISATION_LABEL}) 
    WHERE org.organisationidentifier = row.{SOURCE_NODE_ID}
    
    // First try to match with published activity
    OPTIONAL MATCH (act:{ACTIVITY_LABEL}) 
    WHERE act.iatiidentifier = row.{TARGET_NODE_ID}
    
    // If organisation not found, try phantom organisation
    WITH row, org, act
    OPTIONAL MATCH (phantomOrg:{PHANTOM_ORG_LABEL}) 
    WHERE phantomOrg.reference = row.{SOURCE_NODE_ID} AND org IS NULL
    
    // If activity not found, try phantom activity
    OPTIONAL MATCH (phantomAct:{PHANTOM_ACTIVITY_LABEL}) 
    WHERE phantomAct.phantom_activity_identifier = row.{TARGET_NODE_ID} AND act IS NULL
    
    // Use the appropriate nodes (published or phantom)
    WITH row, 
         CASE WHEN org IS NOT NULL THEN org ELSE phantomOrg END as sourceNode,
         CASE WHEN act IS NOT NULL THEN act ELSE phantomAct END as targetNode
    
    // Only create the relationship if both nodes exist
    WHERE sourceNode IS NOT NULL AND targetNode IS NOT NULL
    
    // Create the relationship
    MERGE (sourceNode)-[r:{NEO4J_EDGE_TYPE}]->(targetNode)
    ON CREATE SET {set_clause_str}
    ON MATCH SET {set_clause_str}
    RETURN count(r) as relationshipsCreated
    """

    # 6. Execute Loading in Batches
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
    skipped_missing_node_count = 0
    print(f"Starting batch load (batch size: {batch_size})...")
    print(f"Cypher Query Template:\n{cypher_query}") # Print the template for debugging

    with tqdm(total=expected_count, desc=f"Edges :{NEO4J_EDGE_TYPE}", unit=" edges") as pbar:
         while True:
            try:
                batch_data = pg_cursor.fetchmany(batch_size)
            except psycopg2.Error as e:
                 print(f"\nError fetching batch from PostgreSQL: {e}", file=sys.stderr)
                 break

            if not batch_data: break # End of data

            # Convert Row objects to dictionaries and sanitise keys for Neo4j parameters
            batch_list = []
            for row_dict in [dict(row) for row in batch_data]:
                if row_dict.get(SOURCE_NODE_ID) is None or row_dict.get(TARGET_NODE_ID) is None:
                    skipped_missing_node_count += 1
                    continue

                # Sanitise keys in the dictionary for the Cypher query parameter map
                sanitised_item = {}
                for col in SOURCE_COLUMNS:
                    # Get the value from the row
                    value = row_dict.get(col)
                    
                    # Standard sanitisation for property names (replace hyphens)
                    prop_name = col.replace("-", "_")
                    
                    # Convert Decimal to float for Neo4j compatibility
                    if isinstance(value, Decimal):
                        value = float(value)
                    # Add the value to the dictionary using the determined property name
                    sanitised_item[prop_name] = value
                
                batch_list.append(sanitised_item)

            if not batch_list: # If all rows in batch had null IDs
                pbar.update(len(batch_data))
                continue

            try:
                with neo4j_driver.session(database="neo4j") as session:
                    result = session.execute_write(
                        lambda tx: tx.run(cypher_query, batch=batch_list).consume()
                    )
                    # Get the number of relationships created
                    # Note: Neo4j doesn't provide a direct counter for relationships_matched
                    # Only relationships_created is available
                    created = result.counters.relationships_created
                    
                    processed_count += created
                    # Check if we had missing nodes based on expected vs actual created
                    if created < len(batch_list):
                        skipped_missing_node_count += len(batch_list) - created
                        print(f"\nNote: {len(batch_list) - created} edges skipped in this batch due to missing nodes.")
                    
                    pbar.update(len(batch_data))
                    
            except Exception as e:
                print(f"\nError processing batch in Neo4j: {e}", file=sys.stderr)
                print(f"Failed Cypher: {cypher_query}", file=sys.stderr)
                pg_cursor.close()
                return False # Stop on Neo4j errors

    pg_cursor.close()
    if skipped_missing_node_count > 0:
        print(f"\nTotal edges skipped due to missing nodes: {skipped_missing_node_count}")
    print(f"\nFinished batch loading. Processed {processed_count} edges.")

    # 7. Get final count from Neo4j (after loading)
    count_after = get_neo4j_edge_count(neo4j_driver, NEO4J_EDGE_TYPE)
    if count_after is not None:
        print(f"\n--- Count Summary ---")
        print(f"Expected Edge Count (from PG table): {expected_count}")
        print(f"Skipped Edges (missing nodes):       {skipped_missing_node_count}")
        print(f"Net Expected Edges:                  {expected_count - skipped_missing_node_count}")
        print(f"Count Before Load:                   {count_before if count_before is not None else 'N/A'}")
        print(f"Count After Load (Neo4j):            {count_after}")

        # Note: The actual count might not match expected for edges as some nodes might not exist
        if count_after < (expected_count - skipped_missing_node_count):
             print(f"Note: Final Neo4j count ({count_after}) is lower than net expected count ({expected_count - skipped_missing_node_count}). This may be due to missing source or target nodes.", file=sys.stderr)
    else:
        print("Could not verify final counts after loading.", file=sys.stderr)

    return True # Indicate success


# --- Main Execution ---

def main():
    parser = argparse.ArgumentParser(
        description=f"Load {NEO4J_EDGE_TYPE} edges from PostgreSQL ({DBT_TARGET_SCHEMA}.{SOURCE_TABLE}) to Neo4j."
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
        print("--- Starting Participation Edge Load ---")
        neo4j_driver = get_neo4j_driver()
        pg_conn = get_postgres_connection()

        success = load_participation_edges(pg_conn, neo4j_driver, batch_size)

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
            print("\nParticipation edge loading process finished successfully.")
        else:
            print("\nParticipation edge loading process finished with errors or was interrupted.", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()