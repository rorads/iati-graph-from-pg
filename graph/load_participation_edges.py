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

# Log file for skipped edge details
SKIPPED_DETAILS_LOG_FILENAME = "participation_edges_skipped_details.log"

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

    summary_log_filename = "participation_edges_skipped_summary.log"
    # Define detail log filename using constant
    detail_log_filename = SKIPPED_DETAILS_LOG_FILENAME 

    # Add this near the beginning with pg_conn parameter
    check_node_existence(neo4j_driver, pg_conn)
    
    # Initialize counters
    skipped_null_id_count = 0
    skipped_missing_node_count = 0 # This will now count skips identified by Neo4j during merge
    # Rename processed_count to be more specific
    successful_merge_operations = 0

    # 1. Get expected count from PostgreSQL
    expected_count = get_pg_count(pg_conn, DBT_TARGET_SCHEMA, SOURCE_TABLE)
    if expected_count is None: return False, 0, 0, 0 # Indicate failure, return counts
    if expected_count == 0:
        print(f"Skipping edge loading - no rows found in {DBT_TARGET_SCHEMA}.{SOURCE_TABLE}.")
        # Write empty summary file
        try:
            with open(summary_log_filename, 'w') as f:
                f.write(f"Skipped edge summary for {NEO4J_EDGE_TYPE}\n")
                f.write(f"Source: {DBT_TARGET_SCHEMA}.{SOURCE_TABLE}\n")
                f.write("No rows found in source table.\n")
                f.write("Skipped due to NULL IDs: 0\n")
                f.write("Skipped due to missing nodes (Neo4j): 0\n")
            print(f"Skip summary written to {os.path.abspath(summary_log_filename)}")
            # Also write empty details log
            with open(detail_log_filename, 'w') as f:
                f.write("organisation_id\tactivity_id\treason\n") # Header
            print(f"Skip details log initialized at {os.path.abspath(detail_log_filename)}")
        except IOError as e:
            print(f"Error writing summary/detail log file: {e}", file=sys.stderr)
        return True, 0, 0, 0 # Indicate success, return counts

    # 2. Get current count from Neo4j (before loading)
    count_before = get_neo4j_edge_count(neo4j_driver, NEO4J_EDGE_TYPE)
    # Don't exit if count fails, just note it

    # 3. Prepare PostgreSQL Cursor
    pg_cursor = pg_conn.cursor(name='fetch_participation_links', cursor_factory=psycopg2.extras.DictCursor)
    pg_cursor.itersize = batch_size

    # 4. Prepare SELECT Query for all desired columns
    select_cols_str = ", ".join([f'"{c}"' for c in SOURCE_COLUMNS])
    select_query = f'SELECT {select_cols_str} FROM "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}";'

    # 5. Prepare Cypher Query for Batch Loading - MODIFIED
    # This query now processes the batch and explicitly returns details for skipped rows
    set_clauses = []
    for col in EDGE_PROPERTY_COLUMNS:
        prop_name = col.replace("-", "_")
        set_clauses.append(f"r.{prop_name} = row.{prop_name}")
    set_clause_str = ", ".join(set_clauses)

    cypher_query = f"""
    UNWIND $batch as row
    // Match source nodes (published or phantom)
    OPTIONAL MATCH (org:{ORGANISATION_LABEL}) WHERE org.organisationidentifier = row.{SOURCE_NODE_ID}
    OPTIONAL MATCH (phantomOrg:{PHANTOM_ORG_LABEL}) WHERE org IS NULL AND phantomOrg.reference = row.{SOURCE_NODE_ID}
    WITH row, COALESCE(org, phantomOrg) as sourceNode
    // Match target nodes (published or phantom)
    OPTIONAL MATCH (act:{ACTIVITY_LABEL}) WHERE act.iatiidentifier = row.{TARGET_NODE_ID}
    OPTIONAL MATCH (phantomAct:{PHANTOM_ACTIVITY_LABEL}) WHERE act IS NULL AND phantomAct.phantom_activity_identifier = row.{TARGET_NODE_ID}
    WITH row, sourceNode, COALESCE(act, phantomAct) as targetNode
    
    // Conditional MERGE for valid pairs
    FOREACH (
        _ IN CASE WHEN sourceNode IS NOT NULL AND targetNode IS NOT NULL THEN [1] ELSE [] END |
        MERGE (sourceNode)-[r:{NEO4J_EDGE_TYPE}]->(targetNode)
        ON CREATE SET {set_clause_str}
        ON MATCH SET {set_clause_str}
    )
    
    // Return details ONLY for rows where merge didn't happen
    WITH row, sourceNode, targetNode
    WHERE sourceNode IS NULL OR targetNode IS NULL
    RETURN 
        row.{SOURCE_NODE_ID} as org_id, 
        row.{TARGET_NODE_ID} as act_id,
        sourceNode IS NULL as source_missing, 
        targetNode IS NULL as target_missing
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
         return False, skipped_null_id_count, skipped_missing_node_count, successful_merge_operations

    # Rename processed_count
    # Reset skipped_missing_node_count here, null id skips counted separately
    skipped_missing_node_count = 0
    print(f"Starting batch load (batch size: {batch_size})...")
    # print(f"Cypher Query Template:\n{cypher_query}") # Keep commented out unless debugging
    print(f"Skipped edge details will be logged to: {os.path.abspath(detail_log_filename)}")

    # Open detail log file in append mode
    try:
        with open(detail_log_filename, 'a') as detail_log_file:
            # Write header if the file is new/empty
            if detail_log_file.tell() == 0:
                 detail_log_file.write("organisation_id\tactivity_id\treason\n")

            with tqdm(total=expected_count, desc=f"Edges :{NEO4J_EDGE_TYPE}", unit=" edges") as pbar:
                 while True:
                    try:
                        batch_data = pg_cursor.fetchmany(batch_size)
                    except psycopg2.Error as e:
                         print(f"\nError fetching batch from PostgreSQL: {e}", file=sys.stderr)
                         break

                    if not batch_data: break # End of data

                    batch_list = []
                    rows_in_batch_attempt = 0
                    batch_initial_count = len(batch_data) # How many rows we got from PG

                    for row_dict in [dict(row) for row in batch_data]:
                        rows_in_batch_attempt += 1
                        # Check for NULL IDs first (cheap check)
                        if row_dict.get(SOURCE_NODE_ID) is None or row_dict.get(TARGET_NODE_ID) is None:
                            skipped_null_id_count += 1
                            # Log NULL skips to the detail file as well
                            org_id = row_dict.get(SOURCE_NODE_ID, 'NULL')
                            act_id = row_dict.get(TARGET_NODE_ID, 'NULL')
                            detail_log_file.write(f"{org_id}\t{act_id}\tNULL_ID\n")
                            continue

                        # Sanitise keys (remains the same)
                        sanitised_item = {}
                        for col in SOURCE_COLUMNS:
                            value = row_dict.get(col)
                            prop_name = col.replace("-", "_")
                            if isinstance(value, Decimal):
                                value = float(value)
                            sanitised_item[prop_name] = value
                        
                        batch_list.append(sanitised_item)

                    # Update progress bar based on rows fetched from PG, including null ID skips
                    pbar.update(batch_initial_count) # Use initial count before null ID filtering

                    if not batch_list: # If all rows in batch had null IDs or were otherwise filtered before Neo4j
                        detail_log_file.flush() # Ensure NULL ID skips are written
                        continue

                    # Process the valid batch items with Neo4j
                    try:
                        with neo4j_driver.session(database="neo4j") as session:
                            # Use execute_write for the operation
                            # The query now returns the list of skipped records
                            results = session.execute_write(
                                lambda tx: tx.run(cypher_query, batch=batch_list).data() # Use .data() to get list of dicts
                            )
                            
                            # results contains a list of skipped records
                            skipped_in_batch_neo4j = len(results)
                            # Calculate successful merges (CREATE or MATCH)
                            merges_in_batch = len(batch_list) - skipped_in_batch_neo4j

                            successful_merge_operations += merges_in_batch # Increment by successful merges
                            skipped_missing_node_count += skipped_in_batch_neo4j # Increment by skips identified by Neo4j
                            
                            # Log details for skipped records from this batch
                            for skipped_record in results:
                                org_id = skipped_record.get('org_id', 'ERROR')
                                act_id = skipped_record.get('act_id', 'ERROR')
                                source_missing = skipped_record.get('source_missing', True) # Default to True if key missing
                                target_missing = skipped_record.get('target_missing', True) # Default to True if key missing
                                
                                reason = "UNKNOWN"
                                if source_missing and target_missing:
                                    reason = "BOTH_MISSING"
                                elif source_missing:
                                    reason = "SOURCE_ORG_MISSING"
                                elif target_missing:
                                    reason = "TARGET_ACT_MISSING"
                                    
                                detail_log_file.write(f"{org_id}\t{act_id}\t{reason}\n")

                            # Flush after processing the batch to ensure logs are written promptly
                            detail_log_file.flush()

                    except Exception as e:
                        print(f"\nError processing batch in Neo4j: {e}", file=sys.stderr)
                        # print(f"Failed Cypher: {cypher_query}", file=sys.stderr) # Keep commented unless debugging
                        pg_cursor.close()
                        return False, skipped_null_id_count, skipped_missing_node_count, successful_merge_operations # Stop on Neo4j errors
                        
    except IOError as e:
        print(f"\nError opening or writing to detail log file {detail_log_filename}: {e}", file=sys.stderr)
        # Continue without detail logging if file fails? Or return error? For now, let's return False.
        pg_cursor.close()
        return False, skipped_null_id_count, skipped_missing_node_count, successful_merge_operations

    pg_cursor.close()
    
    # 7. Get final count from Neo4j (after loading)
    count_after = get_neo4j_edge_count(neo4j_driver, NEO4J_EDGE_TYPE)
    
    # Print summary of skipped edges
    print(f"\n--- Skipped Edges Summary ---")
    print(f"Total edges skipped due to NULL IDs:       {skipped_null_id_count}")
    print(f"Total edges skipped due to missing nodes (Neo4j): {skipped_missing_node_count}")
    total_skipped = skipped_null_id_count + skipped_missing_node_count
    print(f"Total skipped edges overall:             {total_skipped}")

    # Write summary to log file
    try:
        with open(summary_log_filename, 'w') as f:
            f.write(f"Skipped edge summary for {NEO4J_EDGE_TYPE}\n")
            f.write(f"Source: {DBT_TARGET_SCHEMA}.{SOURCE_TABLE}\n")
            f.write(f"Total expected edges (from PG): {expected_count}\n")
            f.write(f"Skipped due to NULL IDs: {skipped_null_id_count}\n")
            f.write(f"Skipped due to missing nodes (Neo4j): {skipped_missing_node_count}\n")
            f.write(f"Total skipped: {total_skipped}\n")
            # Update log message to be clearer
            f.write(f"Total successful MERGE operations (created or matched): {successful_merge_operations}\n")
            f.write(f"Neo4j count before load: {count_before if count_before is not None else 'N/A'}\n")
            f.write(f"Neo4j count after load: {count_after if count_after is not None else 'N/A'}\n")
        print(f"Skip summary written to {os.path.abspath(summary_log_filename)}")
    except IOError as e:
        print(f"Error writing summary log file: {e}", file=sys.stderr)


    if count_after is not None and count_before is not None:
        new_edges_created = count_after - count_before
        print(f"\n--- Count Summary ---")
        print(f"Expected Edge Count (from PG table): {expected_count}")
        print(f"Skipped Edges (NULL IDs):            {skipped_null_id_count}")
        print(f"Skipped Edges (missing nodes):       {skipped_missing_node_count}")
        net_expected_merges = expected_count - total_skipped
        print(f"Net Expected MERGE Operations:       {net_expected_merges}")
        print(f"Actual Successful MERGE Operations:  {successful_merge_operations}")
        print(f"Count Before Load:                   {count_before}")
        print(f"Count After Load (Neo4j):            {count_after}")
        print(f"Net New Edges Created:               {new_edges_created}")

        # Revised check: Compare actual merges vs expected merges
        if successful_merge_operations != net_expected_merges:
             print(f"Warning: The number of successful MERGE operations ({successful_merge_operations}) does not match the net expected count ({net_expected_merges}). Check batch processing logic.", file=sys.stderr)
        elif new_edges_created == 0 and successful_merge_operations > 0:
             print(f"Note: {successful_merge_operations} MERGE operations were successful, but no new edges were created. All relationships likely existed already and were updated (ON MATCH).")
        elif new_edges_created < successful_merge_operations:
             # This case is less likely with MERGE but could indicate other issues.
             print(f"Note: {successful_merge_operations} MERGE operations were successful, creating {new_edges_created} new edges. Some existing relationships were matched and updated.")

    else:
        print("Could not verify final counts after loading.", file=sys.stderr)

    return True, skipped_null_id_count, skipped_missing_node_count, successful_merge_operations # Indicate success


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
    final_null_skips = 0
    final_missing_node_skips = 0
    final_successful_merges = 0
    try:
        print("--- Starting Participation Edge Load ---")
        neo4j_driver = get_neo4j_driver()
        pg_conn = get_postgres_connection()

        success, final_null_skips, final_missing_node_skips, final_successful_merges = load_participation_edges(pg_conn, neo4j_driver, batch_size)

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
            print(f"Summary: Skipped (Null IDs): {final_null_skips}, Skipped (Missing Nodes): {final_missing_node_skips}, Successful Merges: {final_successful_merges}")
        else:
            print("\nParticipation edge loading process finished with errors or was interrupted.", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()