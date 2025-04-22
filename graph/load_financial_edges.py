# graph/load_financial_edges.py

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
SOURCE_TABLE = "financial_links"
NEO4J_EDGE_TYPE = "FINANCIAL_TRANSACTION" # Edge type/relationship name

# Node labels for source and target
ORGANISATION_LABEL = "PublishedOrganisation"
PHANTOM_ORG_LABEL = "PhantomOrganisation"
ACTIVITY_LABEL = "PublishedActivity"
PHANTOM_ACTIVITY_LABEL = "PhantomActivity"

# Column names from financial_links.sql
SOURCE_NODE_ID_COL = "source_node_id"
TARGET_NODE_ID_COL = "target_node_id"
SOURCE_NODE_TYPE_COL = "source_node_type" # 'ORGANISATION' or 'ACTIVITY'
TARGET_NODE_TYPE_COL = "target_node_type" # 'ORGANISATION' or 'ACTIVITY'

# Columns to load from PostgreSQL - based on the SQL model
SOURCE_COLUMNS = [
    SOURCE_NODE_ID_COL,
    TARGET_NODE_ID_COL,
    SOURCE_NODE_TYPE_COL,
    TARGET_NODE_TYPE_COL,
    "transactiontype_code",
    "transaction_type_name",
    "currency",
    "total_value_usd"
]

# Edge property columns (these become properties on the relationship)
EDGE_PROPERTY_COLUMNS = [
    "transactiontype_code",
    "transaction_type_name",
    "currency",
    "total_value_usd"
]

# Processing Batch Size
DEFAULT_BATCH_SIZE = 500 # Reduced default batch size due to potentially more complex query

# Log file for skipped edge details
LOG_DIR = "logs" # Define log directory
SKIPPED_DETAILS_LOG_FILENAME = os.path.join(LOG_DIR, "financial_edges_skipped_details.log")
SUMMARY_LOG_FILENAME = os.path.join(LOG_DIR, "financial_edges_skipped_summary.log")

# --- Helper Functions (Copied and potentially adapted from load_participation_edges.py) ---

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
            result = session.execute_read(lambda tx: tx.run(cypher).single())
            count = result["count"] if result else 0
            print(f"Current edge count for :{edge_type} in Neo4j: {count}")
            return count
    except Exception as e:
        print(f"Error getting Neo4j edge count for :{edge_type}: {e}", file=sys.stderr)
        return None # Return None to indicate failure

def check_node_existence(neo4j_driver, pg_conn, batch_size=100):
    """Samples IDs and checks node existence to help debug missing nodes (adapted for financial links)."""
    print("\n--- Node Existence Check (Debugging) ---") # Added leading newline

    # Log node counts first
    node_types = [
        {"label": ORGANISATION_LABEL, "id_prop": "organisationidentifier"},
        {"label": PHANTOM_ORG_LABEL, "id_prop": "reference"},
        {"label": ACTIVITY_LABEL, "id_prop": "iatiidentifier"},
        {"label": PHANTOM_ACTIVITY_LABEL, "id_prop": "phantom_activity_identifier"}
    ]
    for node_type in node_types:
        cypher = f"MATCH (n:{node_type['label']}) RETURN count(n) AS count"
        try:
            with neo4j_driver.session() as session:
                result = session.execute_read(lambda tx: tx.run(cypher).single())
                count = result["count"] if result else 0
                print(f"  Node count for :{node_type['label']}: {count}")
        except Exception as e:
            print(f"  Error getting count for {node_type['label']}: {e}")

    # Sample some source and target IDs from the financial_links table
    print("\n  Sampling IDs from financial_links table:") # Fixed internal newline
    sample_ids = set()
    try:
        with pg_conn.cursor() as cursor:
            # Sample source IDs
            cursor.execute(f"""
                SELECT DISTINCT {SOURCE_NODE_ID_COL}
                FROM "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}"
                WHERE {SOURCE_NODE_ID_COL} IS NOT NULL AND {SOURCE_NODE_ID_COL} <> ''
                LIMIT 5
            """)
            sample_ids.update([row[0] for row in cursor.fetchall()])

            # Sample target IDs
            cursor.execute(f"""
                SELECT DISTINCT {TARGET_NODE_ID_COL}
                FROM "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}"
                WHERE {TARGET_NODE_ID_COL} IS NOT NULL AND {TARGET_NODE_ID_COL} <> ''
                LIMIT 5
            """)
            sample_ids.update([row[0] for row in cursor.fetchall()])

            print(f"  Sample IDs (org or activity): {list(sample_ids)}")

            # Check if these IDs exist in Neo4j across relevant node types
            print("\n  Checking if sampled IDs exist in Neo4j:") # Fixed previous incorrect edit

            for sample_id in sample_ids:
                found_in = []
                with neo4j_driver.session() as session:
                    # Check published organisations
                    count = session.execute_read(
                        lambda tx: tx.run(f"MATCH (n:{ORGANISATION_LABEL}) WHERE n.organisationidentifier = $id RETURN COUNT(n) as count", id=sample_id).single()["count"] or 0
                    )
                    if count > 0: found_in.append(f"{ORGANISATION_LABEL} ({count})")

                    # Check phantom organisations
                    count = session.execute_read(
                        lambda tx: tx.run(f"MATCH (n:{PHANTOM_ORG_LABEL}) WHERE n.reference = $id RETURN COUNT(n) as count", id=sample_id).single()["count"] or 0
                    )
                    if count > 0: found_in.append(f"{PHANTOM_ORG_LABEL} ({count})")

                    # Check published activities
                    count = session.execute_read(
                        lambda tx: tx.run(f"MATCH (n:{ACTIVITY_LABEL}) WHERE n.iatiidentifier = $id RETURN COUNT(n) as count", id=sample_id).single()["count"] or 0
                    )
                    if count > 0: found_in.append(f"{ACTIVITY_LABEL} ({count})")

                    # Check phantom activities
                    count = session.execute_read(
                        lambda tx: tx.run(f"MATCH (n:{PHANTOM_ACTIVITY_LABEL}) WHERE n.phantom_activity_identifier = $id RETURN COUNT(n) as count", id=sample_id).single()["count"] or 0
                    )
                    if count > 0: found_in.append(f"{PHANTOM_ACTIVITY_LABEL} ({count})")
                
                if found_in:
                     print(f"  ID '{sample_id}': Found as {', '.join(found_in)}")
                else:
                     print(f"  ID '{sample_id}': Not found as any standard node type.")
            
    except Exception as e:
        print(f"  Error during sampling check: {e}")
    
    print("--- End of Node Existence Check ---")


# --- Data Loading Function ---

def load_financial_edges(pg_conn, neo4j_driver, batch_size):
    """Loads financial transaction edges from PostgreSQL to Neo4j."""
    print(f"--- Loading Edges: {DBT_TARGET_SCHEMA}.{SOURCE_TABLE} -> :{NEO4J_EDGE_TYPE} ---")

    # Use defined constants for log filenames
    detail_log_filename = SKIPPED_DETAILS_LOG_FILENAME
    summary_log_filename = SUMMARY_LOG_FILENAME

    # Perform node existence check
    check_node_existence(neo4j_driver, pg_conn)

    # Initialize counters
    skipped_null_id_count = 0
    skipped_missing_node_count = 0
    successful_merge_operations = 0

    # 1. Get expected count from PostgreSQL
    expected_count = get_pg_count(pg_conn, DBT_TARGET_SCHEMA, SOURCE_TABLE)
    if expected_count is None: return False, 0, 0, 0
    if expected_count == 0:
        print(f"Skipping edge loading - no rows found in {DBT_TARGET_SCHEMA}.{SOURCE_TABLE}.")
        try:
            with open(summary_log_filename, 'w') as f:
                f.write(f"Skipped edge summary for {NEO4J_EDGE_TYPE}\n")
                f.write(f"Source: {DBT_TARGET_SCHEMA}.{SOURCE_TABLE}\n")
                f.write("No rows found in source table.\n")
                f.write("Skipped due to NULL IDs: 0\n")
                f.write("Skipped due to missing nodes (Neo4j): 0\n")
            print(f"Skip summary written to {os.path.abspath(summary_log_filename)}")
            with open(detail_log_filename, 'w') as f:
                f.write(f"{SOURCE_NODE_ID_COL}\t{TARGET_NODE_ID_COL}\tsource_type\ttarget_type\treason\n") # Header
            print(f"Skip details log initialized at {os.path.abspath(detail_log_filename)}")
        except IOError as e:
            print(f"Error writing summary/detail log file: {e}", file=sys.stderr)
        return True, 0, 0, 0

    # 2. Get current count from Neo4j (before loading)
    count_before = get_neo4j_edge_count(neo4j_driver, NEO4J_EDGE_TYPE)

    # 3. Prepare PostgreSQL Cursor
    pg_cursor = pg_conn.cursor(name='fetch_financial_links', cursor_factory=psycopg2.extras.DictCursor)
    pg_cursor.itersize = batch_size

    # 4. Prepare SELECT Query for all desired columns
    select_cols_str = ", ".join([f'"{c}"' for c in SOURCE_COLUMNS])
    select_query = f'SELECT {select_cols_str} FROM "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}";'

    # 5. Prepare Cypher Query for Batch Loading
    set_clauses = []
    for col in EDGE_PROPERTY_COLUMNS:
        # Use original column names directly as property keys in Cypher for simplicity
        # Neo4j property keys don't have the same restrictions as variables
        prop_name = col # e.g., total_value_usd
        set_clauses.append(f"r.{prop_name} = row.{prop_name}")
    set_clause_str = ", ".join(set_clauses)

    # This query handles conditional node matching based on type
    # Note: Using COALESCE to pick the first non-null matched node (published or phantom)
    cypher_query = f"""
    UNWIND $batch as row
    
    // Match source node conditionally
    WITH row
    OPTIONAL MATCH (pubOrgS:{ORGANISATION_LABEL}) WHERE row.{SOURCE_NODE_TYPE_COL} = 'ORGANISATION' AND pubOrgS.organisationidentifier = row.{SOURCE_NODE_ID_COL}
    OPTIONAL MATCH (phanOrgS:{PHANTOM_ORG_LABEL}) WHERE row.{SOURCE_NODE_TYPE_COL} = 'ORGANISATION' AND pubOrgS IS NULL AND phanOrgS.reference = row.{SOURCE_NODE_ID_COL}
    OPTIONAL MATCH (pubActS:{ACTIVITY_LABEL}) WHERE row.{SOURCE_NODE_TYPE_COL} = 'ACTIVITY' AND pubActS.iatiidentifier = row.{SOURCE_NODE_ID_COL}
    OPTIONAL MATCH (phanActS:{PHANTOM_ACTIVITY_LABEL}) WHERE row.{SOURCE_NODE_TYPE_COL} = 'ACTIVITY' AND pubActS IS NULL AND phanActS.phantom_activity_identifier = row.{SOURCE_NODE_ID_COL}
    WITH row, 
         COALESCE(pubOrgS, phanOrgS, pubActS, phanActS) as sourceNode
         
    // Match target node conditionally
    OPTIONAL MATCH (pubOrgT:{ORGANISATION_LABEL}) WHERE row.{TARGET_NODE_TYPE_COL} = 'ORGANISATION' AND pubOrgT.organisationidentifier = row.{TARGET_NODE_ID_COL}
    OPTIONAL MATCH (phanOrgT:{PHANTOM_ORG_LABEL}) WHERE row.{TARGET_NODE_TYPE_COL} = 'ORGANISATION' AND pubOrgT IS NULL AND phanOrgT.reference = row.{TARGET_NODE_ID_COL}
    OPTIONAL MATCH (pubActT:{ACTIVITY_LABEL}) WHERE row.{TARGET_NODE_TYPE_COL} = 'ACTIVITY' AND pubActT.iatiidentifier = row.{TARGET_NODE_ID_COL}
    OPTIONAL MATCH (phanActT:{PHANTOM_ACTIVITY_LABEL}) WHERE row.{TARGET_NODE_TYPE_COL} = 'ACTIVITY' AND pubActT IS NULL AND phanActT.phantom_activity_identifier = row.{TARGET_NODE_ID_COL}
    WITH row, sourceNode, 
         COALESCE(pubOrgT, phanOrgT, pubActT, phanActT) as targetNode

    // Conditional MERGE only if both nodes are found
    FOREACH (
        _ IN CASE WHEN sourceNode IS NOT NULL AND targetNode IS NOT NULL THEN [1] ELSE [] END |
        // MERGE creates or matches the relationship. Key needs to be unique for the relationship type.
        // Here, we assume source, target, and transaction type define uniqueness.
        MERGE (sourceNode)-[r:{NEO4J_EDGE_TYPE} {{ transactiontype_code: row.transactiontype_code }}]->(targetNode)
        ON CREATE SET {set_clause_str}
        ON MATCH SET {set_clause_str} // Update properties if relationship already exists
    )
    
    // Return details for rows where merge didn't happen
    WITH row, sourceNode, targetNode
    WHERE sourceNode IS NULL OR targetNode IS NULL
    RETURN 
        row.{SOURCE_NODE_ID_COL} as source_id, 
        row.{TARGET_NODE_ID_COL} as target_id,
        row.{SOURCE_NODE_TYPE_COL} as source_type,
        row.{TARGET_NODE_TYPE_COL} as target_type,
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
             print(f"Hint: A column in SOURCE_COLUMNS ({SOURCE_COLUMNS}) does not exist in '{DBT_TARGET_SCHEMA}.{SOURCE_TABLE}'. Verify SQL model and SOURCE_COLUMNS.", file=sys.stderr)
         pg_cursor.close()
         return False, skipped_null_id_count, skipped_missing_node_count, successful_merge_operations

    skipped_missing_node_count = 0
    print(f"Starting batch load (batch size: {batch_size})...")
    # print(f"Cypher Query Template: {cypher_query}") # Uncomment for debugging
    print(f"Skipped edge details will be logged to: {os.path.abspath(detail_log_filename)}")

    try:
        with open(detail_log_filename, 'a') as detail_log_file:
            # Write header if the file is new/empty
            if detail_log_file.tell() == 0:
                 detail_log_file.write(f"{SOURCE_NODE_ID_COL}\t{TARGET_NODE_ID_COL}\tsource_type\ttarget_type\treason\n") # Header

            with tqdm(total=expected_count, desc=f"Edges :{NEO4J_EDGE_TYPE}", unit=" edges") as pbar:
                 while True:
                    try:
                        batch_data = pg_cursor.fetchmany(batch_size)
                    except psycopg2.Error as e:
                         print(f"Error fetching batch from PostgreSQL: {e}", file=sys.stderr)
                         break

                    if not batch_data: break

                    batch_list = []
                    batch_initial_count = len(batch_data)

                    for row_dict in [dict(row) for row in batch_data]:
                        # Check for NULL IDs or types first
                        source_id = row_dict.get(SOURCE_NODE_ID_COL)
                        target_id = row_dict.get(TARGET_NODE_ID_COL)
                        source_type = row_dict.get(SOURCE_NODE_TYPE_COL)
                        target_type = row_dict.get(TARGET_NODE_TYPE_COL)

                        if not all([source_id, target_id, source_type, target_type]):
                            skipped_null_id_count += 1
                            # Log NULL/missing crucial info skips
                            reason = "NULL_ID_OR_TYPE"
                            s_id = source_id or 'NULL'
                            t_id = target_id or 'NULL'
                            s_type = source_type or 'NULL'
                            t_type = target_type or 'NULL'
                            detail_log_file.write(f"{s_id}\t{t_id}\t{s_type}\t{t_type}\t{reason}\n")
                            continue
                        
                        # Prepare item for batch: convert Decimal, ensure all props exist
                        sanitised_item = {}
                        for col in SOURCE_COLUMNS:
                            value = row_dict.get(col)
                            prop_name = col # Use original col name as key
                            if isinstance(value, Decimal):
                                value = float(value) # Convert Decimal to float for Neo4j
                            elif value is None and col in EDGE_PROPERTY_COLUMNS:
                                value = "" # Replace None with empty string for properties? Or handle in Cypher? Let's use empty string for now.
                            sanitised_item[prop_name] = value
                        
                        batch_list.append(sanitised_item)

                    pbar.update(batch_initial_count)

                    if not batch_list:
                        detail_log_file.flush()
                        continue

                    # Process the valid batch items with Neo4j
                    try:
                        with neo4j_driver.session(database="neo4j") as session:
                            results = session.execute_write(
                                lambda tx: tx.run(cypher_query, batch=batch_list).data()
                            )
                            
                            skipped_in_batch_neo4j = len(results)
                            merges_in_batch = len(batch_list) - skipped_in_batch_neo4j

                            successful_merge_operations += merges_in_batch
                            skipped_missing_node_count += skipped_in_batch_neo4j
                            
                            # Log details for skipped records from this batch
                            for skipped_record in results:
                                s_id = skipped_record.get('source_id', 'ERROR')
                                t_id = skipped_record.get('target_id', 'ERROR')
                                s_type = skipped_record.get('source_type', 'ERROR')
                                t_type = skipped_record.get('target_type', 'ERROR')
                                source_missing = skipped_record.get('source_missing', True)
                                target_missing = skipped_record.get('target_missing', True)
                                
                                reason = "UNKNOWN"
                                if source_missing and target_missing:
                                    reason = "BOTH_NODES_MISSING"
                                elif source_missing:
                                    reason = f"SOURCE_{s_type}_MISSING"
                                elif target_missing:
                                    reason = f"TARGET_{t_type}_MISSING"
                                    
                                detail_log_file.write(f"{s_id}\t{t_id}\t{s_type}\t{t_type}\t{reason}\n")

                            detail_log_file.flush()

                    except Exception as e:
                        print(f"Error processing batch in Neo4j: {e}", file=sys.stderr)
                        # print(f"Failed Cypher: {cypher_query}", file=sys.stderr) # Uncomment for debugging
                        # print(f"Failed Batch sample: {batch_list[:2]}", file=sys.stderr) # Uncomment for debugging
                        pg_cursor.close()
                        return False, skipped_null_id_count, skipped_missing_node_count, successful_merge_operations
                        
    except IOError as e:
        print(f"Error opening or writing to detail log file {detail_log_filename}: {e}", file=sys.stderr)
        pg_cursor.close()
        return False, skipped_null_id_count, skipped_missing_node_count, successful_merge_operations

    pg_cursor.close()
    
    # 7. Get final count from Neo4j (after loading)
    count_after = get_neo4j_edge_count(neo4j_driver, NEO4J_EDGE_TYPE)
    
    # Print summary of skipped edges
    print(f"--- Skipped Edges Summary ---")
    print(f"Total edges skipped due to NULL IDs/Types: {skipped_null_id_count}")
    print(f"Total edges skipped due to missing nodes (Neo4j): {skipped_missing_node_count}")
    total_skipped = skipped_null_id_count + skipped_missing_node_count
    print(f"Total skipped edges overall:             {total_skipped}")

    # Write summary to log file
    try:
        with open(summary_log_filename, 'w') as f:
            f.write(f"Skipped edge summary for {NEO4J_EDGE_TYPE}\n")
            f.write(f"Source: {DBT_TARGET_SCHEMA}.{SOURCE_TABLE}\n")
            f.write(f"Total expected edges (from PG): {expected_count}\n")
            f.write(f"Skipped due to NULL IDs/Types: {skipped_null_id_count}\n")
            f.write(f"Skipped due to missing nodes (Neo4j): {skipped_missing_node_count}\n")
            f.write(f"Total skipped: {total_skipped}\n")
            f.write(f"Total successful MERGE operations (created or matched): {successful_merge_operations}\n")
            f.write(f"Neo4j count before load: {count_before if count_before is not None else 'N/A'}\n")
            f.write(f"Neo4j count after load: {count_after if count_after is not None else 'N/A'}\n")
        print(f"Skip summary written to {os.path.abspath(summary_log_filename)}")
    except IOError as e:
        print(f"Error writing summary log file: {e}", file=sys.stderr)


    if count_after is not None and count_before is not None:
        new_edges_created = count_after - count_before
        print(f"--- Count Summary ---")
        print(f"Expected Edge Count (from PG table): {expected_count}")
        print(f"Skipped Edges (NULL IDs/Types):      {skipped_null_id_count}")
        print(f"Skipped Edges (missing nodes):       {skipped_missing_node_count}")
        net_expected_merges = expected_count - total_skipped
        print(f"Net Expected MERGE Operations:       {net_expected_merges}")
        print(f"Actual Successful MERGE Operations:  {successful_merge_operations}")
        print(f"Count Before Load:                   {count_before}")
        print(f"Count After Load (Neo4j):            {count_after}")
        print(f"Net New Edges Created:               {new_edges_created}")

        if successful_merge_operations != net_expected_merges:
             print(f"Warning: The number of successful MERGE operations ({successful_merge_operations}) does not match the net expected count ({net_expected_merges}). Check logs and batch processing.", file=sys.stderr)
        elif new_edges_created == 0 and successful_merge_operations > 0:
             print(f"Note: {successful_merge_operations} MERGE operations were successful, but no new edges were created. All relationships likely existed already and were updated (ON MATCH).")
        elif new_edges_created < successful_merge_operations:
             print(f"Note: {successful_merge_operations} MERGE operations were successful, creating {new_edges_created} new edges. Some existing relationships were matched and updated.")
    else:
        print("Could not verify final counts after loading.", file=sys.stderr)

    return True, skipped_null_id_count, skipped_missing_node_count, successful_merge_operations


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
        print("--- Starting Financial Edge Load ---")
        neo4j_driver = get_neo4j_driver()
        pg_conn = get_postgres_connection()

        success, final_null_skips, final_missing_node_skips, final_successful_merges = load_financial_edges(pg_conn, neo4j_driver, batch_size)

    except KeyboardInterrupt:
        print("Process interrupted by user.", file=sys.stderr)
        success = False
    except Exception as e:
        print(f"An unexpected error occurred during the loading process: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        success = False
    finally:
        if neo4j_driver: neo4j_driver.close(); print("Neo4j connection closed.")
        if pg_conn: pg_conn.close(); print("PostgreSQL connection closed.")

        end_time = time.time()
        print(f"Total execution time: {end_time - start_time:.2f} seconds.")

        if success:
            print("Financial edge loading process finished successfully.")
            print(f"Summary: Skipped (Null IDs/Types): {final_null_skips}, Skipped (Missing Nodes): {final_missing_node_skips}, Successful Merges: {final_successful_merges}")
        else:
            print("Financial edge loading process finished with errors or was interrupted.", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main() 