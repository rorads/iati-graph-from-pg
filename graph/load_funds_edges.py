# graph/load_funds_edges.py

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
SOURCE_TABLE = "funds_links"
NEO4J_EDGE_TYPE = "FUNDS" # Edge type/relationship name

# Node labels for source and target (only activities for this relationship)
ACTIVITY_LABEL = "PublishedActivity"
PHANTOM_ACTIVITY_LABEL = "PhantomActivity"

# Column names from funds_links.sql
SOURCE_NODE_ID_COL = "source_node_id"
TARGET_NODE_ID_COL = "target_node_id"
SOURCE_NODE_TYPE_COL = "source_node_type" # Always 'ACTIVITY' for this relationship
TARGET_NODE_TYPE_COL = "target_node_type" # Always 'ACTIVITY' for this relationship

# Columns to load from PostgreSQL - based on the SQL model
SOURCE_COLUMNS = [
    SOURCE_NODE_ID_COL,
    TARGET_NODE_ID_COL,
    SOURCE_NODE_TYPE_COL,
    TARGET_NODE_TYPE_COL,
    "currency",
    "total_value_usd"
]

# Edge property columns (these become properties on the relationship)
EDGE_PROPERTY_COLUMNS = [
    "currency",
    "total_value_usd"
]

# Processing Batch Size
DEFAULT_BATCH_SIZE = 500

# Log file for skipped edge details
LOG_DIR = "logs" # Define log directory
SKIPPED_DETAILS_LOG_FILENAME = os.path.join(LOG_DIR, "funds_edges_skipped_details.log")
SUMMARY_LOG_FILENAME = os.path.join(LOG_DIR, "funds_edges_skipped_summary.log")

def check_node_existence(neo4j_driver, pg_conn, batch_size=100):
    """Samples IDs and checks node existence to help debug missing nodes."""
    print("\n--- Node Existence Check (Debugging) ---")

    # Log node counts first
    node_types = [
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

    # Sample some source and target IDs from the funds_links table
    print("\n  Sampling IDs from funds_links table:")
    sample_ids = set()
    try:
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(f"""
            (SELECT 
                {SOURCE_NODE_ID_COL} AS id, 'SOURCE' AS type,
                {SOURCE_NODE_TYPE_COL} AS node_type
            FROM 
                "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}" 
            LIMIT {batch_size})
            UNION ALL
            (SELECT 
                {TARGET_NODE_ID_COL} AS id, 'TARGET' AS type,
                {TARGET_NODE_TYPE_COL} AS node_type
            FROM 
                "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}" 
            LIMIT {batch_size})
        """)
        
        for row in pg_cursor.fetchall():
            id_val, id_type, node_type = row
            sample_ids.add((id_val, id_type, node_type))
        
        pg_cursor.close()
    except Exception as e:
        print(f"  Error sampling IDs: {e}")
        return
    
    # Check if these IDs exist in Neo4j
    print(f"  Checking {len(sample_ids)} ID samples in Neo4j...")
    
    for id_val, id_type, node_type in sample_ids:
        if not id_val:
            print(f"  Warning: NULL ID value found in {id_type} field")
            continue
        
        # For activities, check both published and phantom nodes
        if node_type == 'ACTIVITY':
            cypher = f"""
            OPTIONAL MATCH (pub:{ACTIVITY_LABEL}) WHERE pub.iatiidentifier = $id
            OPTIONAL MATCH (phan:{PHANTOM_ACTIVITY_LABEL}) WHERE phan.phantom_activity_identifier = $id
            RETURN pub IS NOT NULL OR phan IS NOT NULL AS exists
            """
        else:
            print(f"  Unexpected node type: {node_type} for ID: {id_val}")
            continue
        
        try:
            with neo4j_driver.session() as session:
                result = session.execute_read(lambda tx: tx.run(cypher, id=id_val).single())
                exists = result["exists"] if result else False
                if not exists:
                    print(f"  Warning: {id_type} {node_type} ID '{id_val}' not found in Neo4j")
        except Exception as e:
            print(f"  Error checking existence of ID '{id_val}': {e}")
    
    print("--- End of Node Existence Check ---\n")


def get_pg_count(pg_conn, schema, table):
    """Gets the row count from a PostgreSQL table."""
    try:
        cursor = pg_conn.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}";')
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except psycopg2.Error as e:
        print(f"Error getting count from {schema}.{table}: {e}", file=sys.stderr)
        if "relation" in str(e) and "does not exist" in str(e):
            print(f"Hint: Ensure schema '{schema}' and table '{table}' exist and are accessible by user '{pg_conn.info.user}'.", file=sys.stderr)
        return None


def get_neo4j_edge_count(neo4j_driver, edge_type):
    """Gets the count of a specific relationship type from Neo4j."""
    cypher = f"MATCH ()-[r:{edge_type}]->() RETURN count(r) AS count"
    try:
        with neo4j_driver.session() as session:
            result = session.execute_read(lambda tx: tx.run(cypher).single())
            return result["count"] if result else 0
    except Exception as e:
        print(f"Error getting Neo4j count for {edge_type}: {e}", file=sys.stderr)
        return 0


def load_funds_edges(pg_conn, neo4j_driver, batch_size):
    """Loads funds relationship edges from PostgreSQL to Neo4j."""
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
    pg_cursor = pg_conn.cursor(name='fetch_funds_links', cursor_factory=psycopg2.extras.DictCursor)
    pg_cursor.itersize = batch_size

    # 4. Prepare SELECT Query for all desired columns
    select_cols_str = ", ".join([f'"{c}"' for c in SOURCE_COLUMNS])
    select_query = f'SELECT {select_cols_str} FROM "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}";'

    # 5. Prepare Cypher Query for Batch Loading
    set_clauses = []
    for col in EDGE_PROPERTY_COLUMNS:
        # Use original column names directly as property keys in Cypher for simplicity
        prop_name = col # e.g., total_value_usd
        set_clauses.append(f"r.{prop_name} = row.{prop_name}")
    set_clause_str = ", ".join(set_clauses)

    # This query handles conditional node matching based on type (published or phantom activities)
    cypher_query = f"""
    UNWIND $batch as row
    
    // Match source node conditionally
    WITH row
    OPTIONAL MATCH (pubActS:{ACTIVITY_LABEL}) WHERE pubActS.iatiidentifier = row.{SOURCE_NODE_ID_COL}
    OPTIONAL MATCH (phanActS:{PHANTOM_ACTIVITY_LABEL}) WHERE pubActS IS NULL AND phanActS.phantom_activity_identifier = row.{SOURCE_NODE_ID_COL}
    WITH row, 
         COALESCE(pubActS, phanActS) as sourceNode
         
    // Match target node conditionally
    OPTIONAL MATCH (pubActT:{ACTIVITY_LABEL}) WHERE pubActT.iatiidentifier = row.{TARGET_NODE_ID_COL}
    OPTIONAL MATCH (phanActT:{PHANTOM_ACTIVITY_LABEL}) WHERE pubActT IS NULL AND phanActT.phantom_activity_identifier = row.{TARGET_NODE_ID_COL}
    WITH row, sourceNode, 
         COALESCE(pubActT, phanActT) as targetNode

    // Conditional MERGE only if both nodes are found
    FOREACH (
        _ IN CASE WHEN sourceNode IS NOT NULL AND targetNode IS NOT NULL THEN [1] ELSE [] END |
        MERGE (sourceNode)-[r:{NEO4J_EDGE_TYPE}]->(targetNode)
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
    print(f"Skipped edge details will be logged to: {os.path.abspath(detail_log_filename)}")

    # Open detail log file for writing skip details
    try:
        detail_log_file = open(detail_log_filename, 'w')
        # Write header
        detail_log_file.write(f"{SOURCE_NODE_ID_COL}\t{TARGET_NODE_ID_COL}\tsource_type\ttarget_type\treason\n")
    except IOError as e:
        print(f"Error opening detail log file: {e}", file=sys.stderr)
        pg_cursor.close()
        return False, skipped_null_id_count, skipped_missing_node_count, successful_merge_operations

    # Process in batches with a progress bar
    try:
        with tqdm(total=expected_count, desc="Loading edges", unit="rows") as pbar:
            # Get an estimate of total expected batches for progress display
            total_batches = (expected_count + batch_size - 1) // batch_size
            
            while True:
                batch_data = pg_cursor.fetchmany(batch_size)
                batch_initial_count = len(batch_data)
                
                if not batch_data:
                    break # No more data
                
                batch_list = []
                
                # Process each row in the batch
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
                            value = "" # Replace None with empty string for properties
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
                    print(f"Error in Neo4j batch processing: {e}", file=sys.stderr)
                    # Continue with the next batch despite errors
    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
    except Exception as e:
        print(f"Error during batch processing: {e}", file=sys.stderr)
    finally:
        pg_cursor.close()
        detail_log_file.close()

    # 7. Get final count from Neo4j (after loading)
    count_after = get_neo4j_edge_count(neo4j_driver, NEO4J_EDGE_TYPE)
    actual_loaded = count_after - count_before
    
    # 8. Save skip summary
    try:
        with open(summary_log_filename, 'w') as f:
            f.write(f"Skipped edge summary for {NEO4J_EDGE_TYPE}\n")
            f.write(f"Source: {DBT_TARGET_SCHEMA}.{SOURCE_TABLE}\n")
            f.write(f"Total source rows: {expected_count}\n")
            f.write(f"Skipped due to NULL IDs: {skipped_null_id_count}\n")
            f.write(f"Skipped due to missing nodes (Neo4j): {skipped_missing_node_count}\n")
            f.write(f"Successful merge operations: {successful_merge_operations}\n")
            f.write(f"Neo4j edge count before: {count_before}\n")
            f.write(f"Neo4j edge count after: {count_after}\n")
            f.write(f"Net change in Neo4j: {actual_loaded}\n")
            
            # Warn about discrepancies
            expected_success = expected_count - (skipped_null_id_count + skipped_missing_node_count)
            if successful_merge_operations != expected_success:
                f.write(f"WARNING: Merge operation count ({successful_merge_operations}) does not match expected ({expected_success})\n")
            if actual_loaded != successful_merge_operations:
                f.write(f"WARNING: Net increase in Neo4j ({actual_loaded}) does not match merge operations ({successful_merge_operations})\n")
                
        print(f"Skip summary written to {os.path.abspath(summary_log_filename)}")
    except IOError as e:
        print(f"Error writing summary log file: {e}", file=sys.stderr)

    print(f"--- Load summary for :{NEO4J_EDGE_TYPE} ---")
    print(f"Total source rows: {expected_count}")
    print(f"Skipped due to NULL IDs: {skipped_null_id_count}")
    print(f"Skipped due to missing nodes: {skipped_missing_node_count}")
    print(f"Successful merge operations: {successful_merge_operations}")
    print(f"Neo4j relationship count before: {count_before}")
    print(f"Neo4j relationship count after: {count_after}")
    print(f"Net change in Neo4j: {actual_loaded}")
    
    return True, skipped_null_id_count, skipped_missing_node_count, successful_merge_operations


def main():
    parser = argparse.ArgumentParser(description='Load funds relationship edges from PostgreSQL to Neo4j.')
    parser.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE,
                      help=f'Batch size for processing (default: {DEFAULT_BATCH_SIZE})')
    args = parser.parse_args()
    
    # Connect to databases
    neo4j_driver = get_neo4j_driver()
    pg_conn = get_postgres_connection()
    
    if not neo4j_driver or not pg_conn:
        print("Failed to connect to one or both databases.")
        return 1

    try:
        # Load the edges
        start_time = time.time()
        success, skipped_null, skipped_missing, successful = load_funds_edges(pg_conn, neo4j_driver, args.batch_size)
        elapsed = time.time() - start_time
        print(f"Process{'ed' if success else ' failed'} in {elapsed:.2f} seconds.")
        
        if success:
            return 0
        else:
            return 1
    finally:
        # Clean up connections
        if neo4j_driver:
            neo4j_driver.close()
        if pg_conn:
            pg_conn.close()


if __name__ == "__main__":
    sys.exit(main()) 