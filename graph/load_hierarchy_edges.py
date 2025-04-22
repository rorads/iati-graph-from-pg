#!/usr/bin/env python3
"""
graph/load_hierarchy_edges.py

Loads parent-child relationships between activities into Neo4j efficiently,
using pre-fetched node IDs, batched writes, and detailed logging inspired by
load_funds_edges.py.
"""
import os
import sys
import time
from tqdm import tqdm

import psycopg2
import psycopg2.extras

# Import shared database functions and configuration
# Ensure db_utils.py is in the same directory or Python path
try:
    from db_utils import get_neo4j_driver, get_postgres_connection
except ImportError:
    print("Error: Unable to import db_utils. Make sure db_utils.py is accessible.", file=sys.stderr)
    sys.exit(1)

# --- Configuration ---
DBT_TARGET_SCHEMA = "iati_graph"
SOURCE_TABLE = "hierarchy_links"
NEO4J_EDGE_TYPE = "PARENT_OF"

ACTIVITY_LABEL = "PublishedActivity"
PHANTOM_ACTIVITY_LABEL = "PhantomActivity"

# Source table columns
SOURCE_NODE_ID_COL = "source_node_id"
TARGET_NODE_ID_COL = "target_node_id"
# These node type columns exist in hierarchy_links but aren't strictly needed if we pre-fetch
# SOURCE_NODE_TYPE_COL = "source_node_type"
# TARGET_NODE_TYPE_COL = "target_node_type"
DECLARED_BY_COL = "declared_by" # Note: PG type is text[], Cypher expects single value

# Columns to load from PostgreSQL source table
SOURCE_COLUMNS = [
    SOURCE_NODE_ID_COL,
    TARGET_NODE_ID_COL,
    DECLARED_BY_COL
]

DEFAULT_BATCH_SIZE = 1000 # Keep batch size reasonable

# Logging Configuration (relative to script execution dir, which is 'graph')
LOG_DIR = "logs"
SKIPPED_DETAILS_LOG_FILENAME = os.path.join(LOG_DIR, "hierarchy_edges_skipped_details.log")
SUMMARY_LOG_FILENAME = os.path.join(LOG_DIR, "hierarchy_edges_skipped_summary.log")

# --- Helper Functions ---

def get_pg_count(pg_conn, schema, table):
    """Gets the total row count from a PostgreSQL table."""
    # Use try-with-resources for cursor
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}";')
            count = cursor.fetchone()[0]
            print(f"Source Table: Found {count} rows in {schema}.{table}")
            return count
    except psycopg2.Error as e:
        print(f"Error getting count from {schema}.{table}: {e}", file=sys.stderr)
        return None

def get_neo4j_edge_count(neo4j_driver, edge_type):
    """Gets the count of edges with a specific type in Neo4j."""
    cypher = f"MATCH ()-[r:{edge_type}]->() RETURN count(r) AS count"
    try:
        with neo4j_driver.session() as session:
            result = session.read_transaction(lambda tx: tx.run(cypher).single())
            count = result["count"] if result else 0
            print(f"Neo4j: Found {count} existing :{edge_type} edges.")
            return count
    except Exception as e:
        print(f"Error getting Neo4j edge count for :{edge_type}: {e}", file=sys.stderr)
        return None

def fetch_activity_ids(pg_conn, schema, table, id_column, node_type_desc):
    """Fetches all unique, non-null IDs from a specified PG table."""
    print(f"Pre-fetching {node_type_desc} node IDs from {schema}.{table}...")
    ids = set()
    query = f"SELECT DISTINCT \"{id_column}\" FROM \"{schema}\".\"{table}\" WHERE \"{id_column}\" IS NOT NULL"
    # Use try-with-resources for cursor
    try:
        with pg_conn.cursor(name=f"fetch_{id_column}_cursor") as cursor:
            cursor.execute(query)
            # Iterate efficiently
            for row in cursor:
                ids.add(row[0])
        print(f"  Fetched {len(ids)} unique {node_type_desc} IDs.")
        return ids
    except psycopg2.Error as e:
        print(f"Error fetching {node_type_desc} IDs: {e}", file=sys.stderr)
        return None

def run_neo4j_merge_batch(neo4j_driver, batch_data):
    """
    Executes the batched Cypher query to merge edges, handling potential missing nodes.
    Returns a tuple: (number_of_merges_attempted, list_of_skipped_rows_details)
    """
    if not batch_data:
        return 0, []

    # Cypher similar to load_funds_edges: OPTIONAL MATCH, COALESCE, FOREACH/CASE MERGE
    # Note: We don't need the type flags (src_is_published etc.) anymore
    batched_cypher = f"""
    UNWIND $batch AS row

    // Optional match source node (published or phantom)
    WITH row
    OPTIONAL MATCH (pubSrc:{ACTIVITY_LABEL}) WHERE pubSrc.iatiidentifier = row.src_id
    OPTIONAL MATCH (phanSrc:{PHANTOM_ACTIVITY_LABEL}) WHERE pubSrc IS NULL AND phanSrc.phantom_activity_identifier = row.src_id
    WITH row, COALESCE(pubSrc, phanSrc) as sourceNode

    // Optional match target node (published or phantom)
    OPTIONAL MATCH (pubTgt:{ACTIVITY_LABEL}) WHERE pubTgt.iatiidentifier = row.tgt_id
    OPTIONAL MATCH (phanTgt:{PHANTOM_ACTIVITY_LABEL}) WHERE pubTgt IS NULL AND phanTgt.phantom_activity_identifier = row.tgt_id
    WITH row, sourceNode, COALESCE(pubTgt, phanTgt) as targetNode

    // Conditional MERGE only if both nodes are found
    FOREACH (
        _ IN CASE WHEN sourceNode IS NOT NULL AND targetNode IS NOT NULL THEN [1] ELSE [] END |
        MERGE (sourceNode)-[rel:{NEO4J_EDGE_TYPE}]->(targetNode)
        // Set properties (handle potential array from PG - take first element or null)
        SET rel.{DECLARED_BY_COL} = CASE
            WHEN row.declared IS NULL THEN null
            WHEN size(row.declared) > 0 THEN row.declared[0]
            ELSE null
        END
    )

    // Return details for rows where merge didn't happen (nodes missing)
    WITH row, sourceNode, targetNode
    WHERE sourceNode IS NULL OR targetNode IS NULL
    RETURN
        row.src_id as source_id,
        row.tgt_id as target_id,
        sourceNode IS NULL as source_missing,
        targetNode IS NULL as target_missing
    """
    try:
        with neo4j_driver.session() as session:
            results = session.execute_write(lambda tx: tx.run(batched_cypher, batch=batch_data).data())
            merges_attempted = len(batch_data) - len(results)
            skipped_details = results # List of dictionaries with skip info
            return merges_attempted, skipped_details
    except Exception as e:
        print(f"\nError processing Neo4j batch: {e}", file=sys.stderr)
        # Indicate error: return -1 merges, empty skip list
        return -1, []

# --- Main Loading Function ---

def load_hierarchy_edges(pg_conn, neo4j_driver, batch_size):
    """Loads parent-child relationships from PostgreSQL to Neo4j."""
    print(f"\n--- Starting Edge Load: {DBT_TARGET_SCHEMA}.{SOURCE_TABLE} -> :{NEO4J_EDGE_TYPE} ---")
    start_time = time.time()

    # 1. Ensure log directory exists
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        print(f"Log directory ensured at: {os.path.abspath(LOG_DIR)}")
    except OSError as e:
        print(f"Error creating log directory '{LOG_DIR}': {e}", file=sys.stderr)
        return False # Cannot proceed without logging

    # 2. Get initial counts
    expected_pg_count = get_pg_count(pg_conn, DBT_TARGET_SCHEMA, SOURCE_TABLE)
    if expected_pg_count is None: return False
    if expected_pg_count == 0:
        print("Source table is empty. Skipping load.")
        try:
            with open(SUMMARY_LOG_FILENAME, 'w') as f:
                f.write("Skipped: Source table was empty.")
            with open(SKIPPED_DETAILS_LOG_FILENAME, 'w') as f:
                 f.write(f"{SOURCE_NODE_ID_COL}\t{TARGET_NODE_ID_COL}\tskip_reason\n") # Header
        except IOError as e:
            print(f"Error writing empty log files: {e}", file=sys.stderr)
        return True

    initial_neo4j_count = get_neo4j_edge_count(neo4j_driver, NEO4J_EDGE_TYPE)
    if initial_neo4j_count is None: return False

    # 3. Pre-fetching IDs is now OPTIONAL but can be useful for initial sanity checks or summary reporting
    # published_ids = fetch_activity_ids(pg_conn, DBT_TARGET_SCHEMA, "published_activities", "iatiidentifier", "published activity")
    # phantom_ids = fetch_activity_ids(pg_conn, DBT_TARGET_SCHEMA, "phantom_activities", "phantom_activity_identifier", "phantom activity")
    # if published_ids is None or phantom_ids is None:
    #     print("Failed to pre-fetch node IDs (optional check). Continuing load.", file=sys.stderr)
        # No longer fatal if this fails

    # 4. Fetch from PG and load to Neo4j in batches
    query = f"SELECT {', '.join(SOURCE_COLUMNS)} FROM \"{DBT_TARGET_SCHEMA}\".\"{SOURCE_TABLE}\""
    pg_cursor = None
    detail_log_file = None
    processed_pg_rows = 0
    skipped_null_id_count = 0
    skipped_missing_node_count = 0 # Count skips identified by Neo4j
    successful_merge_operations = 0 # Count merges attempted by Neo4j query
    neo4j_batch = []

    try:
        # Open detail log for writing
        detail_log_file = open(SKIPPED_DETAILS_LOG_FILENAME, 'w')
        detail_log_file.write(f"{SOURCE_NODE_ID_COL}\t{TARGET_NODE_ID_COL}\tskip_reason\n") # Header
        print(f"Logging skipped edge details to: {os.path.abspath(SKIPPED_DETAILS_LOG_FILENAME)}")

        # Use a named server-side cursor
        pg_cursor = pg_conn.cursor(name="hierarchy_edge_cursor", cursor_factory=psycopg2.extras.DictCursor)
        pg_cursor.itersize = batch_size
        pg_cursor.execute(query)

        print("Iterating through source rows and preparing batches...")
        with tqdm(total=expected_pg_count, desc=f"Processing {SOURCE_TABLE}", unit=" rows") as pbar:
            while True:
                try:
                    pg_batch = pg_cursor.fetchmany(batch_size)
                except psycopg2.Error as e:
                    print(f"\nError fetching batch from PostgreSQL: {e}", file=sys.stderr)
                    raise # Re-raise to be caught by outer try-except

                if not pg_batch:
                    break # End of data

                rows_in_pg_batch = len(pg_batch)
                processed_pg_rows += rows_in_pg_batch

                for row in pg_batch:
                    src_id = row[SOURCE_NODE_ID_COL]
                    tgt_id = row[TARGET_NODE_ID_COL]
                    declared = row[DECLARED_BY_COL]

                    # Validate NULLs before adding to batch
                    if not src_id or not tgt_id:
                        skipped_null_id_count += 1
                        reason = "NULL_SOURCE_ID" if not src_id else "NULL_TARGET_ID"
                        s_id_log = src_id or 'NULL'
                        t_id_log = tgt_id or 'NULL'
                        detail_log_file.write(f"{s_id_log}\t{t_id_log}\t{reason}\n")
                        continue

                    # REMOVED: Python pre-check using published_ids/phantom_ids
                    # src_is_published = src_id in published_ids
                    # src_is_phantom = src_id in phantom_ids
                    # tgt_is_published = tgt_id in published_ids
                    # tgt_is_phantom = tgt_id in phantom_ids
                    # src_exists = src_is_published or src_is_phantom
                    # tgt_exists = tgt_is_published or tgt_is_phantom
                    # if not src_exists or not tgt_exists:
                    #     ...
                    #     log skip ...
                    #     continue

                    # Add to Neo4j batch (send all non-null ID rows)
                    neo4j_batch.append({
                        "src_id": src_id,
                        "tgt_id": tgt_id,
                        "declared": declared,
                        # REMOVED: Don't need type flags anymore
                        # "src_is_published": src_is_published,
                        # "tgt_is_published": tgt_is_published,
                    })

                    # Process Neo4j batch if full
                    if len(neo4j_batch) >= batch_size:
                        merges_attempted, skipped_details = run_neo4j_merge_batch(neo4j_driver, neo4j_batch)

                        if merges_attempted < 0: # Check for error signal
                            print("Aborting due to error during batch processing.", file=sys.stderr)
                            raise Exception("Neo4j batch processing failed") # Raise to abort

                        successful_merge_operations += merges_attempted
                        skipped_missing_node_count += len(skipped_details)

                        # Log skips identified by Neo4j
                        for skip_info in skipped_details:
                            s_id = skip_info.get('source_id', 'ERROR')
                            t_id = skip_info.get('target_id', 'ERROR')
                            source_missing = skip_info.get('source_missing', True)
                            target_missing = skip_info.get('target_missing', True)
                            reason = "UNKNOWN_NODE_MISSING"
                            if source_missing and target_missing:
                                reason = "BOTH_NODES_MISSING"
                            elif source_missing:
                                reason = "SOURCE_NODE_MISSING"
                            elif target_missing:
                                reason = "TARGET_NODE_MISSING"
                            detail_log_file.write(f"{s_id}\t{t_id}\t{reason}\n")

                        neo4j_batch = [] # Reset batch

                # Update progress bar after processing the pg_batch
                pbar.update(rows_in_pg_batch)
                detail_log_file.flush() # Flush logs periodically

        # Process the final batch
        if neo4j_batch:
            merges_attempted, skipped_details = run_neo4j_merge_batch(neo4j_driver, neo4j_batch)
            if merges_attempted >= 0:
                successful_merge_operations += merges_attempted
                skipped_missing_node_count += len(skipped_details)
                # Log final skips
                for skip_info in skipped_details:
                    s_id = skip_info.get('source_id', 'ERROR')
                    t_id = skip_info.get('target_id', 'ERROR')
                    source_missing = skip_info.get('source_missing', True)
                    target_missing = skip_info.get('target_missing', True)
                    reason = "UNKNOWN_NODE_MISSING"
                    if source_missing and target_missing:
                        reason = "BOTH_NODES_MISSING"
                    elif source_missing:
                        reason = "SOURCE_NODE_MISSING"
                    elif target_missing:
                        reason = "TARGET_NODE_MISSING"
                    detail_log_file.write(f"{s_id}\t{t_id}\t{reason}\n")
            else:
                 print("Error processing final batch.", file=sys.stderr)
                 # Allow summary reporting even if final batch fails

    except psycopg2.Error as e:
        print(f"\nDatabase error during processing: {e}", file=sys.stderr)
        if pg_conn.closed == 0:
             pg_conn.rollback() # Rollback PG transaction
        return False # Indicate failure
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}", file=sys.stderr)
        if pg_conn and pg_conn.closed == 0:
            pg_conn.rollback()
        return False # Indicate failure
    finally:
        if pg_cursor:
            pg_cursor.close()
        if detail_log_file:
            detail_log_file.close()
            print(f"Closed detail log file.")

    # 5. Final counts and reporting
    end_time = time.time()
    final_neo4j_count = get_neo4j_edge_count(neo4j_driver, NEO4J_EDGE_TYPE)
    actual_loaded = (final_neo4j_count - initial_neo4j_count) if final_neo4j_count is not None else 'N/A'
    print("--- Load Summary ---")
    print(f"Processed {processed_pg_rows} rows from {SOURCE_TABLE}.")
    print(f"Skipped {skipped_null_id_count} rows due to NULL IDs (PG check).")
    print(f"Skipped {skipped_missing_node_count} rows due to missing nodes (Neo4j check).")
    print(f"Attempted to merge {successful_merge_operations} edges in batches.")
    if final_neo4j_count is not None:
        print(f"Neo4j :{NEO4J_EDGE_TYPE} count: Before={initial_neo4j_count}, After={final_neo4j_count}, Diff={actual_loaded}")
    else:
        print("Could not verify final Neo4j edge count.")
    print(f"Total time: {end_time - start_time:.2f} seconds.")

    # Write summary log
    try:
        with open(SUMMARY_LOG_FILENAME, 'w') as f:
            f.write(f"Edge Load Summary for {NEO4J_EDGE_TYPE}\n")
            f.write(f"Source: {DBT_TARGET_SCHEMA}.{SOURCE_TABLE}\n")
            f.write(f"Total source rows processed: {processed_pg_rows} (Expected: {expected_pg_count})\n")
            f.write(f"Skipped due to NULL IDs (PG check): {skipped_null_id_count}\n")
            f.write(f"Skipped due to missing nodes (Neo4j check): {skipped_missing_node_count}\n")
            f.write(f"Successful merge operations (batches): {successful_merge_operations}\n")
            f.write(f"Neo4j edge count before: {initial_neo4j_count}\n")
            f.write(f"Neo4j edge count after: {final_neo4j_count}\n")
            f.write(f"Net change in Neo4j: {actual_loaded}\n")
            f.write(f"Total execution time: {end_time - start_time:.2f} seconds\n")

            expected_success = expected_pg_count - (skipped_null_id_count + skipped_missing_node_count)
            # Note: successful_merge_operations counts the *attempts* within the FOREACH/CASE.
            # This count should match expected_success if logic is correct.
            if successful_merge_operations != expected_success:
                 f.write(f"WARNING: Merge operation count ({successful_merge_operations}) does not match expected successful rows ({expected_success}). Potential issue in batching or counting.\n")
            if final_neo4j_count is not None and actual_loaded != successful_merge_operations:
                 # This warning might still trigger if relationships already existed and were updated (ON MATCH)
                 # rather than created (ON CREATE), as merge operations count includes both.
                 f.write(f"WARNING: Net increase in Neo4j ({actual_loaded}) may not match merge operations ({successful_merge_operations}) if existing edges were updated.\n")
        print(f"Summary log written to: {os.path.abspath(SUMMARY_LOG_FILENAME)}")
    except IOError as e:
        print(f"Error writing summary log file: {e}", file=sys.stderr)

    print(f"--- Finished Edge Load: {DBT_TARGET_SCHEMA}.{SOURCE_TABLE} -> :{NEO4J_EDGE_TYPE} ---")
    return True # Indicate successful completion

# --- Main Execution ---

def main():
    # Argument parsing could be added here if needed (e.g., for batch size)
    # parser = argparse.ArgumentParser(...)
    # args = parser.parse_args()
    # batch_size = args.batch_size or DEFAULT_BATCH_SIZE
    batch_size = DEFAULT_BATCH_SIZE

    pg_conn = None
    neo4j_driver = None
    success = False
    exit_code = 0

    try:
        print("Establishing database connections...")
        # Use try-with-resources for connections if preferred, but requires context managers in db_utils
        pg_conn = get_postgres_connection()
        neo4j_driver = get_neo4j_driver()

        if pg_conn and neo4j_driver:
            # Using server-side cursors, autocommit should generally be OFF
            pg_conn.autocommit = False
            print("Running hierarchy edge load...")
            success = load_hierarchy_edges(pg_conn, neo4j_driver, batch_size)

            if success:
                 print("Load function reported success. Committing transaction.")
                 pg_conn.commit()
            else:
                 print("Load function reported failure. Rolling back transaction.")
                 pg_conn.rollback()
                 exit_code = 1 # Signal failure
        else:
            print("Failed to establish database connections. Exiting.", file=sys.stderr)
            exit_code = 1

    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Rolling back transaction.")
        if pg_conn and pg_conn.closed == 0:
            pg_conn.rollback()
        exit_code = 1
    except Exception as e:
        print(f"\nAn unexpected error occurred in main: {e}", file=sys.stderr)
        if pg_conn and pg_conn.closed == 0:
            pg_conn.rollback()
        exit_code = 1
    finally:
        # Ensure connections are closed
        if pg_conn:
            try:
                pg_conn.close()
                print("PostgreSQL connection closed.")
            except Exception as e:
                print(f"Error closing PostgreSQL connection: {e}", file=sys.stderr)
        if neo4j_driver:
            try:
                neo4j_driver.close()
                print("Neo4j driver closed.")
            except Exception as e:
                print(f"Error closing Neo4j driver: {e}", file=sys.stderr)

    if exit_code == 0:
        print("Hierarchy edge loading completed successfully.")
    else:
        print("Hierarchy edge loading failed or was interrupted.", file=sys.stderr)

    sys.exit(exit_code)

if __name__ == '__main__':
    main()