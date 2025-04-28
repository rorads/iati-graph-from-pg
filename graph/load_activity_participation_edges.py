# graph/load_activity_participation_edges.py
"""
Loads activity-to-activity participation edges from PostgreSQL (activity_participation_summary_links)
to Neo4j, matching both published and phantom activities for source and target nodes.
Each edge includes role_codes and role_names as array properties.

Usage:
    python load_activity_participation_edges.py --batch-size 100

Logs:
    - logs/activity_participation_edges_skipped_details.log
    - logs/activity_participation_edges_skipped_summary.log
"""

import argparse
import os
import sys
import time
from decimal import Decimal

import psycopg2
import psycopg2.extras
from tqdm import tqdm

from db_utils import get_neo4j_driver, get_postgres_connection

# --- Configuration ---
DBT_TARGET_SCHEMA = "iati_graph"
SOURCE_TABLE = "activity_participation_summary_links"
NEO4J_EDGE_TYPE = "ACTIVITY_PARTICIPATION"  # Edge type/relationship name

ACTIVITY_LABEL = "PublishedActivity"
PHANTOM_ACTIVITY_LABEL = "PhantomActivity"

SOURCE_NODE_ID_COL = "source_activity_id"
TARGET_NODE_ID_COL = "target_activity_id"
ROLE_CODES_COL = "role_codes"
ROLE_NAMES_COL = "role_names"

SOURCE_COLUMNS = [
    SOURCE_NODE_ID_COL,
    TARGET_NODE_ID_COL,
    ROLE_CODES_COL,
    ROLE_NAMES_COL
]

EDGE_PROPERTY_COLUMNS = [
    ROLE_CODES_COL,
    ROLE_NAMES_COL
]

DEFAULT_BATCH_SIZE = 500
LOG_DIR = "logs"
SKIPPED_DETAILS_LOG_FILENAME = os.path.join(LOG_DIR, "activity_participation_edges_skipped_details.log")
SUMMARY_LOG_FILENAME = os.path.join(LOG_DIR, "activity_participation_edges_skipped_summary.log")


def check_node_existence(neo4j_driver, pg_conn, batch_size=100):
    """Samples IDs and checks node existence to help debug missing nodes."""
    print("\n--- Node Existence Check (Debugging) ---")
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
    print("\n  Sampling IDs from activity_participation_summary_links table:")
    sample_ids = set()
    try:
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(f"""
            (SELECT {SOURCE_NODE_ID_COL} AS id, 'SOURCE' AS type FROM "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}" LIMIT {batch_size})
            UNION ALL
            (SELECT {TARGET_NODE_ID_COL} AS id, 'TARGET' AS type FROM "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}" LIMIT {batch_size})
        """)
        for row in pg_cursor.fetchall():
            id_val, id_type = row
            sample_ids.add((id_val, id_type))
        pg_cursor.close()
    except Exception as e:
        print(f"  Error sampling IDs: {e}")
        return
    print(f"  Checking {len(sample_ids)} ID samples in Neo4j...")
    for id_val, id_type in sample_ids:
        if not id_val:
            print(f"  Warning: NULL ID value found in {id_type} field")
            continue
        cypher = f"""
        OPTIONAL MATCH (pub:{ACTIVITY_LABEL}) WHERE pub.iatiidentifier = $id
        OPTIONAL MATCH (phan:{PHANTOM_ACTIVITY_LABEL}) WHERE phan.phantom_activity_identifier = $id
        RETURN pub IS NOT NULL OR phan IS NOT NULL AS exists
        """
        try:
            with neo4j_driver.session() as session:
                result = session.execute_read(lambda tx: tx.run(cypher, id=id_val).single())
                exists = result["exists"] if result else False
                if not exists:
                    print(f"  Warning: {id_type} ACTIVITY ID '{id_val}' not found in Neo4j")
        except Exception as e:
            print(f"  Error checking existence of ID '{id_val}': {e}")
    print("--- End of Node Existence Check ---\n")

def get_pg_count(pg_conn, schema, table):
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
    cypher = f"MATCH ()-[r:{edge_type}]->() RETURN count(r) AS count"
    try:
        with neo4j_driver.session() as session:
            result = session.execute_read(lambda tx: tx.run(cypher).single())
            return result["count"] if result else 0
    except Exception as e:
        print(f"Error getting Neo4j count for {edge_type}: {e}", file=sys.stderr)
        return 0

def load_activity_participation_edges(pg_conn, neo4j_driver, batch_size):
    """Loads activity-to-activity participation edges from PostgreSQL to Neo4j."""
    print(f"--- Loading Edges: {DBT_TARGET_SCHEMA}.{SOURCE_TABLE} -> :{NEO4J_EDGE_TYPE} ---")
    detail_log_filename = SKIPPED_DETAILS_LOG_FILENAME
    summary_log_filename = SUMMARY_LOG_FILENAME
    check_node_existence(neo4j_driver, pg_conn)
    skipped_null_id_count = 0
    skipped_missing_node_count = 0
    successful_merge_operations = 0
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
                f.write(f"{SOURCE_NODE_ID_COL}\t{TARGET_NODE_ID_COL}\treason\n")
            print(f"Skip details log initialized at {os.path.abspath(detail_log_filename)}")
        except IOError as e:
            print(f"Error writing summary/detail log file: {e}", file=sys.stderr)
        return True, 0, 0, 0
    count_before = get_neo4j_edge_count(neo4j_driver, NEO4J_EDGE_TYPE)
    pg_cursor = pg_conn.cursor(name='fetch_activity_participation_links', cursor_factory=psycopg2.extras.DictCursor)
    pg_cursor.itersize = batch_size
    select_cols_str = ", ".join([f'"{c}"' for c in SOURCE_COLUMNS])
    select_query = f'SELECT {select_cols_str} FROM "{DBT_TARGET_SCHEMA}"."{SOURCE_TABLE}";'
    set_clauses = []
    for col in EDGE_PROPERTY_COLUMNS:
        set_clauses.append(f"r.{col} = row.{col}")
    set_clause_str = ", ".join(set_clauses)
    cypher_query = f"""
    UNWIND $batch as row
    // Match source node (published or phantom)
    OPTIONAL MATCH (pubS:{ACTIVITY_LABEL}) WHERE pubS.iatiidentifier = row.{SOURCE_NODE_ID_COL}
    OPTIONAL MATCH (phanS:{PHANTOM_ACTIVITY_LABEL}) WHERE pubS IS NULL AND phanS.phantom_activity_identifier = row.{SOURCE_NODE_ID_COL}
    WITH row, COALESCE(pubS, phanS) as sourceNode
    // Match target node (published or phantom)
    OPTIONAL MATCH (pubT:{ACTIVITY_LABEL}) WHERE pubT.iatiidentifier = row.{TARGET_NODE_ID_COL}
    OPTIONAL MATCH (phanT:{PHANTOM_ACTIVITY_LABEL}) WHERE pubT IS NULL AND phanT.phantom_activity_identifier = row.{TARGET_NODE_ID_COL}
    WITH row, sourceNode, COALESCE(pubT, phanT) as targetNode
    FOREACH (
        _ IN CASE WHEN sourceNode IS NOT NULL AND targetNode IS NOT NULL THEN [1] ELSE [] END |
        MERGE (sourceNode)-[r:{NEO4J_EDGE_TYPE}]->(targetNode)
        ON CREATE SET {set_clause_str}
        ON MATCH SET {set_clause_str}
    )
    WITH row, sourceNode, targetNode
    WHERE sourceNode IS NULL OR targetNode IS NULL
    RETURN 
        row.{SOURCE_NODE_ID_COL} as source_id, 
        row.{TARGET_NODE_ID_COL} as target_id,
        sourceNode IS NULL as source_missing, 
        targetNode IS NULL as target_missing
    """
    print(f"Executing SELECT query: {select_query}")
    try:
        pg_cursor.execute(select_query)
    except psycopg2.Error as e:
        print(f"Error executing SELECT query: {e}", file=sys.stderr)
        pg_cursor.close()
        return False, skipped_null_id_count, skipped_missing_node_count, successful_merge_operations
    skipped_missing_node_count = 0
    print(f"Starting batch load (batch size: {batch_size})...")
    print(f"Skipped edge details will be logged to: {os.path.abspath(detail_log_filename)}")
    try:
        with open(detail_log_filename, 'a') as detail_log_file:
            if detail_log_file.tell() == 0:
                detail_log_file.write(f"{SOURCE_NODE_ID_COL}\t{TARGET_NODE_ID_COL}\treason\n")
            with tqdm(total=expected_count, desc=f"Edges :{NEO4J_EDGE_TYPE}", unit=" edges") as pbar:
                while True:
                    try:
                        batch_data = pg_cursor.fetchmany(batch_size)
                    except psycopg2.Error as e:
                        print(f"\nError fetching batch from PostgreSQL: {e}", file=sys.stderr)
                        break
                    if not batch_data:
                        break
                    batch_list = []
                    rows_in_batch_attempt = 0
                    batch_initial_count = len(batch_data)
                    for row_dict in [dict(row) for row in batch_data]:
                        rows_in_batch_attempt += 1
                        if row_dict.get(SOURCE_NODE_ID_COL) is None or row_dict.get(TARGET_NODE_ID_COL) is None:
                            skipped_null_id_count += 1
                            source_id = row_dict.get(SOURCE_NODE_ID_COL, 'NULL')
                            target_id = row_dict.get(TARGET_NODE_ID_COL, 'NULL')
                            detail_log_file.write(f"{source_id}\t{target_id}\tNULL_ID\n")
                            continue
                        sanitised_item = {}
                        for col in SOURCE_COLUMNS:
                            value = row_dict.get(col)
                            if isinstance(value, Decimal):
                                value = float(value)
                            sanitised_item[col] = value
                        batch_list.append(sanitised_item)
                    pbar.update(batch_initial_count)
                    if not batch_list:
                        detail_log_file.flush()
                        continue
                    try:
                        with neo4j_driver.session(database="neo4j") as session:
                            results = session.execute_write(
                                lambda tx: tx.run(cypher_query, batch=batch_list).data()
                            )
                            skipped_in_batch_neo4j = len(results)
                            merges_in_batch = len(batch_list) - skipped_in_batch_neo4j
                            successful_merge_operations += merges_in_batch
                            skipped_missing_node_count += skipped_in_batch_neo4j
                            for skipped_record in results:
                                source_id = skipped_record.get('source_id', 'ERROR')
                                target_id = skipped_record.get('target_id', 'ERROR')
                                source_missing = skipped_record.get('source_missing', True)
                                target_missing = skipped_record.get('target_missing', True)
                                reason = "UNKNOWN"
                                if source_missing and target_missing:
                                    reason = "BOTH_MISSING"
                                elif source_missing:
                                    reason = "SOURCE_MISSING"
                                elif target_missing:
                                    reason = "TARGET_MISSING"
                                detail_log_file.write(f"{source_id}\t{target_id}\t{reason}\n")
                            detail_log_file.flush()
                    except Exception as e:
                        print(f"\nError processing batch in Neo4j: {e}", file=sys.stderr)
                        pg_cursor.close()
                        return False, skipped_null_id_count, skipped_missing_node_count, successful_merge_operations
    except IOError as e:
        print(f"\nError opening or writing to detail log file {detail_log_filename}: {e}", file=sys.stderr)
        pg_cursor.close()
        return False, skipped_null_id_count, skipped_missing_node_count, successful_merge_operations
    pg_cursor.close()
    count_after = get_neo4j_edge_count(neo4j_driver, NEO4J_EDGE_TYPE)
    print(f"\n--- Skipped Edges Summary ---")
    print(f"Total edges skipped due to NULL IDs:       {skipped_null_id_count}")
    print(f"Total edges skipped due to missing nodes (Neo4j): {skipped_missing_node_count}")
    total_skipped = skipped_null_id_count + skipped_missing_node_count
    print(f"Total skipped edges overall:             {total_skipped}")
    try:
        with open(summary_log_filename, 'w') as f:
            f.write(f"Skipped edge summary for {NEO4J_EDGE_TYPE}\n")
            f.write(f"Source: {DBT_TARGET_SCHEMA}.{SOURCE_TABLE}\n")
            f.write(f"Total expected edges (from PG): {expected_count}\n")
            f.write(f"Skipped due to NULL IDs: {skipped_null_id_count}\n")
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
        if successful_merge_operations != net_expected_merges:
            print(f"Warning: The number of successful MERGE operations ({successful_merge_operations}) does not match the net expected count ({net_expected_merges}). Check batch processing logic.", file=sys.stderr)
        elif new_edges_created == 0 and successful_merge_operations > 0:
            print(f"Note: {successful_merge_operations} MERGE operations were successful, but no new edges were created. All relationships likely existed already and were updated (ON MATCH).")
        elif new_edges_created < successful_merge_operations:
            print(f"Note: {successful_merge_operations} MERGE operations were successful, creating {new_edges_created} new edges. Some existing relationships were matched and updated.")
    else:
        print("Could not verify final counts after loading.", file=sys.stderr)
    return True, skipped_null_id_count, skipped_missing_node_count, successful_merge_operations

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
        print("--- Starting Activity Participation Edge Load ---")
        neo4j_driver = get_neo4j_driver()
        pg_conn = get_postgres_connection()
        success, final_null_skips, final_missing_node_skips, final_successful_merges = load_activity_participation_edges(pg_conn, neo4j_driver, batch_size)
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
            print("\nActivity participation edge loading process finished successfully.")
            print(f"Summary: Skipped (Null IDs): {final_null_skips}, Skipped (Missing Nodes): {final_missing_node_skips}, Successful Merges: {final_successful_merges}")
        else:
            print("\nActivity participation edge loading process finished with errors or was interrupted.", file=sys.stderr)
            sys.exit(1)

if __name__ == "__main__":
    main() 