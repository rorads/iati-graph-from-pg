#!/usr/bin/env python3
"""
graph/load_hierarchy_edges.py

Loads hierarchy edges (parent-child and sibling) between activities into Neo4j.
"""
import os
import sys
import time
from tqdm import tqdm

import psycopg2
import psycopg2.extras

# Import shared database functions and configuration
from db_utils import get_neo4j_driver, get_postgres_connection

# --- Configuration ---
DBT_TARGET_SCHEMA = "iati_graph"
SOURCE_TABLE = "hierarchy_links"
NEO4J_EDGE_TYPE = "HIERARCHY"

# Node labels for source and target
ACTIVITY_LABEL = "PublishedActivity"
PHANTOM_ACTIVITY_LABEL = "PhantomActivity"

# Column names from DBT model
SOURCE_NODE_ID_COL = "source_node_id"
TARGET_NODE_ID_COL = "target_node_id"
REL_TYPE_COL = "relationship_type"
DECLARED_BY_COL = "declared_by"

# Columns to load from PostgreSQL
SOURCE_COLUMNS = [
    SOURCE_NODE_ID_COL,
    TARGET_NODE_ID_COL,
    REL_TYPE_COL,
    DECLARED_BY_COL
]

# Edge property columns (these become properties on the relationship)
EDGE_PROPERTY_COLUMNS = [
    REL_TYPE_COL,
    DECLARED_BY_COL
]

# Processing Batch Size
DEFAULT_BATCH_SIZE = 1000

# Log files
DETAILS_LOG = "hierarchy_edges_skipped_details.log"
SUMMARY_LOG = "hierarchy_edges_skipped_summary.log"

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
            return None

def get_neo4j_edge_count(neo4j_driver, edge_type):
    """Gets the count of edges with a specific type in Neo4j."""
    cypher = f"MATCH ()-[r:{edge_type}]->() RETURN count(r) AS count"
    try:
        with neo4j_driver.session() as session:
            result = session.run(cypher).single()
            count = result["count"] if result else 0
            print(f"Current edge count for :{edge_type} in Neo4j: {count}")
            return count
    except Exception as e:
        print(f"Error getting Neo4j edge count for :{edge_type}: {e}", file=sys.stderr)
        return None

def load_hierarchy_edges(pg_conn, neo4j_driver, batch_size):
    """Loads hierarchy edges from PostgreSQL to Neo4j."""
    print(f"--- Loading Edges: {DBT_TARGET_SCHEMA}.{SOURCE_TABLE} -> :{NEO4J_EDGE_TYPE} ---")

    # 1. Get expected count
    expected = get_pg_count(pg_conn, DBT_TARGET_SCHEMA, SOURCE_TABLE)
    if expected is None:
        print("Failed to get expected row count. Exiting.")
        return
    if expected == 0:
        print(f"No rows in {DBT_TARGET_SCHEMA}.{SOURCE_TABLE}, skipping load.")
        return

    # 2. Get existing edge count
    before = get_neo4j_edge_count(neo4j_driver, NEO4J_EDGE_TYPE)

    # 3. Fetch and load in batches
    query = f"SELECT {', '.join(SOURCE_COLUMNS)} FROM \"{DBT_TARGET_SCHEMA}\".\"{SOURCE_TABLE}\""
    cursor = pg_conn.cursor(name="hierarchy_cursor", cursor_factory=psycopg2.extras.DictCursor)
    cursor.itersize = batch_size
    cursor.execute(query)

    cypher = f"""
    MERGE (src:{ACTIVITY_LABEL} {{ iatiidentifier: $src }})
    MERGE (tgt:{ACTIVITY_LABEL} {{ iatiidentifier: $tgt }})
    MERGE (src)-[rel:{NEO4J_EDGE_TYPE}]->(tgt)
    SET rel.{REL_TYPE_COL} = $rtype,
        rel.{DECLARED_BY_COL} = $declared
    """

    merged = 0
    for row in tqdm(cursor, total=expected, desc="Loading hierarchy edges"):
        src = row[SOURCE_NODE_ID_COL]
        tgt = row[TARGET_NODE_ID_COL]
        rtype = row[REL_TYPE_COL]
        declared = row[DECLARED_BY_COL]
        # Skip invalid
        if not src or not tgt:
            continue
        try:
            with neo4j_driver.session() as session:
                session.run(cypher, src=src, tgt=tgt, rtype=rtype, declared=declared)
            merged += 1
        except Exception as e:
            print(f"Error creating edge {src}->{tgt}: {e}", file=sys.stderr)
            continue

    after = get_neo4j_edge_count(neo4j_driver, NEO4J_EDGE_TYPE)
    print(f"--- Finished loading hierarchy edges. Merged: {merged} (before: {before}, after: {after}) ---")

def main():
    pg_conn = get_postgres_connection()
    neo4j_driver = get_neo4j_driver()
    load_hierarchy_edges(pg_conn, neo4j_driver, DEFAULT_BATCH_SIZE)

if __name__ == '__main__':
    main()