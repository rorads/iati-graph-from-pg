# graph/load_graph.py

import argparse
import os
import sys
import time  # Import time module for sleep

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from neo4j import GraphDatabase
from tqdm import tqdm

load_dotenv()

# --- Configuration ---

# Neo4j connection details (defaults match docker-compose.yml)
# Use bolt:// for direct connection to single instance, avoids routing issues
NEO4J_URI = os.getenv(
    "NEO4J_URI", "bolt://localhost:7687"
)  # Changed localhost to service name 'neo4j' and scheme to bolt://
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "dev_password")

# PostgreSQL connection details (defaults match docker-compose.yml)
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://iatitables:dev_password@localhost:5433/iatitables"
)
# Explicitly set the target schema for dbt models
DBT_TARGET_SCHEMA = "iati_graph"

# Processing Batch Size
DEFAULT_BATCH_SIZE = 1000

# --- Node and Edge Definitions ---
# These map dbt models (PostgreSQL tables/views) to Neo4j graph elements.

NODE_CONFIGS = [
    {
        "dbt_model": "published_activities",
        "label": "Activity", 
        "id_property": "iatiidentifier",
        "columns": [
            "iatiidentifier",
            "title_narrative",
            "reportingorg_ref",
            "reportingorg_narrative",
            "reportingorg_type",
            "activitystatus_code",
            "plannedstart",
            "plannedend",
            "actualstart",
            "actualend",
            "lastupdateddatetime",
            "hierarchy",
            "dportal-link",
        ],
    },
    {
        "dbt_model": "published_organisations",
        "label": "Organisation",
        "id_property": "organisationidentifier",
        "columns": [
            "organisationidentifier",
            "name_narrative",
            "hierarchy",
            "reportingorg_ref",
        ],
    },
    {
        # Merge phantom orgs onto the main Organisation label, using 'reference' as ID
        "dbt_model": "phantom_organisations",
        "label": "Organisation",
        "id_property": "reference",
        "columns": [
            "reference",
            "distinct_narratives",
            "phantom_in_participatingorg",
            "phantom_in_transaction_provider",
            "phantom_in_transaction_receiver",
            "phantom_in_orgbudget_recipient",
        ],
    },
    {
        # Merge phantom activities onto the main Activity label, using 'phantom_activity_identifier' as ID
        "dbt_model": "phantom_activities",
        "label": "Activity",
        "id_property": "phantom_activity_identifier",
        "columns": [
            "phantom_activity_identifier",
            "source_column",
            "source_activity_id",
        ],
    },
]

EDGE_CONFIGS = [
    {
        "dbt_model": "participation_links",
        "type": "PARTICIPATES_IN",
        "source_node": {
            "label": "Organisation",
            "id_property": "organisationidentifier",
            "dbt_column": "organisation_id",
        },
        "target_node": {
            "label": "Activity",
            "id_property": "iatiidentifier",
            "dbt_column": "activity_id",
        },
        "columns": ["role_code", "role_name"],  # Properties of the relationship
    },
    {
        "dbt_model": "financial_links",
        "type": "FINANCIAL_FLOW",
        # Source/Target are dynamic based on *_node_type columns in the data
        "source_node": {
            "dbt_column": "source_node_id",
            "type_column": "source_node_type",
        },
        "target_node": {
            "dbt_column": "target_node_id",
            "type_column": "target_node_type",
        },
        "columns": [
            "transactiontype_code",
            "transaction_type_name",
            "currency",
            "total_value_usd",
        ],
    },
]

# --- Database Connection Functions ---


def get_neo4j_driver():
    """Establishes connection to Neo4j."""
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        # Add a short delay after initial connection attempt
        print("Waiting 2 seconds for Neo4j to stabilize...")
        time.sleep(2)  # Wait 2 seconds (changed from 30)
        driver.verify_connectivity()  # Verify connection again after delay
        print(f"Successfully connected to Neo4j at {NEO4J_URI}.")
        return driver
    except Exception as e:
        print(f"Error connecting to Neo4j at {NEO4J_URI}: {e}", file=sys.stderr)
        # Add specific check for common routing error
        if "Unable to retrieve routing information" in str(e):
            print(
                "Hint: Ensure Neo4j is running and accessible. For single instances, "
                "try using the 'bolt://' URI scheme.",
                file=sys.stderr,
            )
        sys.exit(1)


def get_postgres_connection():
    """Establishes connection to PostgreSQL."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        print("Successfully connected to PostgreSQL.")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}", file=sys.stderr)
        sys.exit(1)


def test_neo4j_write(driver):
    """Attempts to write a single test node to Neo4j using CREATE."""
    print("\n--- Testing Neo4j Write Operation (using CREATE) --- ")
    test_node_name = f"TestCreate_{int(time.time())}" # Use timestamp for uniqueness
    try:
        with driver.session(database="neo4j") as session: # Explicitly use default db
            session.execute_write(lambda tx: tx.run(
                "CREATE (t:TestNode {name: $name, timestamp: timestamp()})",
                name=test_node_name
            ))
        print(
            f"Successfully created test node (:TestNode {{name: '{test_node_name}'}})"
        )
        return True
    except Exception as e:
        print(f"Error creating test node: {e}", file=sys.stderr)
        return False


# --- Data Loading Functions ---


def create_constraints(driver):
    """Creates uniqueness constraints in Neo4j for faster MERGE operations."""
    print("Applying Neo4j constraints...")
    constraints = []
    for config in NODE_CONFIGS:
        constraints.append(
            f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{config['label']})"
            f" REQUIRE n.{config['id_property']} IS UNIQUE"
        )

    # Ensure constraints are created for both Activity ID types if merging onto same label
    has_published_activity_id = any(
        c["label"] == "Activity" and c["id_property"] == "iatiidentifier"
        for c in NODE_CONFIGS
    )
    has_phantom_activity_id = any(
        c["label"] == "Activity" and c["id_property"] == "phantom_activity_identifier"
        for c in NODE_CONFIGS
    )
    if has_published_activity_id and has_phantom_activity_id:
        constraints.append(
            "CREATE CONSTRAINT IF NOT EXISTS FOR"
            " (n:Activity) REQUIRE n.phantom_activity_identifier IS UNIQUE"
        )

    # Ensure constraints are created for both Organisation ID types if merging onto same label
    has_published_org_id = any(
        c["label"] == "Organisation" and c["id_property"] == "organisationidentifier"
        for c in NODE_CONFIGS
    )
    has_phantom_org_id = any(
        c["label"] == "Organisation" and c["id_property"] == "reference"
        for c in NODE_CONFIGS
    )
    if has_published_org_id and has_phantom_org_id:
        constraints.append(
            "CREATE CONSTRAINT IF NOT EXISTS FOR"
            " (n:Organisation) REQUIRE n.reference IS UNIQUE"
        )

    with driver.session() as session:
        for constraint_cypher in tqdm(constraints, desc="Constraints"):
            try:
                session.run(constraint_cypher)
            except Exception as e:
                # Ignore errors if constraint creation fails (e.g., enterprise features not available)
                # but log them. Neo4j Community supports unique node property constraints.
                print(
                    f"Warning: Could not apply constraint '{constraint_cypher}'."
                    f" Reason: {e}",
                    file=sys.stderr,
                )
    print("Constraints applied.")


def load_nodes(pg_conn, neo4j_driver, config, batch_size):
    """Loads nodes from a PostgreSQL table/view into Neo4j."""
    model = config["dbt_model"]
    label = config["label"]
    id_prop = config["id_property"]
    cols = config["columns"]
    print(f"Loading nodes: {model} -> :{label}({{{id_prop}}})...")

    pg_cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    neo4j_session = neo4j_driver.session()

    try:
        # Count rows for progress bar
        count_query = f'SELECT COUNT(*) FROM {DBT_TARGET_SCHEMA}."{model}";'
        pg_cursor.execute(count_query)
        total_rows = pg_cursor.fetchone()[0]

        if total_rows == 0:
            print(f"Skipping {model} - no rows found.")
            return

        # Prepare SELECT query
        select_cols = ", ".join([f'"{c}"' for c in cols])
        select_query = f'SELECT {select_cols} FROM {DBT_TARGET_SCHEMA}."{model}";'
        pg_cursor.execute(select_query)

        # Prepare Cypher query parts
        cypher_props = []
        for col in cols:
            # Avoid overwriting the ID property during SET
            if col != id_prop:
                # Handle potential naming conflicts or invalid characters if necessary
                prop_name = col.replace("-", "_")  # Basic sanitisation
                cypher_props.append(f"n.{prop_name} = row.{prop_name}")

        set_clause = f"SET {', '.join(cypher_props)}" if cypher_props else ""

        # Construct parameterised Cypher query
        cypher_query = f"""
        UNWIND $batch as row
        MERGE (n:{label} {{{id_prop.replace('-', '_')}: row.{id_prop.replace('-', '_')}}})
        {set_clause}
        """

        with tqdm(total=total_rows, desc=f"Nodes {label}", unit=" nodes") as pbar:
            while True:
                batch_data = pg_cursor.fetchmany(batch_size)
                if not batch_data:
                    break

                # Convert Row objects to dictionaries and sanitise keys for Neo4j
                batch_list = []
                for row in batch_data:
                    sanitised_row = {k.replace('-', '_'): v for k, v in dict(row).items()}
                    batch_list.append(sanitised_row)

                try:
                    neo4j_session.execute_write(
                        lambda tx: tx.run(cypher_query, batch=batch_list)
                    )
                    pbar.update(len(batch_list))
                except Exception as e:
                    print(f"\nError processing batch for {model}: {e}", file=sys.stderr)
                    print(f"Cypher: {cypher_query}", file=sys.stderr)
                    # Consider adding logging for failed batch data: print(batch_list)
                    # Depending on the error, you might want to stop or continue

    finally:
        pg_cursor.close()
        neo4j_session.close()


def load_edges(pg_conn, neo4j_driver, config, batch_size):
    """Loads edges (relationships) from PostgreSQL into Neo4j."""
    model = config["dbt_model"]
    rel_type = config["type"]
    print(f"Loading edges: {model} -> -[:{rel_type}]->...")

    pg_cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    neo4j_session = neo4j_driver.session()

    # Determine columns to select from PG
    all_pg_cols = set()
    property_cols = config["columns"]
    source_info = config["source_node"]
    target_info = config["target_node"]

    all_pg_cols.add(source_info["dbt_column"])
    if "type_column" in source_info:
        all_pg_cols.add(source_info["type_column"])

    all_pg_cols.add(target_info["dbt_column"])
    if "type_column" in target_info:
        all_pg_cols.add(target_info["type_column"])

    all_pg_cols.update(property_cols)
    select_cols_str = ", ".join([f'"{c}"' for c in sorted(list(all_pg_cols))])

    try:
        count_query = f'SELECT COUNT(*) FROM {DBT_TARGET_SCHEMA}."{model}";'
        pg_cursor.execute(count_query)
        total_rows = pg_cursor.fetchone()[0]

        if total_rows == 0:
            print(f"Skipping {model} - no rows found.")
            return

        select_query = f'SELECT {select_cols_str} FROM {DBT_TARGET_SCHEMA}."{model}";'
        pg_cursor.execute(select_query)

        # --- Cypher Query Construction ---
        # Base parts
        cypher_unwind = "UNWIND $batch as row"
        cypher_merge_rel = f"MERGE (source)-[r:{rel_type}]->(target)"

        # Relationship properties
        cypher_props = []
        for col in property_cols:
            prop_name = col.replace("-", "_")  # Basic sanitisation
            cypher_props.append(f"r.{prop_name} = row.{prop_name}")
        cypher_set = f"SET {', '.join(cypher_props)}" if cypher_props else ""

        # Source/Target Matching - Handles fixed and dynamic types
        source_match_parts = []
        target_match_parts = []

        # Helper function to create WHERE clause for multi-ID properties
        def get_multi_id_where_clause(label, id_col):
            if label == "Activity":
                return f"(n.iatiidentifier = row.{id_col} OR n.phantom_activity_identifier = row.{id_col})"
            elif label == "Organisation":
                return f"(n.organisationidentifier = row.{id_col} OR n.reference = row.{id_col})"
            else:
                # Fallback for labels with single expected ID property (needs config update if not)
                # This assumes the id_property in config is the correct one for single-ID labels
                print(f"Warning: Assuming single ID property for label {label}", file=sys.stderr)
                # Find the relevant id_property from NODE_CONFIGS (less robust)
                single_id_prop = next((c['id_property'] for c in NODE_CONFIGS if c['label'] == label), None)
                if single_id_prop:
                    return f"n.{single_id_prop} = row.{id_col}"
                else:
                    # If no specific ID property found, raise error or use a default?
                    raise ValueError(f"Cannot determine ID property for fixed label {label}")


        if "label" in source_info:  # Fixed source node type
            src_label = source_info["label"]
            src_dbt_col = source_info["dbt_column"]
            where_clause = get_multi_id_where_clause(src_label, src_dbt_col).replace("n.", "source.")
            source_match_parts.append(f"MATCH (source:{src_label}) WHERE {where_clause}")
            # Need to carry 'row' and 'source' to target matching
            target_match_parts.append(f"WITH row, source")

        else:  # Dynamic source node type
            src_id_col = source_info["dbt_column"]
            src_type_col = source_info["type_column"]
            source_match_parts.append(f"""
            OPTIONAL MATCH (source_act:Activity) WHERE row.{src_type_col} = 'ACTIVITY' AND {get_multi_id_where_clause('Activity', src_id_col).replace('n.', 'source_act.')}
            OPTIONAL MATCH (source_org:Organisation) WHERE row.{src_type_col} = 'ORGANISATION' AND {get_multi_id_where_clause('Organisation', src_id_col).replace('n.', 'source_org.')}
            WITH row, coalesce(source_act, source_org) as source
            WHERE source IS NOT NULL""")
            # Need to carry 'row' and 'source' to target matching
            target_match_parts.append(f"WITH row, source") # source defined in the coalesce part


        if "label" in target_info:  # Fixed target node type
            tgt_label = target_info["label"]
            tgt_dbt_col = target_info["dbt_column"]
            where_clause = get_multi_id_where_clause(tgt_label, tgt_dbt_col).replace("n.", "target.")
            # Append to existing target_match_parts (which already includes WITH row, source)
            target_match_parts.append(f"MATCH (target:{tgt_label}) WHERE {where_clause}")
            target_match_parts.append(f"WITH row, source, target") # Ensure target is carried forward

        else:  # Dynamic target node type
            tgt_id_col = target_info["dbt_column"]
            tgt_type_col = target_info["type_column"]
            # Append to existing target_match_parts (which already includes WITH row, source)
            target_match_parts.append(f"""
            OPTIONAL MATCH (target_act:Activity) WHERE row.{tgt_type_col} = 'ACTIVITY' AND {get_multi_id_where_clause('Activity', tgt_id_col).replace('n.', 'target_act.')}
            OPTIONAL MATCH (target_org:Organisation) WHERE row.{tgt_type_col} = 'ORGANISATION' AND {get_multi_id_where_clause('Organisation', tgt_id_col).replace('n.', 'target_org.')}
            WITH row, source, coalesce(target_act, target_org) as target
            WHERE target IS NOT NULL""")
            # target is defined in the coalesce part


        # Combine parts into the final Cypher query
        # Ensure the WITH clauses correctly chain the variables
        cypher_query = f"""
        {cypher_unwind}
        // No need to sanitise keys in Cypher anymore
        // WITH [k IN keys(row) | {{key: replace(k, '-', '_'), value: row[k]}}] as kv_list
        // WITH apoc.map.fromPairs(kv_list) as row 
        {' '.join(source_match_parts)}
        {' '.join(target_match_parts)}
        // Final check before merge (optional, WHERE clauses in MATCH should handle this)
        // WHERE source IS NOT NULL AND target IS NOT NULL
        {cypher_merge_rel}
        {cypher_set}
        """

        # Execute in batches
        with tqdm(total=total_rows, desc=f"Edges {rel_type}", unit=" edges") as pbar:
            while True:
                batch_data = pg_cursor.fetchmany(batch_size)
                if not batch_data:
                    break

                # Convert Row objects to dictionaries and sanitise keys
                batch_list = []
                for row in batch_data:
                    sanitised_row = {k.replace('-', '_'): v for k, v in dict(row).items()}
                    batch_list.append(sanitised_row)

                # Clean data before sending (e.g., ensure required fields exist)
                # Use sanitised keys for checks
                valid_batch = []
                sanitised_src_col = source_info["dbt_column"].replace('-', '_')
                sanitised_tgt_col = target_info["dbt_column"].replace('-', '_')
                sanitised_src_type_col = source_info.get("type_column", "").replace('-', '_')
                sanitised_tgt_type_col = target_info.get("type_column", "").replace('-', '_')

                for item in batch_list:
                    required_keys = [sanitised_src_col, sanitised_tgt_col]
                    if sanitised_src_type_col:
                        required_keys.append(sanitised_src_type_col)
                    if sanitised_tgt_type_col:
                        required_keys.append(sanitised_tgt_type_col)

                    if all(k in item and item[k] is not None for k in required_keys):
                        valid_batch.append(item)

                if not valid_batch:
                    pbar.update(len(batch_data))  # Update progress even if skipping
                    continue

                try:
                    # Use execute_write for transactional safety
                    neo4j_session.execute_write(
                        lambda tx: tx.run(cypher_query, batch=valid_batch)
                    )
                    pbar.update(
                        len(batch_data)
                    )  # Assume success for progress, adjust if skipping errors
                except Exception as e:
                    print(
                        f"\nError processing edge batch for {model}: {e}",
                        file=sys.stderr,
                    )
                    print(f"Cypher: {cypher_query}", file=sys.stderr)
                    # print(f"Failed Batch Sample: {valid_batch[:2]}", file=sys.stderr)
                    # Consider stopping or continuing based on error type

    finally:
        pg_cursor.close()
        neo4j_session.close()


# --- Main Execution ---


def main():
    parser = argparse.ArgumentParser(
        description="Load IATI data from PostgreSQL (dbt models) to Neo4j."
    )
    parser.add_argument("--nodes", action="store_true", help="Load only nodes.")
    parser.add_argument("--edges", action="store_true", help="Load only edges.")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Number of records to process per batch.",
    )
    parser.add_argument(
        "--skip-constraints",
        action="store_true",
        help="Skip creating Neo4j constraints.",
    )

    args = parser.parse_args()

    # Determine what to load
    load_nodes_flag = True
    load_edges_flag = True
    if args.nodes and not args.edges:
        load_edges_flag = False
    elif args.edges and not args.nodes:
        load_nodes_flag = False
    # If neither or both are specified, load both

    batch_size = args.batch_size

    # Get database connections
    neo4j_driver = None
    pg_conn = None
    try:
        neo4j_driver = get_neo4j_driver()

        pg_conn = get_postgres_connection()

        # Create constraints (unless skipped)
        if not args.skip_constraints:
            create_constraints(neo4j_driver)

        # Load Nodes
        if load_nodes_flag:
            print("\n--- Loading Nodes ---")
            for config in NODE_CONFIGS:
                load_nodes(pg_conn, neo4j_driver, config, batch_size)
            print("Node loading complete.")

        # Load Edges
        if load_edges_flag:
            print("\n--- Loading Edges ---")
            for config in EDGE_CONFIGS:
                load_edges(pg_conn, neo4j_driver, config, batch_size)
            print("Edge loading complete.")

        print("\nGraph loading process finished.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if neo4j_driver:
            neo4j_driver.close()
            print("Neo4j connection closed.")
        if pg_conn:
            pg_conn.close()
            print("PostgreSQL connection closed.")


if __name__ == "__main__":
    main()
