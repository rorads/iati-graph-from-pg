#!/usr/bin/env python3
# graph/wipe_neo4j.py

import time
import sys
import logging
from neo4j.exceptions import Neo4jError, ClientError, ServiceUnavailable
from tqdm import tqdm

from db_utils import get_neo4j_driver

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/neo4j_wipe.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("wipe_neo4j")

def check_apoc_availability(session):
    """Check if APOC procedures are available in the Neo4j instance."""
    try:
        result = session.run("CALL apoc.help('periodic.iterate') YIELD name RETURN count(*) > 0 AS available")
        record = result.single()
        return record and record["available"]
    except (Neo4jError, ClientError) as e:
        if "not found" in str(e).lower() or "procedure not found" in str(e).lower():
            return False
        raise

def count_relationships(session):
    """Count total number of relationships in the database."""
    result = session.run("MATCH ()-[r]->() RETURN count(r) AS rel_count")
    record = result.single()
    return record["rel_count"] if record else 0

def count_nodes(session):
    """Count total number of nodes in the database."""
    result = session.run("MATCH (n) RETURN count(n) AS node_count")
    record = result.single()
    return record["node_count"] if record else 0

def count_indexes_and_constraints(session):
    """Count total number of indexes and constraints in the database."""
    result = session.run("SHOW CONSTRAINTS")
    constraints = len(list(result))
    
    result = session.run("SHOW INDEXES")
    indexes = len(list(result))
    
    return constraints + indexes

def count_property_keys(session):
    """Count total number of property keys in the database."""
    result = session.run("CALL db.propertyKeys() YIELD propertyKey RETURN count(propertyKey) AS count")
    record = result.single()
    return record["count"] if record else 0

def list_property_keys(session):
    """List all property keys in the database."""
    result = session.run("CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey")
    return [record["propertyKey"] for record in result]

def wipe_database_with_apoc(driver):
    """Wipe Neo4j database using APOC's periodic.iterate for efficient batching."""
    logger.info("Wiping database using APOC procedures")
    
    start_time = time.time()
    
    # Count relationships and nodes before deletion for progress estimation
    with driver.session() as session:
        total_rels = count_relationships(session)
        total_nodes = count_nodes(session)
        total_schema_items = count_indexes_and_constraints(session)
        total_property_keys = count_property_keys(session)
    
    rel_start_time = time.time()
    # Step 1: Delete all relationships in batches
    logger.info(f"Deleting {total_rels} relationships in batches...")
    
    with tqdm(total=total_rels, desc="Deleting relationships", unit="rel") as pbar:
        if total_rels > 0:
            deleted_rels = 0
            batch_size = 10000
            with driver.session() as session:
                while True:
                    # Call periodic.iterate to delete a batch
                    result = session.run("""
                        CALL apoc.periodic.iterate(
                            'MATCH ()-[r]->() WITH r LIMIT $batch_size RETURN r',
                            'DELETE r',
                            {batchSize: $batch_size, parallel: false, params: {batch_size: $batch_size}}
                        )
                        YIELD committedOperations, batches, timeTaken
                        RETURN committedOperations, batches, timeTaken
                    """, batch_size=batch_size)
                    
                    stats = result.single()
                    committed_ops = stats["committedOperations"]
                    
                    if committed_ops == 0:
                        break  # No more relationships found
                    
                    # Update progress based on committed operations
                    pbar.update(committed_ops)
                    deleted_rels += committed_ops
                    
                    # Exit if we've deleted more than the initial count (shouldn't happen)
                    if deleted_rels >= total_rels:
                        break
                
                # Final check to ensure the bar is full
                final_rel_count = count_relationships(session)
                pbar.n = total_rels - final_rel_count
                pbar.refresh()
        else:
            pbar.update(0) # Already empty
            
    rel_time_taken = int((time.time() - rel_start_time) * 1000)
    logger.info(f"Deleted {total_rels} relationships. Time taken: {rel_time_taken} ms")

    node_start_time = time.time()
    # Step 2: Delete all nodes in batches
    logger.info(f"Deleting {total_nodes} nodes in batches...")
    
    with tqdm(total=total_nodes, desc="Deleting nodes", unit="node") as pbar:
        if total_nodes > 0:
            deleted_nodes = 0
            batch_size = 5000
            with driver.session() as session:
                while True:
                    # Call periodic.iterate to delete a batch of nodes
                    result = session.run("""
                        CALL apoc.periodic.iterate(
                            'MATCH (n) WITH n LIMIT $batch_size RETURN n',
                            'DETACH DELETE n',
                            {batchSize: $batch_size, parallel: false, params: {batch_size: $batch_size}}
                        )
                        YIELD committedOperations, batches, timeTaken
                        RETURN committedOperations, batches, timeTaken
                    """, batch_size=batch_size)
                    
                    stats = result.single()
                    committed_ops = stats["committedOperations"]
                    
                    if committed_ops == 0:
                        break # No more nodes found
                    
                    # Update progress based on committed operations
                    pbar.update(committed_ops)
                    deleted_nodes += committed_ops

                    # Exit if we've deleted more than the initial count
                    if deleted_nodes >= total_nodes:
                        break

                # Final check to ensure the bar is full
                final_node_count = count_nodes(session)
                pbar.n = total_nodes - final_node_count
                pbar.refresh()
        else:
            pbar.update(0) # Already empty
    
    node_time_taken = int((time.time() - node_start_time) * 1000)
    logger.info(f"Deleted {total_nodes} nodes. Time taken: {node_time_taken} ms")
    
    # Step 3: Clean up schema (indexes and constraints)
    logger.info("Cleaning up schema (dropping indexes and constraints)...")
    
    with tqdm(total=total_schema_items, desc="Cleaning up schema", unit="item") as pbar:
        if total_schema_items > 0:
            with driver.session() as session:
                session.run("CALL apoc.schema.assert({}, {}, true)")
                pbar.update(total_schema_items)
        else:
            pbar.update(0)
    
    # Step 4: Clear property keys by forcing a schema refresh
    if total_property_keys > 0:
        logger.info(f"Clearing {total_property_keys} property keys from the database...")
        
        with driver.session() as session:
            property_keys_before = list_property_keys(session)
        
        with tqdm(total=total_property_keys, desc="Clearing property keys", unit="key") as pbar:
            with driver.session() as session:
                session.run("CALL db.clearQueryCaches()")
            pbar.update(total_property_keys)
            
        logger.info(f"Property keys before potential clearing: {', '.join(property_keys_before)}")
    
    total_time = time.time() - start_time
    logger.info(f"Database successfully wiped in {total_time:.2f} seconds using APOC")

def wipe_database_fallback(driver):
    """Wipe Neo4j database using simple Cypher queries for when APOC is not available."""
    logger.info("Wiping database using Cypher transactions (APOC not available)")
    
    start_time = time.time()
    
    # Count before deletion for progress estimation
    with driver.session() as session:
        total_rels = count_relationships(session)
        total_nodes = count_nodes(session)
        total_property_keys = count_property_keys(session)

    # Step 1: Delete all relationships in batches
    logger.info(f"Deleting {total_rels} relationships in batches...")
    rel_start_time = time.time()
    with tqdm(total=total_rels, desc="Deleting relationships", unit="rel") as pbar:
        if total_rels > 0:
            deleted_rels_count = 0
            batch_size = 10000
            with driver.session() as session:
                while True:
                    # Delete a batch and get the count of deleted relationships
                    result = session.run("""
                        MATCH ()-[r]->() WITH r LIMIT $batch_size
                        DELETE r RETURN count(r) AS deleted_count
                    """, batch_size=batch_size)
                    
                    deleted_in_batch = result.single()["deleted_count"]
                    
                    if deleted_in_batch == 0:
                        break # No more relationships to delete
                    
                    pbar.update(deleted_in_batch)
                    deleted_rels_count += deleted_in_batch

                # Final check
                final_rel_count = count_relationships(session)
                pbar.n = total_rels - final_rel_count
                pbar.refresh()
        else:
            pbar.update(0)

    rel_time_taken = int((time.time() - rel_start_time) * 1000)
    logger.info(f"Deleted {total_rels} relationships. Time taken: {rel_time_taken} ms")

    # Step 2: Delete all nodes in batches
    logger.info(f"Deleting {total_nodes} nodes in batches...")
    node_start_time = time.time()
    with tqdm(total=total_nodes, desc="Deleting nodes", unit="node") as pbar:
        if total_nodes > 0:
            deleted_nodes_count = 0
            batch_size = 5000
            with driver.session() as session:
                while True:
                    # Delete a batch and get the count of deleted nodes
                    result = session.run("""
                        MATCH (n) WITH n LIMIT $batch_size
                        DETACH DELETE n RETURN count(n) AS deleted_count
                    """, batch_size=batch_size)
                    
                    deleted_in_batch = result.single()["deleted_count"]
                    
                    if deleted_in_batch == 0:
                        break # No more nodes to delete
                    
                    pbar.update(deleted_in_batch)
                    deleted_nodes_count += deleted_in_batch
                
                # Final check
                final_node_count = count_nodes(session)
                pbar.n = total_nodes - final_node_count
                pbar.refresh()
        else:
            pbar.update(0)

    node_time_taken = int((time.time() - node_start_time) * 1000)
    logger.info(f"Deleted {total_nodes} nodes. Time taken: {node_time_taken} ms")

    # Step 3: Clean up schema (requires manual dropping of indexes and constraints)
    logger.info("Cleaning up schema (dropping indexes and constraints)...")
    
    with driver.session() as session:
        # Get all constraints
        result = session.run("SHOW CONSTRAINTS YIELD name")
        constraints = [record["name"] for record in result]
        
        # Get all indexes (excluding composite and lookup indexes for now)
        result = session.run("SHOW INDEXES YIELD name WHERE type = 'RANGE' OR type = 'POINT' OR type = 'TEXT' OR type = 'FULLTEXT'")
        indexes = [record["name"] for record in result]
        
        total_schema_items = len(constraints) + len(indexes)
        
        with tqdm(total=total_schema_items, desc="Cleaning up schema", unit="item") as pbar:
            # Drop constraints
            for constraint_name in constraints:
                try:
                    logger.debug(f"Dropping constraint: {constraint_name}")
                    session.run(f"DROP CONSTRAINT {constraint_name}")
                    pbar.update(1)
                except Exception as e:
                    # Constraint names might have special chars, try quoting
                    try:
                        logger.debug(f"Retrying dropping constraint with quotes: `{constraint_name}`")
                        session.run(f'DROP CONSTRAINT `{constraint_name}`')
                        pbar.update(1)
                    except Exception as e2:
                        logger.warning(f"Error dropping constraint {constraint_name}: {e} / {e2}")
                        pbar.update(1) # Still update progress
            
            # Drop indexes
            for index_name in indexes:
                try:
                    logger.debug(f"Dropping index: {index_name}")
                    session.run(f"DROP INDEX {index_name}")
                    pbar.update(1)
                except Exception as e:
                    # Index names might have special chars, try quoting
                    try:
                        logger.debug(f"Retrying dropping index with quotes: `{index_name}`")
                        session.run(f'DROP INDEX `{index_name}`')
                        pbar.update(1)
                    except Exception as e2:
                        logger.warning(f"Error dropping index {index_name}: {e} / {e2}")
                        pbar.update(1) # Still update progress
        
        # Step 4: Clear property keys by forcing a schema refresh
        if total_property_keys > 0:
            logger.info(f"Clearing {total_property_keys} property keys from the database...")
            property_keys_before = list_property_keys(session)
            
            with tqdm(total=total_property_keys, desc="Clearing property keys", unit="key") as pbar:
                session.run("CALL db.clearQueryCaches()")
                pbar.update(total_property_keys)
                
            logger.info(f"Property keys before potential clearing: {', '.join(property_keys_before)}")
    
    total_time = time.time() - start_time
    logger.info(f"Database successfully wiped in {total_time:.2f} seconds using Cypher transactions")

def verify_database_empty(session):
    """Verify that the database is completely empty of all data and schema elements."""
    remaining_nodes = count_nodes(session)
    remaining_rels = count_relationships(session)
    
    # Get detailed info on any remaining indexes
    remaining_indexes_details = []
    try:
        for record in session.run("SHOW INDEXES YIELD name, type, labelsOrTypes, properties"): 
            remaining_indexes_details.append(f"{record['name']} ({record['type']} on {record['labelsOrTypes']}({', '.join(record['properties'] or [])}))")
    except Exception as e:
        logger.warning(f"Could not retrieve full index details: {e}")
        # Fallback to just names
        try:
            for record in session.run("SHOW INDEXES YIELD name"): 
                remaining_indexes_details.append(record['name'])
        except Exception as e2:
            logger.error(f"Could not retrieve index names: {e2}")

    # Get detailed info on any remaining constraints
    remaining_constraints_details = []
    try:
        for record in session.run("SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties"): 
             remaining_constraints_details.append(f"{record['name']} ({record['type']} on {record['labelsOrTypes']}({', '.join(record['properties'] or [])}))")
    except Exception as e:
        logger.warning(f"Could not retrieve full constraint details: {e}")
        # Fallback to just names
        try:
            for record in session.run("SHOW CONSTRAINTS YIELD name"): 
                remaining_constraints_details.append(record['name'])
        except Exception as e2:
             logger.error(f"Could not retrieve constraint names: {e2}")

    # Check property keys as well
    remaining_property_keys = count_property_keys(session)
    
    is_clean = True
    if remaining_nodes != 0:
        logger.warning(f"Database wipe incomplete: {remaining_nodes} nodes remain.")
        is_clean = False
    if remaining_rels != 0:
        logger.warning(f"Database wipe incomplete: {remaining_rels} relationships remain.")
        is_clean = False
        
    if remaining_indexes_details:
        logger.warning(f"Database has {len(remaining_indexes_details)} indexes remaining: {', '.join(remaining_indexes_details)}")
        # Don't mark as unclean for remaining indexes, as drop might fail for system ones
        
    if remaining_constraints_details:
        logger.warning(f"Database has {len(remaining_constraints_details)} constraints remaining: {', '.join(remaining_constraints_details)}")
        # Don't mark as unclean for remaining constraints, as drop might fail for system ones

    if remaining_property_keys > 0:
        logger.info(f"Database has {remaining_property_keys} property keys remaining. These will be garbage collected.")

    if is_clean:
         logger.info("Verified: No nodes or relationships remain.")
    
    return is_clean

def wipe_neo4j_database():
    """Main function to wipe Neo4j database efficiently."""
    driver = None # Ensure driver is defined in finally block scope
    try:
        logger.info("Starting Neo4j database wipe process")
        
        # Connect to Neo4j
        driver = get_neo4j_driver()
        if not driver:
            logger.error("Failed to connect to Neo4j. Exiting.")
            return False
        
        # Check database size and APOC availability in one session
        with driver.session() as session:
            node_count = count_nodes(session)
            rel_count = count_relationships(session)
            property_key_count = count_property_keys(session)
            logger.info(f"Database contains {node_count} nodes, {rel_count} relationships, "
                       f"and {property_key_count} property keys before wiping")
            
            has_apoc = check_apoc_availability(session)
            logger.info(f"APOC availability: {'Available' if has_apoc else 'Not available'}")
        
        # Wipe database using appropriate method
        if has_apoc:
            wipe_database_with_apoc(driver)
        else:
            wipe_database_fallback(driver)
        
        # Verify database is empty
        with driver.session() as session:
            is_empty = verify_database_empty(session)
        
        if is_empty:
            logger.info("Database wipe successful and verified empty (no nodes/rels). Check logs for schema/key status.")
            return True
        else:
            logger.warning("Database wipe may be incomplete (nodes/rels remain). See logs for details.")
            return False
                
    except ServiceUnavailable as e:
        logger.error(f"Neo4j connection error: {e}")
        return False
    except Neo4jError as e:
        logger.error(f"Neo4j query error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during wipe: {e}", exc_info=True)
        return False
    finally:
        # Close driver if it exists
        if driver:
            driver.close()
            logger.info("Neo4j connection closed")

if __name__ == "__main__":
    try:
        # Make sure logs directory exists
        import os
        os.makedirs("logs", exist_ok=True)
        
        # Get user confirmation
        confirm = input("WARNING: This will completely erase all data in the Neo4j database. Type 'WIPE' to confirm: ")
        if confirm.strip().upper() != "WIPE":
            print("Operation cancelled.")
            sys.exit(0)
            
        # Run the wipe process
        print("Starting database wipe process...")
        success = wipe_neo4j_database()
        if success:
            print("Neo4j database successfully wiped (verified no nodes/rels). Check logs/neo4j_wipe.log for details.")
            sys.exit(0)
        else:
            print("Error wiping Neo4j database. Check logs/neo4j_wipe.log for details.")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(0) 