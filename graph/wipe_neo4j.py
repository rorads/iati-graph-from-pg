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

def wipe_database_with_apoc(session):
    """Wipe Neo4j database using APOC's periodic.iterate for efficient batching."""
    logger.info("Wiping database using APOC procedures")
    
    start_time = time.time()
    
    # Count relationships before deletion for progress estimation
    total_rels = count_relationships(session)
    total_nodes = count_nodes(session)
    total_schema_items = count_indexes_and_constraints(session)
    
    # Step 1: Delete all relationships in batches
    logger.info(f"Deleting {total_rels} relationships in batches...")
    
    # Create a progress bar for relationships
    with tqdm(total=total_rels, desc="Deleting relationships", unit="rel") as pbar:
        result = session.run("""
            CALL apoc.periodic.iterate(
                'MATCH ()-[r]->() RETURN r',
                'DELETE r',
                {batchSize: 10000, parallel: false}
            )
            YIELD batches, total, timeTaken, committedOperations
            RETURN batches, total, timeTaken, committedOperations
        """)
        
        relationship_stats = result.single()
        # Update progress bar to completion
        pbar.update(total_rels)
        
    logger.info(f"Deleted {relationship_stats['total']} relationships in "
                f"{relationship_stats['batches']} batches. "
                f"Time taken: {relationship_stats['timeTaken']} ms")
    
    # Step 2: Delete all nodes in batches
    logger.info(f"Deleting {total_nodes} nodes in batches...")
    
    # Create a progress bar for nodes
    with tqdm(total=total_nodes, desc="Deleting nodes", unit="node") as pbar:
        result = session.run("""
            CALL apoc.periodic.iterate(
                'MATCH (n) RETURN n',
                'DETACH DELETE n',
                {batchSize: 5000, parallel: false}
            )
            YIELD batches, total, timeTaken, committedOperations
            RETURN batches, total, timeTaken, committedOperations
        """)
        
        node_stats = result.single()
        # Update progress bar to completion
        pbar.update(total_nodes)
    
    logger.info(f"Deleted {node_stats['total']} nodes in "
                f"{node_stats['batches']} batches. "
                f"Time taken: {node_stats['timeTaken']} ms")
    
    # Step 3: Clean up schema (indexes and constraints)
    logger.info("Cleaning up schema (dropping indexes and constraints)...")
    
    # Create a progress bar for schema cleanup
    with tqdm(total=total_schema_items, desc="Cleaning up schema", unit="item") as pbar:
        session.run("CALL apoc.schema.assert({}, {}, true)")
        pbar.update(total_schema_items)
    
    total_time = time.time() - start_time
    logger.info(f"Database successfully wiped in {total_time:.2f} seconds using APOC")

def wipe_database_fallback(session):
    """Wipe Neo4j database using CALL {...} IN TRANSACTIONS for when APOC is not available."""
    logger.info("Wiping database using Cypher transactions (APOC not available)")
    
    start_time = time.time()
    
    # Count before deletion for progress estimation
    total_rels = count_relationships(session)
    total_nodes = count_nodes(session)
    
    # Step 1: Delete all relationships in batches
    logger.info(f"Deleting {total_rels} relationships in batches...")
    
    # Create a progress bar for relationship deletion
    with tqdm(total=total_rels, desc="Deleting relationships", unit="rel") as pbar:
        # For transactions, we can't easily track progress in real-time
        # So we'll use a pseudo-progress approach
        session.run("""
            :auto MATCH ()-[r]->() 
            CALL { WITH r DELETE r } 
            IN TRANSACTIONS OF 10000 ROWS
        """)
        # After completion, update progress bar to 100%
        pbar.update(total_rels)
    
    # Step 2: Delete all nodes
    logger.info(f"Deleting {total_nodes} nodes in batches...")
    
    # Create a progress bar for node deletion
    with tqdm(total=total_nodes, desc="Deleting nodes", unit="node") as pbar:
        session.run("""
            :auto MATCH (n) 
            CALL { WITH n DETACH DELETE n } 
            IN TRANSACTIONS OF 5000 ROWS
        """)
        # After completion, update progress bar to 100%
        pbar.update(total_nodes)
    
    # Step 3: Clean up schema (requires manual dropping of indexes and constraints)
    logger.info("Cleaning up schema (dropping indexes and constraints)...")
    
    # Get all constraints
    result = session.run("SHOW CONSTRAINTS")
    constraints = list(result)
    
    # Get all indexes
    result = session.run("SHOW INDEXES")
    indexes = list(result)
    
    total_schema_items = len(constraints) + len(indexes)
    
    # Create a progress bar for schema cleanup
    with tqdm(total=total_schema_items, desc="Cleaning up schema", unit="item") as pbar:
        # Drop constraints
        for constraint in constraints:
            try:
                constraint_name = constraint[0] if isinstance(constraint, list) else constraint
                logger.info(f"Dropping constraint: {constraint_name}")
                session.run(f"DROP CONSTRAINT {constraint_name}")
                pbar.update(1)
            except Exception as e:
                logger.warning(f"Error dropping constraint {constraint_name}: {e}")
                pbar.update(1)  # Still update the progress bar even if there's an error
        
        # Drop indexes
        for index in indexes:
            try:
                index_name = index[0] if isinstance(index, list) else index
                logger.info(f"Dropping index: {index_name}")
                session.run(f"DROP INDEX {index_name}")
                pbar.update(1)
            except Exception as e:
                logger.warning(f"Error dropping index {index_name}: {e}")
                pbar.update(1)  # Still update the progress bar even if there's an error
    
    total_time = time.time() - start_time
    logger.info(f"Database successfully wiped in {total_time:.2f} seconds using Cypher transactions")

def wipe_neo4j_database():
    """Main function to wipe Neo4j database efficiently."""
    try:
        logger.info("Starting Neo4j database wipe process")
        
        # Connect to Neo4j
        driver = get_neo4j_driver()
        if not driver:
            logger.error("Failed to connect to Neo4j. Exiting.")
            return False
        
        with driver.session() as session:
            # Check database size before wiping
            node_count = count_nodes(session)
            logger.info(f"Database contains {node_count} nodes before wiping")
            
            # Check if APOC is available
            has_apoc = check_apoc_availability(session)
            logger.info(f"APOC availability: {'Available' if has_apoc else 'Not available'}")
            
            if has_apoc:
                wipe_database_with_apoc(session)
            else:
                wipe_database_fallback(session)
            
            # Verify database is empty
            remaining_nodes = count_nodes(session)
            
            if remaining_nodes == 0:
                logger.info("Database wipe successful. Database is now empty.")
                return True
            else:
                logger.warning(f"Database wipe incomplete. {remaining_nodes} nodes still remain.")
                return False
                
    except ServiceUnavailable as e:
        logger.error(f"Neo4j connection error: {e}")
        return False
    except Neo4jError as e:
        logger.error(f"Neo4j query error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return False
    finally:
        # Close driver if it exists
        if 'driver' in locals():
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
            print("Neo4j database successfully wiped.")
            sys.exit(0)
        else:
            print("Error wiping Neo4j database. Check logs for details.")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(0) 