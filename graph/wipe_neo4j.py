#!/usr/bin/env python3
# graph/wipe_neo4j.py

import time
import sys
import logging
from neo4j.exceptions import Neo4jError, ClientError, ServiceUnavailable

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

def wipe_database_with_apoc(session):
    """Wipe Neo4j database using APOC's periodic.iterate for efficient batching."""
    logger.info("Wiping database using APOC procedures")
    
    start_time = time.time()
    
    # Step 1: Delete all relationships in batches
    logger.info("Deleting relationships in batches...")
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
    logger.info(f"Deleted {relationship_stats['total']} relationships in "
                f"{relationship_stats['batches']} batches. "
                f"Time taken: {relationship_stats['timeTaken']} ms")
    
    # Step 2: Delete all nodes in batches
    logger.info("Deleting nodes in batches...")
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
    logger.info(f"Deleted {node_stats['total']} nodes in "
                f"{node_stats['batches']} batches. "
                f"Time taken: {node_stats['timeTaken']} ms")
    
    # Step 3: Clean up schema (indexes and constraints)
    logger.info("Cleaning up schema (dropping indexes and constraints)...")
    session.run("CALL apoc.schema.assert({}, {}, true)")
    
    total_time = time.time() - start_time
    logger.info(f"Database successfully wiped in {total_time:.2f} seconds using APOC")

def wipe_database_fallback(session):
    """Wipe Neo4j database using CALL {...} IN TRANSACTIONS for when APOC is not available."""
    logger.info("Wiping database using Cypher transactions (APOC not available)")
    
    start_time = time.time()
    
    # Step 1: Delete all relationships in batches
    logger.info("Deleting relationships in batches...")
    session.run("""
        :auto MATCH ()-[r]->() 
        CALL { WITH r DELETE r } 
        IN TRANSACTIONS OF 10000 ROWS
    """)
    
    # Step 2: Delete all nodes
    logger.info("Deleting nodes in batches...")
    session.run("""
        :auto MATCH (n) 
        CALL { WITH n DETACH DELETE n } 
        IN TRANSACTIONS OF 5000 ROWS
    """)
    
    # Step 3: Clean up schema (requires manual dropping of indexes and constraints)
    logger.info("Cleaning up schema (dropping indexes and constraints)...")
    
    # Drop constraints
    for constraint in session.run("SHOW CONSTRAINTS").values():
        try:
            constraint_name = constraint[0] if isinstance(constraint, list) else constraint
            logger.info(f"Dropping constraint: {constraint_name}")
            session.run(f"DROP CONSTRAINT {constraint_name}")
        except Exception as e:
            logger.warning(f"Error dropping constraint {constraint_name}: {e}")
    
    # Drop indexes
    for index in session.run("SHOW INDEXES").values():
        try:
            index_name = index[0] if isinstance(index, list) else index
            logger.info(f"Dropping index: {index_name}")
            session.run(f"DROP INDEX {index_name}")
        except Exception as e:
            logger.warning(f"Error dropping index {index_name}: {e}")
    
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
            result = session.run("""
                MATCH (n)
                RETURN count(n) AS node_count
            """)
            record = result.single()
            node_count = record["node_count"] if record else 0
            
            logger.info(f"Database contains {node_count} nodes before wiping")
            
            # Check if APOC is available
            has_apoc = check_apoc_availability(session)
            logger.info(f"APOC availability: {'Available' if has_apoc else 'Not available'}")
            
            if has_apoc:
                wipe_database_with_apoc(session)
            else:
                wipe_database_fallback(session)
            
            # Verify database is empty
            result = session.run("MATCH (n) RETURN count(n) AS node_count")
            record = result.single()
            remaining_nodes = record["node_count"] if record else -1
            
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