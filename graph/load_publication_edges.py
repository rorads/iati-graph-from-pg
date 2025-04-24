#!/usr/bin/env python
# graph/simple_publication_edges.py

import argparse
import os
import sys
import time

import psycopg2
import psycopg2.extras
from tqdm import tqdm

from db_utils import get_neo4j_driver, get_postgres_connection

# Configuration
BATCH_SIZE = 1000
RELATIONSHIP_TYPE = "PUBLISHES"
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "summary_publication_edges.log")

def create_publishes_relationships(pg_conn, neo4j_driver, batch_size=BATCH_SIZE, limit=None, debug=False):
    """
    Create :PUBLISHES relationships from organisations to activities based on their IDs
    """
    print(f"\n--- Creating {RELATIONSHIP_TYPE} relationships ---")
    
    # Ensure log directory exists
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # SQL query to fetch activities with their reporting org references
    sql_query = """
    SELECT 
        a.iatiidentifier as activity_id,
        a.reportingorg_ref as org_ref
    FROM 
        iati_graph.published_activities a
    JOIN 
        iati_graph.published_organisations o ON a.reportingorg_ref = o.organisationidentifier
    WHERE 
        a.reportingorg_ref IS NOT NULL
    """
    
    # Add a LIMIT clause if requested
    if limit:
        sql_query += f" LIMIT {limit}"
        print(f"Testing mode: Processing only {limit} relationships")
    
    # Cypher query to create relationships
    cypher_query = f"""
    UNWIND $batch as row
    
    MATCH (org:PublishedOrganisation {{organisationidentifier: row.org_ref}})
    MATCH (activity:PublishedActivity {{iatiidentifier: row.activity_id}})
    
    MERGE (org)-[:{RELATIONSHIP_TYPE}]->(activity)
    
    RETURN count(*) as count
    """
    
    # Get count of potential relationships
    count_query = """
    SELECT COUNT(*) 
    FROM iati_graph.published_activities a
    JOIN iati_graph.published_organisations o ON a.reportingorg_ref = o.organisationidentifier
    WHERE a.reportingorg_ref IS NOT NULL
    """
    
    if limit:
        count = min(limit, get_pg_count(pg_conn, count_query))
    else:
        count = get_pg_count(pg_conn, count_query)
    
    print(f"Found {count:,} potential relationships to create")
    
    # Process in batches
    created_count = 0
    skipped_count = 0
    processed_count = 0
    
    start_time = time.time()
    
    try:
        with pg_conn.cursor(name='simple_pub_cursor', cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.itersize = batch_size
            cursor.execute(sql_query)
            
            with tqdm(total=count, desc=f"Creating :{RELATIONSHIP_TYPE}", unit="rels") as pbar:
                while True:
                    batch_data = cursor.fetchmany(batch_size)
                    if not batch_data:
                        break
                    
                    # Prepare batch for Neo4j
                    batch = []
                    for row in batch_data:
                        activity_id = row['activity_id']
                        org_ref = row['org_ref']
                        batch.append({'activity_id': activity_id, 'org_ref': org_ref})
                    
                    batch_size_actual = len(batch)
                    if batch_size_actual == 0:
                        continue
                    
                    # Process batch in Neo4j
                    try:
                        with neo4j_driver.session() as session:
                            result = session.run(cypher_query, batch=batch).single()
                            created = result['count'] if result else 0
                            
                            created_count += created
                            processed_count += batch_size_actual
                            skipped_count += (batch_size_actual - created)
                            
                    except Exception as e:
                        print(f"\nError processing batch: {e}")
                        with open(LOG_FILE, 'a') as f:
                            f.write(f"Error processing batch: {e}\n")
                            if debug:
                                f.write(f"Problematic batch (sample): {batch[:5]}\n")
                        
                        # Skip this batch and continue
                        skipped_count += batch_size_actual
                    
                    # Update progress
                    pbar.update(batch_size_actual)
                    
                    # Status updates for large datasets
                    # if processed_count % 50000 == 0:
                    #     print(f"\n  Progress: {processed_count:,}/{count:,} processed, {created_count:,} created")
    
    except Exception as e:
        print(f"Error during processing: {e}")
        return False, processed_count, created_count
    
    # Print summary
    elapsed_time = time.time() - start_time
    rate = processed_count / elapsed_time if elapsed_time > 0 else 0
    
    print(f"\n--- {RELATIONSHIP_TYPE} Creation Summary ---")
    print(f"Total processed: {processed_count:,}")
    print(f"Relationships created: {created_count:,}")
    print(f"Skipped: {skipped_count:,}")
    print(f"Process completed in {elapsed_time:.2f} seconds")
    print(f"Processing rate: {rate:.1f} rows/second")
    
    # Log summary to file
    with open(LOG_FILE, 'a') as f:
        f.write(f"\n--- {RELATIONSHIP_TYPE} Creation Summary ---\n")
        f.write(f"Total processed: {processed_count:,}\n")
        f.write(f"Relationships created: {created_count:,}\n")
        f.write(f"Skipped: {skipped_count:,}\n")
        f.write(f"Process completed in {elapsed_time:.2f} seconds\n")
        f.write(f"Processing rate: {rate:.1f} rows/second\n")
    
    return True, processed_count, created_count

def get_pg_count(pg_conn, query):
    """Get count from PostgreSQL with the provided query"""
    with pg_conn.cursor() as cursor:
        cursor.execute(query)
        count = cursor.fetchone()[0]
        return count

def main():
    parser = argparse.ArgumentParser(description="Create PUBLISHES relationships from organisations to activities")
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE, 
                        help=f'Batch size for processing (default: {BATCH_SIZE})')
    parser.add_argument('--debug', action='store_true', 
                        help='Enable debug mode with more verbose logging')
    parser.add_argument('--limit', type=int, 
                        help='Limit the number of relationships to process (for testing)')
    parser.add_argument('--direction', choices=['org_to_activity', 'activity_to_org'], default='org_to_activity',
                        help='Direction of PUBLISHES relationship (default: org_to_activity)')
    
    args = parser.parse_args()
    
    # Get database connections
    print("Connecting to databases...")
    neo4j_driver = get_neo4j_driver()
    pg_conn = get_postgres_connection()
    
    if not neo4j_driver or not pg_conn:
        print("Failed to connect to one or both databases.")
        return 1
    
    # Run the loading process
    try:
        success, processed, created = create_publishes_relationships(
            pg_conn, 
            neo4j_driver, 
            args.batch_size,
            args.limit,
            args.debug
        )
        
        return 0 if success else 1
    
    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
        return 1
    
    finally:
        # Close database connections
        if neo4j_driver:
            neo4j_driver.close()
        if pg_conn:
            pg_conn.close()

if __name__ == "__main__":
    sys.exit(main()) 