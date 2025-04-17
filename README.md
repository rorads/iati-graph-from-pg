# IATI Graph from Postgres Dump

This project sets up a PostgreSQL and Neo4j database using Docker Compose and provides tools to download and potentially process IATI data.

## Prerequisites

*   Docker and Docker Compose
*   Make
*   [uv](https://github.com/astral-sh/uv) (for Python package management)

## Setup

1.  **Clone the repository (if applicable):**
    ```bash
    git clone <your-repo-url>
    cd iati-graph-from-pg
    ```

2.  **Install Python dependencies:**
    ```bash
    uv sync
    ```

3.  **Set up Environment:**
    *   Run `make setup-env`. This creates a `.env` file from `.env.example` if it doesn't exist.
    *   Edit `docker-compose.yml` and set secure passwords for `POSTGRES_PASSWORD` and `NEO4J_AUTH`.
    *   *(Optional)* Edit `.env` if you need to change `DBT_PROJECT_DIR` or add other variables.

4.  **Download IATI data dump:**
    ```bash
    make download-dump
    ```
    This will download the `iati.dump.gz` file into the `data/pg_dump/` directory (which is ignored by Git).

5.  **Start the databases:**
    ```bash
    docker-compose up -d
    ```
    This will start PostgreSQL (port 5432) and Neo4j (ports 7474, 7687) in the background. The first time Postgres starts, it will attempt to restore the dump from `data/pg_dump/iati.dump.gz` into the `iati` database.

## Usage

*   **Access PostgreSQL:** Connect using a client like `psql` or DBeaver to `localhost:5432` with the password you set.
*   **Access Neo4j Browser:** Open `http://localhost:7474` in your web browser. Log in with username `neo4j` and the password you set.
*   **Running dbt:**
    ```bash
    # Load environment variables from .env into your current shell
    source .env 
    
    # Install dependencies (needed after changes to packages in dbt_project.yml)
    uv run dbt deps
    
    # Run dbt models
    uv run dbt run
    
    # Run other dbt commands similarly...
    ```
*   **Stop databases:** `docker-compose down`
*   **View logs:** `docker-compose logs -f`
*   **List Makefile targets:** `make help`

## Next Steps

*   Implement logic to restore the PostgreSQL dump.
*   Implement logic to transform and load data from PostgreSQL to Neo4j.
