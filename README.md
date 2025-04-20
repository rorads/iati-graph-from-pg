# IATI Graph from Postgres Dump

This project sets up a PostgreSQL and Neo4j database using Docker Compose and processes IATI (International Aid Transparency Initiative) data into a graph database for relational analysis.

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
    make docker-up
    ```
    This will start PostgreSQL (port 5432) and Neo4j (ports 7474, 7687) in the background. The first time Postgres starts, it will attempt to restore the dump from `data/pg_dump/iati.dump.gz` into the `iati` database.

## Usage

*   **Access PostgreSQL:** Connect using a client like `psql` or DBeaver to `localhost:5432` with the password you set.
*   **Access Neo4j Browser:** Open `http://localhost:7474` in your web browser. Log in with username `neo4j` and the password you set.

### Graph Loading Process

The project implements a sequential ETL pipeline that loads data from PostgreSQL into Neo4j in the following order:

1. **Nodes:**
   * Published Activities (aid projects that appear in IATI data)
   * Published Organizations (organizations with IATI publisher accounts)
   * Phantom Activities (activities referenced but not published in IATI)
   * Phantom Organizations (organizations referenced but not formally published)

2. **Relationships:**
   * Participation Links (connects organizations to activities)
   * Financial Transactions (monetary flows between organizations and activities)
   * Funds Links (direct activity-to-activity financial relationships)

To load the complete graph:
```bash
cd graph
python load_graph_sequential.py
```

You can also run individual loading scripts if needed:
```bash
python load_published_activities.py
python load_funds_edges.py
# etc.
```

### SQL Transformations with dbt

The project uses dbt (data build tool) for SQL transformations:
```bash
# Load environment variables from .env into your current shell
source .env 

# Install dependencies (needed after changes to packages in dbt_project.yml)
uv run dbt deps

# Run dbt models
uv run dbt run

# Run specific model
uv run dbt run --select model_name
```

### Docker Commands

*   **Stop databases:** `make docker-down`
*   **View logs:** `docker-compose logs -f`
*   **List Makefile targets:** `make help`

## Graph Model

The Neo4j graph consists of:

* **Node Types:**
  * `:PublishedActivity` - Activities directly published in IATI data
  * `:PhantomActivity` - Activities referenced but not directly published
  * `:PublishedOrganisation` - Organizations with IATI publisher accounts
  * `:PhantomOrganisation` - Organizations referenced in IATI data

* **Relationship Types:**
  * `:PARTICIPATES_IN` - Connects organizations to activities
  * `:FINANCIAL_TRANSACTION` - Represents financial flows between organizations and activities
  * `:FUNDS` - Direct activity-to-activity funding relationship (aggregated from transactions)

## Project Structure

* `docker-compose.yml` - Container configuration
* `postgres-init/` - PostgreSQL initialization scripts
* `data/pg_dump/` - Directory for storing the IATI database dump
* `graph/` - DBT project and Neo4j loading scripts
  * `models/` - DBT models defining SQL transformations
  * `load_*.py` - Individual ETL scripts for loading specific nodes/edges
  * `load_graph_sequential.py` - Master script that runs all ETL steps in sequence