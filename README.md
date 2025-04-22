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
    This will download the `iati.dump.gz` file into the `data/pg_dump/` directory (which is ignored by Git). It uses the `-N` flag to only download if the remote file is newer or the local file is missing.

5.  **Start the databases:**
    ```bash
    make docker-up
    ```
    This will start PostgreSQL (port 5432) and Neo4j (ports 7474, 7687) in the background. The first time Postgres starts, it will attempt to restore the dump from `data/pg_dump/iati.dump.gz` into the `iati` database.

## Usage

*   **Access PostgreSQL:** Connect using a client like `psql` or DBeaver to `localhost:5432` with the password you set.
*   **Access Neo4j Browser:** Open `http://localhost:7474` in your web browser. Log in with username `neo4j` and the password you set.

### Graph Loading Process

The project implements a sequential ETL pipeline that loads data from PostgreSQL into Neo4j. The primary script orchestrates loading in the following order:

1. **Nodes:**
   * Published Activities (aid projects that appear in IATI data)
   * Published Organizations (organizations with IATI publisher accounts)
   * Phantom Activities (activities referenced but not published in IATI)
   * Phantom Organizations (organizations referenced but not formally published)

2. **Relationships:**
   * Participation Links (connects organizations to activities)
   * Financial Transactions (monetary flows between organizations and activities)
   * Funds Links (direct activity-to-activity financial relationships)
   * Hierarchy Links (`:PARENT_OF` relationships between activities)

To build the underlying dbt models and then load the complete graph into Neo4j (running from the project root):
```bash
make dbt-build
make load_graph
```
**Note:** Graph loading scripts (`load_*.py`) and the main `load_graph.py` script are run from within the `graph/` directory by the `make` targets.

You can also run individual loading scripts *from within the `graph/` directory* if needed, after running `make dbt-build`:
```bash
cd graph
uv run python load_published_activities.py
uv run python load_funds_edges.py
uv run python load_hierarchy_edges.py
# etc.
```

To completely wipe the Neo4j database (useful for reloading):
```bash
make wipe-neo4j
```

### SQL Transformations with dbt

The project uses dbt (data build tool) for SQL transformations within the `graph/` directory.

```bash
# Load environment variables from .env into your current shell (run from project root)
source .env 

# Install/update dbt dependencies (run from graph/ directory)
cd graph
uv run dbt deps

# Build dbt models (run from project root)
make dbt-build

# Alternatively, run dbt commands manually from the graph/ directory:
cd graph
uv run dbt run
# Run specific model
uv run dbt run --select model_name
```

### Docker Commands

*   **Stop databases:** `make docker-down`
*   **View logs:** `docker-compose logs -f`
*   **List Makefile targets:** `make help`

### Other Useful Makefile Targets

The `Makefile` provides several convenient targets (run from the project root):

*   `make setup-env`: Creates `.env` from `.env.example` if it doesn't exist.
*   `make download-dump`: Downloads the IATI Postgres dump.
*   `make dbt-build`: Runs `dbt build` within the `graph/` directory.
*   `make load_graph`: Runs the main Python graph loading script (`graph/load_graph.py`).
*   `make wipe-neo4j`: Runs the script to clear all data from the Neo4j database (`graph/wipe_neo4j.py`).
*   `make clone-schemas`: Clones the official IATI-Schemas repository into `additional-resources/IATI-Schemas`. This is not required for the core functionality but can be useful for development or exploration, potentially for agentic development tasks needing schema details.

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
  * `:PARENT_OF` - Represents the hierarchical relationship between activities

## Project Structure

* `docker-compose.yml` - Container configuration
* `postgres-init/` - PostgreSQL initialization scripts
* `data/pg_dump/` - Directory for storing the IATI database dump
* `graph/` - DBT project and Neo4j loading scripts
  * `models/` - DBT models defining SQL transformations
  * `utils/` - Utility scripts (e.g., Neo4j interaction)
  * `load_*.py` - Individual ETL scripts for loading specific nodes/edges
  * `load_graph_sequential.py` - Master script that runs all ETL steps in sequence
  * `wipe_neo4j.py` - Script to wipe the Neo4j database
* `additional-resources/` - Optional directory for supplementary resources (e.g., IATI Schemas cloned via `make clone-schemas`)
* `.env` - Local environment configuration (created via `make setup-env`, ignored by Git)
* `.env.example` - Example environment file
* `Makefile` - Defines common tasks and commands
* `uv.lock`, `pyproject.toml` - Python dependency management files