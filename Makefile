# Load environment variables from .env file if it exists
# Variables in .env will override defaults below
ifneq ($(wildcard .env),)
include .env
export $(shell sed 's/=.*//' .env) # Export variables defined in .env to subshells
endif

# --- Defaults --- 
# Default dbt project directory (relative to Makefile)
DBT_PROJECT_DIR ?= graph

# --- Targets --- 
.PHONY: help download-dump clone-schemas setup-env docker-up docker-down

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  setup-env        Create .env file from .env.example if it doesn't exist."
	@echo "                   After running, use 'source .env' to load variables into your shell."
	@echo "		   Remember to update the .env file with your own values."
	@echo "  download-dump    Download the IATI Postgres dump file (-N flag used)."
	@echo "  clone-schemas    Clone the IATI-Schemas repository into additional-resource/."
	@echo "  dbt-build        Run dbt build in graph directory."
	@echo "  load_graph       Load the graph to neo4j from the graph directory. (requires dbt-build first)"
	@echo "  wipe-neo4j       Wipe the neo4j database and start fresh."
	@echo ""
	@echo "Docker Compose:"
	@echo "  docker-up        Start services defined in docker-compose.yml in detached mode."
	@echo "  docker-down      Stop services defined in docker-compose.yml."
	@echo ""
	@echo "To run dbt commands:"
	@echo "  1. Run 'make setup-env' (once)."
	@echo "  2. Run 'source .env' in your shell."
	@echo "  3. Run dbt commands directly, e.g.:"
	@echo "     uv run dbt deps"
	@echo "     uv run dbt run"

setup-env:
	@if [ ! -f .env ]; then \
		echo "Creating .env file from .env.example..."; \
		cp .env.example .env; \
	else \
		echo ".env file already exists. No action taken."; \
	fi

download-dump:
	@mkdir -p data/pg_dump
	# Add -N to only download if the remote file is newer or local is missing
	@wget -N -P data/pg_dump https://data.tables.iatistandard.org/iati.dump.gz 

clone-schemas:
	@echo "Cloning IATI-Schemas repository..."
	@git clone https://github.com/IATI/IATI-Schemas.git additional-resources/IATI-Schemas
	@echo "IATI-Schemas cloned successfully."

docker-up:
	@echo "Starting Docker containers..."
	@docker compose up -d
	@echo "Docker containers started."

docker-down:
	@echo "Stopping Docker containers..."
	@docker compose down
	@echo "Docker containers stopped."

dbt-build:
	@echo "Running dbt build in graph directory..."
	@cd $(DBT_PROJECT_DIR) && uv run dbt build
	@echo "dbt build completed."

load_graph:
	@echo "Loading graph to neo4j..."
	@cd $(DBT_PROJECT_DIR) && uv run python load_graph_sequential.py
	@echo "Graph loaded to neo4j."

wipe-neo4j:
	@echo "Wiping neo4j database..."
	@cd $(DBT_PROJECT_DIR) && uv run python wipe_neo4j.py
	@echo "Neo4j database wiped."
