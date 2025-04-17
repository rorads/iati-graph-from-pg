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
.PHONY: help download-dump clone-schemas setup-env dbt-deps dbt-run

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  setup-env        Create .env file from .env.example if it doesn't exist."
	@echo "  download-dump    Download the IATI Postgres dump file."
	@echo "  clone-schemas    Clone the IATI-Schemas repository into additional-resource/."
	@echo "  dbt-deps         Install dbt dependencies."
	@echo "  dbt-run          Run dbt models (uses DBT_PROJECT_DIR from .env or default)."

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

dbt-deps:
	@echo "Installing dbt dependencies for project in $(DBT_PROJECT_DIR)..."
	uv run dbt deps --project-dir $(DBT_PROJECT_DIR) --profiles-dir .

dbt-run: dbt-deps
	@echo "Running dbt models in project $(DBT_PROJECT_DIR)..."
	uv run dbt run --project-dir $(DBT_PROJECT_DIR) --profiles-dir .

