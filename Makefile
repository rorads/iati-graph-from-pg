.PHONY: help download-dump clone-schemas

help:
	@echo "Makefile targets:"
	@echo "  download-dump  Download the IATI Postgres dump file."
	@echo "  clone-schemas  Clone the IATI-Schemas repository into additional-resource/."

download-dump:
	@mkdir -p data/pg_dump
	@wget -P data/pg_dump https://data.tables.iatistandard.org/iati.dump.gz

clone-schemas:
	@echo "Cloning IATI-Schemas repository..."
	@git clone https://github.com/IATI/IATI-Schemas.git additional-resources/IATI-Schemas
	@echo "IATI-Schemas cloned successfully."