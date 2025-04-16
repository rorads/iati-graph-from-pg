.PHONY: help download-dump

help:
	@echo "Makefile targets:"
	@echo "  download-dump  Download the IATI Postgres dump file."

download-dump:
	@mkdir -p data/pg_dump
	@wget -P data/pg_dump https://data.tables.iatistandard.org/iati.dump.gz
