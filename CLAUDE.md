# IATI Graph from PostgreSQL - Claude Code Instructions

## Project Overview
This project transforms IATI (International Aid Transparency Initiative) data from PostgreSQL into a Neo4j graph database to enable advanced analysis of aid flows and relationships.

## Development Guidelines

### Git Workflow
- Commit changes as you work
- For small changes, ask for confirmation at the end
- For large features, commit unilaterally
- Always follow Conventional Commits specification (https://www.conventionalcommits.org/)
- Format: `<type>(<scope>): <subject>`
- Types: feat, fix, docs, style, refactor, perf, test, chore
- Subject should start with lowercase and have no period

### Database Interaction
- Use MCP tools for database interaction instead of command line
- PostgreSQL and Neo4j are available via MCP servers
- Prefer MCP tools over direct docker exec commands

### Code Style
- Follow existing patterns in the codebase
- Use dbt for data transformations
- Python scripts for ETL processes
- Document complex queries and transformations

### Testing
- Validate data integrity at each stage
- Test dbt models before graph loading
- Verify graph relationships after loading