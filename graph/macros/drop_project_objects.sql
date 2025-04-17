{% macro drop_project_objects() %}
    {# Get the target schema #}
    {% set target_schema = target.schema %}
    {{ log("Target schema to clean: " ~ target_schema, info=True) }}

    {# Get the list of relations (tables and views) in the target schema from the database #}
    {% set cleanup_query %}
        SELECT
            CASE 
                WHEN table_type = 'BASE TABLE' THEN 'TABLE'
                WHEN table_type = 'VIEW' THEN 'VIEW'
                ELSE NULL -- Add other types if needed, e.g., 'MATERIALIZED VIEW'
            END as relation_type,
            table_name as relation_name
        FROM {{ target.database }}.information_schema.tables
        WHERE table_schema = '{{ target_schema }}'
    {% endset %}

    {{ log("Querying database for relations in schema: " ~ target_schema, info=True) }}
    {% set relations_in_schema = run_query(cleanup_query) %}
    {{ log("Found " ~ relations_in_schema | length ~ " relations in schema.", info=True) }}

    {# Get the list of models, seeds, and snapshots defined in the current dbt project #}
    {% set project_nodes = graph.nodes.values() | selectattr('resource_type', 'in', ['model', 'seed', 'snapshot']) %}
    {% set project_relation_names = project_nodes | map(attribute='name') | map('lower') | list %} 
    {{ log("Found " ~ project_relation_names | length ~ " models, seeds, or snapshots in the project.", info=True) }}
    {# Note: DBT model names are case-insensitive during resolution but might be case-sensitive in the database #}
    {# Adjust case handling if your database is strictly case-sensitive for identifiers #}


    {% set drop_statements = [] %}
    {% set relations_dropped_count = 0 %}

    {# Iterate through relations found in the database schema #}
    {% for row in relations_in_schema %}
        {% set relation_type = row['relation_type'] %}
        {% set relation_name = row['relation_name'] %}
        
        {# Check if this relation corresponds to a model/seed/snapshot in the project #}
        {% if relation_name | lower in project_relation_names %}
            {% set drop_command = "DROP " ~ relation_type ~ " IF EXISTS " ~ target.database ~ "." ~ target_schema ~ "." ~ relation_name ~ " CASCADE;" %}
            {{ log("Preparing to drop: " ~ drop_command, info=True) }}
            {% do drop_statements.append(drop_command) %}
            {% set relations_dropped_count = relations_dropped_count + 1 %}
        {% else %}
             {{ log("Skipping relation (not found in project): " ~ relation_name, info=True) }}
        {% endif %}
    {% endfor %}

    {{ log("Generated " ~ drop_statements | length ~ " DROP statements.", info=True) }}

    {% if drop_statements %}
        {% for drop_statement in drop_statements %}
            {{ log("Executing: " ~ drop_statement, info=True) }}
            {% do run_query(drop_statement) %}
        {% endfor %}
        {{ log("Successfully dropped " ~ relations_dropped_count ~ " relations defined in this project from schema " ~ target_schema ~ ".", info=True) }}
    {% else %}
        {{ log("No relations defined in this project were found in schema " ~ target_schema ~ " to drop.", info=True) }}
    {% endif %}

{% endmacro %}