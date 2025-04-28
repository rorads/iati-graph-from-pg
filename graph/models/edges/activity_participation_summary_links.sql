-- models/edges/activity_participation_summary_links.sql
-- Summarises all declared connections between distinct activity_id and related_activity_id pairs, aggregating role codes and names.

{{
  config(
    materialized='table'
  )
}}

WITH base_links AS (
    SELECT
        activity_id,
        related_activity_id,
        role_code,
        role_name
    FROM {{ ref('participation_links') }}
    WHERE related_activity_id IS NOT NULL
),
aggregated_links AS (
    SELECT
        activity_id AS source_activity_id,
        related_activity_id AS target_activity_id,
        ARRAY_AGG(DISTINCT role_code) AS role_codes,
        ARRAY_AGG(DISTINCT role_name) AS role_names
    FROM base_links
    GROUP BY activity_id, related_activity_id
)
SELECT
    source_activity_id,
    target_activity_id,
    role_codes,
    role_names
FROM aggregated_links
ORDER BY source_activity_id, target_activity_id; 