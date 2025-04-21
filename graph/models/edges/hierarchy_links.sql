-- models/edges/hierarchy_links.sql
-- Creates parent-child relationships between activities, compressing multiple declarations
-- into one edge which records which activities declared it.
{{
  config(
    materialized='table'
  )
}}

WITH raw AS (
  -- Extract relevant related-activity records (types 1=Parent, 2=Child)
  SELECT
    iatiidentifier AS source_id,
    ref AS target_id,
    type AS rel_type
  FROM {{ source('iati_postgres', 'relatedactivity') }}
  WHERE
    iatiidentifier IS NOT NULL AND iatiidentifier <> ''
    AND ref IS NOT NULL AND ref <> ''
    AND type IN ('1', '2')  -- 1=Parent, 2=Child
),
parent_child AS (
  -- Aggregate parent-child declarations (types 1=Parent, 2=Child)
  SELECT
    -- For type 1 (Parent), ref is parent; for type 2 (Child), source is parent
    CASE
      WHEN raw.rel_type = '1' THEN raw.target_id
      ELSE raw.source_id
    END AS parent_id,
    CASE
      WHEN raw.rel_type = '1' THEN raw.source_id
      ELSE raw.target_id
    END AS child_id,
    ARRAY_AGG(DISTINCT raw.source_id) AS declared_by
  FROM raw
  WHERE raw.rel_type IN ('1', '2')
  GROUP BY 1, 2
)

-- Output parent-child relationships
SELECT
  parent_id      AS source_node_id,
  child_id       AS target_node_id,
  'ACTIVITY'     AS source_node_type,
  'ACTIVITY'     AS target_node_type,
  'PARENT_OF'    AS relationship_type,
  declared_by
FROM parent_child
ORDER BY source_node_id, target_node_id