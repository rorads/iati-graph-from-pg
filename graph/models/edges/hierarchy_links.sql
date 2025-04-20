-- models/edges/hierarchy_links.sql
-- Creates parent-child and sibling relationships between activities, compressing multiple declarations
-- into one edge which records which activities declared it.
{{
  config(
    materialized='table'
  )
}}

WITH raw AS (
  -- Extract relevant related-activity records (types 1=Parent, 2=Child, 3=Sibling)
  SELECT
    iatiidentifier AS source_id,
    ref AS target_id,
    type AS rel_type
  FROM {{ source('iati_postgres', 'relatedactivity') }}
  WHERE
    iatiidentifier IS NOT NULL AND iatiidentifier <> ''
    AND ref IS NOT NULL AND ref <> ''
    AND type IN ('1', '2', '3')  -- 1=Parent, 2=Child, 3=Sibling
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
),
explicit_siblings AS (
  -- Explicit sibling relationships (type 3)
  SELECT
    LEAST(raw.source_id, raw.target_id) AS node1_id,
    GREATEST(raw.source_id, raw.target_id) AS node2_id,
    ARRAY_AGG(DISTINCT raw.source_id) AS declared_by
  FROM raw
  WHERE raw.rel_type = '3'
  GROUP BY 1, 2
),
dedup_siblings AS (
  -- Siblings inferred from shared parent in parent_child
  SELECT
    LEAST(pc1.child_id, pc2.child_id) AS node1_id,
    GREATEST(pc1.child_id, pc2.child_id) AS node2_id,
    ARRAY_AGG(DISTINCT pc1.parent_id) AS declared_by
  FROM parent_child pc1
  JOIN parent_child pc2
    ON pc1.parent_id = pc2.parent_id
   AND pc1.child_id < pc2.child_id
  GROUP BY 1, 2
),
siblings AS (
  -- Combine explicit and inferred siblings
  SELECT * FROM explicit_siblings
  UNION ALL
  SELECT * FROM dedup_siblings
)

-- Combine parent-child and sibling relationships into one edge set
SELECT
  parent_id      AS source_node_id,
  child_id       AS target_node_id,
  'ACTIVITY'     AS source_node_type,
  'ACTIVITY'     AS target_node_type,
  'PARENT_OF'    AS relationship_type,
  declared_by
FROM parent_child
UNION ALL
SELECT
  node1_id       AS source_node_id,
  node2_id       AS target_node_id,
  'ACTIVITY'     AS source_node_type,
  'ACTIVITY'     AS target_node_type,
  'SIBLING_OF'   AS relationship_type,
  declared_by
FROM siblings
ORDER BY source_node_id, target_node_id, relationship_type