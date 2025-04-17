-- models/edges/participation_links.sql
-- Links organisations to the activities they participate in, based on the participatingorg table.

{{ 
  config(
    materialized='table'
  )
}}

SELECT DISTINCT
    po.iatiidentifier AS activity_id, -- The activity identifier
    po.ref AS organisation_id,        -- The participating organisation identifier
    po.role AS role_code,               -- The role code (e.g., '1' for Funding)
    po.rolename AS role_name            -- The human-readable role name
FROM 
    {{ source('iati', 'participatingorg') }} po
WHERE 
    po.ref IS NOT NULL AND po.ref <> '' -- Ensure the organisation identifier exists
    AND po.iatiidentifier IS NOT NULL AND po.iatiidentifier <> '' -- Ensure the activity identifier exists
    AND po.role IS NOT NULL AND po.role <> '' -- Ensure role information exists
ORDER BY
    activity_id, 
    organisation_id,
    role_code 