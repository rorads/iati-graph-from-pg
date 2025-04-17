-- models/published_activites.sql
-- This DBT model provides a canonical version of each activity, eliminating duplicates
-- by selecting the most recently updated version of each activity.

{{
  config(
    materialized='table'
  )
}}

WITH ranked_activities AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY iatiidentifier 
            ORDER BY 
                -- Use the most recent version based on lastupdateddatetime
                lastupdateddatetime DESC NULLS LAST,
                -- If no lastupdateddatetime, fall back to dataset name which may contain date information
                dataset DESC
        ) AS row_num
    FROM 
        {{ source('iati', 'activity') }}
    WHERE 
        iatiidentifier IS NOT NULL
)

SELECT 
    iatiidentifier,
    title_narrative,
    reportingorg_ref,
    reportingorg_narrative,
    reportingorg_type,
    activitystatus_code,
    plannedstart,
    plannedend,
    actualstart ,
    actualend,
    lastupdateddatetime,
    hierarchy,
    'https://d-portal.org/ctrack.html#view=act&aid=' || iatiidentifier AS "dportal_link"
FROM 
    ranked_activities
WHERE 
    row_num = 1