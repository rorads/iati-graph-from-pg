-- models/published_organisations.sql
-- This DBT model provides a canonical version of each organisation, eliminating duplicates
-- by selecting the most recently updated version of each organisation.

{{
  config(
    materialized='table'
  )
}}


WITH ranked_organisations AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY organisationidentifier 
            ORDER BY 
                -- Use the most recent version based on lastupdateddatetime if available
                lastupdateddatetime DESC NULLS LAST,
                -- If no lastupdateddatetime, fall back to dataset name which may contain date information
                dataset DESC
        ) AS row_num
    FROM 
        {{ source('iati_postgres', 'organisation') }}
    WHERE 
        organisationidentifier IS NOT NULL
)

SELECT 
    organisationidentifier,
    name_narrative,
    hierarchy,
    reportingorg_ref,
    'https://d-portal.org/ctrack.html?publisher=' || organisationidentifier AS dportal_link
FROM 
    ranked_organisations
WHERE 
    row_num = 1