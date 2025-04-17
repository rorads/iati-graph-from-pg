-- models/nodes/phantom_activities.sql
-- This DBT model identifies activity identifiers that are referenced
-- in various tables but do not exist in the main 'activity' table.

{{ 
  config(
    materialized='table'
  )
}}

WITH ReferencedActivities AS (

    -- From participatingorg.activityid
    SELECT
        activityid AS reference,
        'participatingorg.activityid' AS source_column,
        iatiidentifier AS source_activity_id -- The activity where the reference was found
    FROM {{ source('iati_postgres', 'participatingorg') }} -- Adjust source name if needed
    WHERE activityid IS NOT NULL AND activityid <> ''

    UNION ALL

    -- From transaction.providerorg_provideractivityid
    SELECT
        providerorg_provideractivityid AS reference,
        'transaction.providerorg_provideractivityid' AS source_column,
        iatiidentifier AS source_activity_id
    FROM {{ source('iati_postgres', 'transaction') }}
    WHERE providerorg_provideractivityid IS NOT NULL AND providerorg_provideractivityid <> ''

    UNION ALL

    -- From transaction.receiverorg_receiveractivityid
    SELECT
        receiverorg_receiveractivityid AS reference,
        'transaction.receiverorg_receiveractivityid' AS source_column,
        iatiidentifier AS source_activity_id
    FROM {{ source('iati_postgres', 'transaction') }}
    WHERE receiverorg_receiveractivityid IS NOT NULL AND receiverorg_receiveractivityid <> ''

    UNION ALL

    -- From relatedactivity.ref
    SELECT
        ref AS reference,
        'relatedactivity.ref' AS source_column,
        iatiidentifier AS source_activity_id
    FROM {{ source('iati_postgres', 'relatedactivity') }}
    WHERE ref IS NOT NULL AND ref <> ''

    UNION ALL

    -- From planneddisbursement.providerorg_provideractivityid
    SELECT
        providerorg_provideractivityid AS reference,
        'planneddisbursement.providerorg_provideractivityid' AS source_column,
        iatiidentifier AS source_activity_id
    FROM {{ source('iati_postgres', 'planneddisbursement') }}
    WHERE providerorg_provideractivityid IS NOT NULL AND providerorg_provideractivityid <> ''

    UNION ALL

    -- From planneddisbursement.receiverorg_receiveractivityid
    SELECT
        receiverorg_receiveractivityid AS reference,
        'planneddisbursement.receiverorg_receiveractivityid' AS source_column,
        iatiidentifier AS source_activity_id
    FROM {{ source('iati_postgres', 'planneddisbursement') }}
    WHERE receiverorg_receiveractivityid IS NOT NULL AND receiverorg_receiveractivityid <> ''

),
ExistingActivities AS (
    -- Get all unique, non-null/empty activity identifiers from the main activity table
    SELECT DISTINCT iatiidentifier
    FROM {{ source('iati_postgres', 'activity') }}
    WHERE iatiidentifier IS NOT NULL AND iatiidentifier <> ''
)
-- Select distinct referenced activities that do not exist in the main activity table
SELECT DISTINCT
    ra.reference AS phantom_activity_identifier,
    ra.source_column,
    ra.source_activity_id
FROM ReferencedActivities ra
LEFT JOIN ExistingActivities ea ON ra.reference = ea.iatiidentifier
WHERE ea.iatiidentifier IS NULL
ORDER BY ra.reference, ra.source_activity_id