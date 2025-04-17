-- models/phantom_organisations.sql
-- This DBT model provides a list of organisations that are referenced in the 
-- participatingorg, transaction, and organisation_recipientorgbudget tables,
-- but are not present in the organisation or otheridentifier tables.

{{
  config(
    materialized='table'
  )
}}

-- Consolidated query to find unique references not present in organisation or otheridentifier tables,
-- along with all associated narratives and boolean flags indicating the source table.
WITH UnmatchedReferences AS (
    -- Participating Org References
    SELECT
        p.ref AS reference,
        p.narrative AS narrative,
        TRUE AS is_participatingorg_ref,
        FALSE AS is_transaction_provider_ref,
        FALSE AS is_transaction_receiver_ref,
        FALSE AS is_orgbudget_recipient_ref
    FROM {{ source('iati_postgres', 'participatingorg') }} p
    WHERE
        p.ref IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM {{ source('iati_postgres', 'organisation') }} o WHERE o.organisationidentifier = p.ref)
        AND NOT EXISTS (SELECT 1 FROM {{ source('iati_postgres', 'otheridentifier') }} oi WHERE oi.ref = p.ref)
    UNION ALL
    -- Transaction Provider Org References
    SELECT
        t.providerorg_ref AS reference,
        t.providerorg_narrative AS narrative,
        FALSE AS is_participatingorg_ref,
        TRUE AS is_transaction_provider_ref,
        FALSE AS is_transaction_receiver_ref,
        FALSE AS is_orgbudget_recipient_ref
    FROM {{ source('iati_postgres', 'transaction') }} t
    WHERE
        t.providerorg_ref IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM {{ source('iati_postgres', 'organisation') }} o WHERE o.organisationidentifier = t.providerorg_ref)
        AND NOT EXISTS (SELECT 1 FROM {{ source('iati_postgres', 'otheridentifier') }} oi WHERE oi.ref = t.providerorg_ref)
    UNION ALL
    -- Transaction Receiver Org References
    SELECT
        t.receiverorg_ref AS reference,
        t.receiverorg_narrative AS narrative,
        FALSE AS is_participatingorg_ref,
        FALSE AS is_transaction_provider_ref,
        TRUE AS is_transaction_receiver_ref,
        FALSE AS is_orgbudget_recipient_ref
    FROM {{ source('iati_postgres', 'transaction') }} t
    WHERE
        t.receiverorg_ref IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM {{ source('iati_postgres', 'organisation') }} o WHERE o.organisationidentifier = t.receiverorg_ref)
        AND NOT EXISTS (SELECT 1 FROM {{ source('iati_postgres', 'otheridentifier') }} oi WHERE oi.ref = t.receiverorg_ref)
    UNION ALL
    -- Organisation Recipient Org Budget References
    SELECT
        orb.recipientorg_ref AS reference,
        orb.recipientorg_narrative AS narrative,
        FALSE AS is_participatingorg_ref,
        FALSE AS is_transaction_provider_ref,
        FALSE AS is_transaction_receiver_ref,
        TRUE AS is_orgbudget_recipient_ref
    FROM {{ source('iati_postgres', 'organisation_recipientorgbudget') }} orb
    WHERE
        orb.recipientorg_ref IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM {{ source('iati_postgres', 'organisation') }} o WHERE o.organisationidentifier = orb.recipientorg_ref)
        AND NOT EXISTS (SELECT 1 FROM {{ source('iati_postgres', 'otheridentifier') }} oi WHERE oi.ref = orb.recipientorg_ref)
)
SELECT
    reference,
    -- Aggregate distinct, non-empty narratives into an array for each reference
    array_agg(DISTINCT narrative) FILTER (WHERE narrative IS NOT NULL AND narrative <> '') AS distinct_narratives,
    -- Aggregate boolean flags: TRUE if the reference appeared in that source at least once and was unresolved
    BOOL_OR(is_participatingorg_ref) AS phantom_in_participatingorg,
    BOOL_OR(is_transaction_provider_ref) AS phantom_in_transaction_provider,
    BOOL_OR(is_transaction_receiver_ref) AS phantom_in_transaction_receiver,
    BOOL_OR(is_orgbudget_recipient_ref) AS phantom_in_orgbudget_recipient
FROM UnmatchedReferences
-- Exclude rows where the reference itself might be null (unlikely here, but safe)
WHERE reference IS NOT NULL
GROUP BY reference
ORDER BY reference
