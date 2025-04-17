-- models/edges/financial_links.sql
-- Creates aggregated financial links between organisations and activities.
-- Each edge represents the total value for a specific transaction type 
-- flowing between an organisation and an activity, standardized to USD.

{{ 
  config(
    materialized='table'
  )
}}


WITH ProviderToActivity AS (
    -- Aggregate funds flowing FROM a provider organisation TO an activity
    SELECT
        t.providerorg_ref AS source_node_id,   -- Organisation providing funds
        t.iatiidentifier AS target_node_id,     -- Activity receiving funds
        'ORGANISATION' AS source_node_type,     -- Type of the source node
        'ACTIVITY' AS target_node_type,         -- Type of the target node
        t.transactiontype_code,                 -- Code for the transaction type (e.g., 'C', 'D')
        t.transactiontype_codename AS transaction_type_name, -- Name of the transaction type
        'USD' AS currency,                      -- Using standardized USD for all transactions
        SUM(t.value_usd) AS total_value_usd     -- Sum of USD values for this group
    FROM 
        {{ source('iati_postgres', 'transaction') }} t
    WHERE 
        t.providerorg_ref IS NOT NULL AND t.providerorg_ref <> ''
        AND t.iatiidentifier IS NOT NULL AND t.iatiidentifier <> ''
        AND t.transactiontype_code IS NOT NULL AND t.transactiontype_code <> ''
        AND t.value_usd IS NOT NULL            -- Ensure USD value exists
    GROUP BY
        t.providerorg_ref,
        t.iatiidentifier,
        t.transactiontype_code,
        t.transactiontype_codename

), ActivityToReceiver AS (
    -- Aggregate funds flowing FROM an activity TO a receiver organisation
    SELECT
        t.iatiidentifier AS source_node_id,     -- Activity providing funds
        t.receiverorg_ref AS target_node_id,    -- Organisation receiving funds
        'ACTIVITY' AS source_node_type,         -- Type of the source node
        'ORGANISATION' AS target_node_type,     -- Type of the target node
        t.transactiontype_code,
        t.transactiontype_codename AS transaction_type_name,
        'USD' AS currency,                      -- Using standardized USD for all transactions
        SUM(t.value_usd) AS total_value_usd     -- Sum of USD values for this group
    FROM 
        {{ source('iati_postgres', 'transaction') }} t
    WHERE 
        t.receiverorg_ref IS NOT NULL AND t.receiverorg_ref <> ''
        AND t.iatiidentifier IS NOT NULL AND t.iatiidentifier <> ''
        AND t.transactiontype_code IS NOT NULL AND t.transactiontype_code <> ''
        AND t.value_usd IS NOT NULL            -- Ensure USD value exists
    GROUP BY
        t.iatiidentifier,
        t.receiverorg_ref,
        t.transactiontype_code,
        t.transactiontype_codename
)

-- Combine the two directions of flow
SELECT * FROM ProviderToActivity
UNION ALL
SELECT * FROM ActivityToReceiver
ORDER BY
    source_node_id,
    target_node_id,
    transactiontype_code