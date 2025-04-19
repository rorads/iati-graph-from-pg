-- models/edges/funds_links.sql
-- Creates a simplified "funds" relationship between activities, aggregating all transaction types
-- into a single directional relationship that shows the flow of funds between activities.

{{ 
  config(
    materialized='table'
  )
}}

WITH ActivityFinancialLinks AS (
    -- Extract all activity-to-activity financial links from the transaction table
    SELECT
        -- Source and target identifiers
        t.providerorg_provideractivityid AS source_node_id,  -- Activity providing the funds
        t.iatiidentifier AS target_node_id,                  -- Activity receiving the funds
        
        -- Always activity nodes for this relationship
        'ACTIVITY' AS source_node_type,
        'ACTIVITY' AS target_node_type,
        
        -- Standardized currency
        'USD' AS currency,
        
        -- Sum of all transactions in USD 
        SUM(t.value_usd) AS total_value_usd
    FROM 
        {{ source('iati_postgres', 'transaction') }} t
    WHERE 
        -- Ensure it's an activity-to-activity link
        t.providerorg_provideractivityid IS NOT NULL 
        AND t.providerorg_provideractivityid <> ''
        AND t.iatiidentifier IS NOT NULL 
        AND t.iatiidentifier <> ''
        
        -- Only include transaction types that represent fund flows
        AND t.transactiontype_code IN ('1', '2', '3', '4', '11')  -- Incoming Funds, Outgoing Commitment, Disbursement, Expenditure, Incoming Commitment
        AND t.value_usd IS NOT NULL
    GROUP BY
        t.providerorg_provideractivityid,
        t.iatiidentifier
)

SELECT
    source_node_id,
    target_node_id,
    source_node_type,
    target_node_type,
    currency,
    total_value_usd
FROM 
    ActivityFinancialLinks
WHERE
    total_value_usd > 0  -- Only include positive financial flows 