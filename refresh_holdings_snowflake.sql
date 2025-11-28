-- =====================================================
-- REFRESH ALLDAY_WALLET_HOLDINGS_CURRENT TABLE
-- Run this in Snowflake to update wallet holdings
-- =====================================================

-- Step 1: Create or replace the table with fresh data from raw blockchain events
CREATE OR REPLACE TABLE NFL_ALLDAY.ANALYTICS.ALLDAY_WALLET_HOLDINGS_CURRENT AS

WITH burned_nfts AS (
    SELECT event_data:id::STRING AS nft_id
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
    WHERE event_type = 'MomentNFTBurned'
      AND tx_succeeded = TRUE
      AND block_timestamp >= '2021-01-01'
),

-- Get all Deposit events
deposit_events AS (
    SELECT
        event_data:id::STRING AS nft_id,
        LOWER(event_data:to::STRING) AS wallet_address,
        block_timestamp,
        block_height,
        event_index,
        'Deposit' AS event_type
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'Deposit'
      AND tx_succeeded = TRUE
      AND block_timestamp >= '2021-01-01'
),

-- Get all Withdraw events  
withdraw_events AS (
    SELECT
        event_data:id::STRING AS nft_id,
        LOWER(event_data:from::STRING) AS wallet_address,
        block_timestamp,
        block_height,
        event_index,
        'Withdraw' AS event_type
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'Withdraw'
      AND tx_succeeded = TRUE
      AND block_timestamp >= '2021-01-01'
),

-- Combine all events
all_events AS (
    SELECT * FROM deposit_events
    UNION ALL
    SELECT * FROM withdraw_events
),

-- For each NFT, find the latest event
latest_events AS (
    SELECT
        nft_id,
        wallet_address,
        block_timestamp AS last_event_ts,
        event_type,
        ROW_NUMBER() OVER (
            PARTITION BY nft_id
            ORDER BY block_timestamp DESC, block_height DESC, event_index DESC
        ) AS rn
    FROM all_events
),

-- Get locked NFTs
locked_events AS (
    SELECT
        event_data:id::STRING AS nft_id,
        LOWER(event_data:to::STRING) AS wallet_address,
        block_timestamp,
        block_height
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.b6f2481eba4df97b.NFTLocker'
      AND event_type = 'NFTLocked'
      AND tx_succeeded = TRUE
      AND block_timestamp >= '2021-01-01'
),

unlocked_events AS (
    SELECT
        event_data:id::STRING AS nft_id,
        block_timestamp,
        block_height
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.b6f2481eba4df97b.NFTLocker'
      AND event_type = 'NFTUnlocked'
      AND tx_succeeded = TRUE
      AND block_timestamp >= '2021-01-01'
),

-- Determine if NFT is currently locked
lock_status AS (
    SELECT
        l.nft_id,
        TRUE AS is_locked
    FROM locked_events l
    LEFT JOIN unlocked_events u 
        ON l.nft_id = u.nft_id 
        AND u.block_timestamp > l.block_timestamp
    WHERE u.nft_id IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY l.nft_id ORDER BY l.block_timestamp DESC) = 1
)

-- Final: Current holdings (NFTs where latest event is Deposit, not burned)
SELECT
    le.wallet_address,
    le.nft_id,
    COALESCE(ls.is_locked, FALSE) AS is_locked,
    le.last_event_ts
FROM latest_events le
LEFT JOIN lock_status ls ON le.nft_id = ls.nft_id
WHERE le.rn = 1
  AND le.event_type = 'Deposit'
  AND le.wallet_address IS NOT NULL
  AND le.nft_id NOT IN (SELECT nft_id FROM burned_nfts);

-- Show result count
SELECT COUNT(*) AS total_holdings FROM NFL_ALLDAY.ANALYTICS.ALLDAY_WALLET_HOLDINGS_CURRENT;

-- Check a specific wallet (your wallet)
SELECT COUNT(*) AS your_wallet_count 
FROM NFL_ALLDAY.ANALYTICS.ALLDAY_WALLET_HOLDINGS_CURRENT 
WHERE wallet_address = '0x7541bafd155b683e';

