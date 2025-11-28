-- =====================================================
-- DIAGNOSTIC: Check raw blockchain data for your wallet
-- Run each query separately to understand what's happening
-- =====================================================

-- 1. Check total Deposit events for your wallet (should be ~2840+)
SELECT COUNT(*) AS total_deposits_ever
FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
  AND event_type = 'Deposit'
  AND LOWER(event_data:to::STRING) = '0x7541bafd155b683e'
  AND tx_succeeded = TRUE;

-- 2. Check total Withdraw events from your wallet
SELECT COUNT(*) AS total_withdraws_ever
FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
  AND event_type = 'Withdraw'
  AND LOWER(event_data:from::STRING) = '0x7541bafd155b683e'
  AND tx_succeeded = TRUE;

-- 3. Check the latest event timestamp in FLOW_ONCHAIN_CORE_DATA
SELECT MAX(block_timestamp) AS latest_event_timestamp
FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay';

-- 4. Check recent deposits to your wallet (last 30 days)
SELECT COUNT(*) AS deposits_last_30_days
FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
  AND event_type = 'Deposit'
  AND LOWER(event_data:to::STRING) = '0x7541bafd155b683e'
  AND tx_succeeded = TRUE
  AND block_timestamp >= DATEADD(day, -30, CURRENT_TIMESTAMP());

-- 5. Check the current holdings table count
SELECT COUNT(*) AS current_holdings_table_count
FROM NFL_ALLDAY.ANALYTICS.ALLDAY_WALLET_HOLDINGS_CURRENT
WHERE wallet_address = '0x7541bafd155b683e';

-- 6. FULL CALCULATION: What SHOULD your count be?
-- This calculates from scratch using raw events
WITH your_deposits AS (
    SELECT 
        event_data:id::STRING AS nft_id,
        block_timestamp,
        event_index
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'Deposit'
      AND LOWER(event_data:to::STRING) = '0x7541bafd155b683e'
      AND tx_succeeded = TRUE
),
your_withdraws AS (
    SELECT 
        event_data:id::STRING AS nft_id,
        block_timestamp,
        event_index
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'Withdraw'
      AND LOWER(event_data:from::STRING) = '0x7541bafd155b683e'
      AND tx_succeeded = TRUE
),
burned AS (
    SELECT event_data:id::STRING AS nft_id
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
    WHERE event_type = 'MomentNFTBurned'
      AND tx_succeeded = TRUE
),
-- NFTs you received that weren't later withdrawn or burned
current_holdings AS (
    SELECT d.nft_id
    FROM your_deposits d
    LEFT JOIN your_withdraws w 
        ON d.nft_id = w.nft_id 
        AND (w.block_timestamp > d.block_timestamp 
             OR (w.block_timestamp = d.block_timestamp AND w.event_index > d.event_index))
    WHERE w.nft_id IS NULL
      AND d.nft_id NOT IN (SELECT nft_id FROM burned)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY d.nft_id ORDER BY d.block_timestamp DESC, d.event_index DESC) = 1
)
SELECT COUNT(*) AS calculated_current_holdings
FROM current_holdings;

