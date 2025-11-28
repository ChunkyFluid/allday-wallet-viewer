-- FIND YOUR MISSING ~620 MOMENTS
-- Run each query to understand where they are

-- 1. Simple calculation: deposits - withdraws (not accounting for re-deposits)
SELECT 
    (SELECT COUNT(*) FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
     WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
       AND event_type = 'Deposit'
       AND LOWER(event_data:to::STRING) = '0x7541bafd155b683e'
       AND tx_succeeded = TRUE) AS total_deposits,
    (SELECT COUNT(*) FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
     WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
       AND event_type = 'Withdraw'
       AND LOWER(event_data:from::STRING) = '0x7541bafd155b683e'
       AND tx_succeeded = TRUE) AS total_withdraws,
    (SELECT COUNT(*) FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
     WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
       AND event_type = 'Deposit'
       AND LOWER(event_data:to::STRING) = '0x7541bafd155b683e'
       AND tx_succeeded = TRUE) -
    (SELECT COUNT(*) FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
     WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
       AND event_type = 'Withdraw'
       AND LOWER(event_data:from::STRING) = '0x7541bafd155b683e'
       AND tx_succeeded = TRUE) AS simple_net;

-- 2. Count UNIQUE NFTs ever deposited to your wallet
SELECT COUNT(DISTINCT event_data:id::STRING) AS unique_nfts_ever_deposited
FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
  AND event_type = 'Deposit'
  AND LOWER(event_data:to::STRING) = '0x7541bafd155b683e'
  AND tx_succeeded = TRUE;

-- 3. Count UNIQUE NFTs ever withdrawn from your wallet  
SELECT COUNT(DISTINCT event_data:id::STRING) AS unique_nfts_ever_withdrawn
FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
  AND event_type = 'Withdraw'
  AND LOWER(event_data:from::STRING) = '0x7541bafd155b683e'
  AND tx_succeeded = TRUE;

-- 4. CORRECT CALCULATION: For each NFT, check if YOU are the current owner
-- (Latest deposit event globally goes TO your wallet)
WITH all_transfers AS (
    SELECT
        event_data:id::STRING AS nft_id,
        CASE 
            WHEN event_type = 'Deposit' THEN LOWER(event_data:to::STRING)
            WHEN event_type = 'Withdraw' THEN LOWER(event_data:from::STRING)
        END AS wallet,
        event_type,
        block_timestamp,
        block_height,
        event_index
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type IN ('Deposit', 'Withdraw')
      AND tx_succeeded = TRUE
      AND block_timestamp >= '2021-01-01'
),
-- Get the LATEST event for each NFT
latest_event_per_nft AS (
    SELECT *
    FROM all_transfers
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY nft_id 
        ORDER BY block_timestamp DESC, block_height DESC, event_index DESC
    ) = 1
),
-- Get burned NFTs
burned AS (
    SELECT event_data:id::STRING AS nft_id
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
    WHERE event_type = 'MomentNFTBurned'
      AND tx_succeeded = TRUE
)
-- NFTs where the latest event is a Deposit TO your wallet
SELECT COUNT(*) AS your_current_holdings
FROM latest_event_per_nft l
WHERE l.event_type = 'Deposit'
  AND l.wallet = '0x7541bafd155b683e'
  AND l.nft_id NOT IN (SELECT nft_id FROM burned WHERE nft_id IS NOT NULL);

-- 5. Check if some of your NFTs are currently "locked" in challenges
:

-- 6. Check NFTs currently listed for sale (in marketplace escrow)
-- Marketplace addresses: 0xb87165dd28b90b6a (TopShot Market), 0x4dfd62c88d1b6462 (NFTStorefront)
WITH your_nfts_ever AS (
    SELECT DISTINCT event_data:id::STRING AS nft_id
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'Deposit'
      AND LOWER(event_data:to::STRING) = '0x7541bafd155b683e'
      AND tx_succeeded = TRUE
),
latest_location AS (
    SELECT 
        event_data:id::STRING AS nft_id,
        CASE 
            WHEN event_type = 'Deposit' THEN LOWER(event_data:to::STRING)
            ELSE NULL
        END AS current_holder,
        event_type
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type IN ('Deposit', 'Withdraw')
      AND tx_succeeded = TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event_data:id::STRING 
        ORDER BY block_timestamp DESC, block_height DESC, event_index DESC
    ) = 1
)
SELECT 
    current_holder,
    COUNT(*) AS nft_count
FROM latest_location l
JOIN your_nfts_ever y ON l.nft_id = y.nft_id
WHERE l.event_type = 'Deposit'
GROUP BY current_holder
ORDER BY nft_count DESC
LIMIT 20;

