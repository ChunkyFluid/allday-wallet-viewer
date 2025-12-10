-- snowflake_diagnose_holdings.sql
-- Diagnostic queries to understand what's in FLOW_ONCHAIN_CORE_DATA for a specific wallet

USE DATABASE ALLDAY_VIEWER;
USE SCHEMA ALLDAY;

-- Replace '0x7541bafd155b683e' with your wallet address in the queries below

-- Count total Deposit events for this wallet
SELECT 
  'Total Deposit events' AS metric,
  COUNT(*) AS count
FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay'
  AND EVENT_TYPE = 'Deposit'
  AND TX_SUCCEEDED = true
  AND BLOCK_TIMESTAMP >= '2021-01-01'
  AND LOWER(EVENT_DATA:to::STRING) = LOWER('0x7541bafd155b683e');

-- Count unique NFTs deposited (latest deposit per NFT)
SELECT 
  'Unique NFTs with deposits' AS metric,
  COUNT(DISTINCT EVENT_DATA:id::STRING) AS count
FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay'
  AND EVENT_TYPE = 'Deposit'
  AND TX_SUCCEEDED = true
  AND BLOCK_TIMESTAMP >= '2021-01-01'
  AND LOWER(EVENT_DATA:to::STRING) = LOWER('0x7541bafd155b683e');

-- Count NFTs that were deposited but later withdrawn
SELECT 
  'NFTs deposited then withdrawn' AS metric,
  COUNT(DISTINCT d.EVENT_DATA:id::STRING) AS count
FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS d
INNER JOIN FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS w
  ON d.EVENT_DATA:id::STRING = w.EVENT_DATA:id::STRING
  AND w.EVENT_TYPE = 'Withdraw'
  AND w.TX_SUCCEEDED = true
  AND w.BLOCK_TIMESTAMP >= d.BLOCK_TIMESTAMP
WHERE d.EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay'
  AND d.EVENT_TYPE = 'Deposit'
  AND d.TX_SUCCEEDED = true
  AND d.BLOCK_TIMESTAMP >= '2021-01-01'
  AND LOWER(d.EVENT_DATA:to::STRING) = LOWER('0x7541bafd155b683e')
  AND LOWER(w.EVENT_DATA:from::STRING) = LOWER('0x7541bafd155b683e');

-- Count NFTs currently locked for this wallet
SELECT 
  'NFTs currently locked' AS metric,
  COUNT(DISTINCT l.EVENT_DATA:id::STRING) AS count
FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS l
LEFT JOIN FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS u
  ON l.EVENT_DATA:id::STRING = u.EVENT_DATA:id::STRING
  AND u.EVENT_TYPE = 'NFTUnlocked'
  AND u.TX_SUCCEEDED = true
  AND u.BLOCK_TIMESTAMP >= l.BLOCK_TIMESTAMP
WHERE l.EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker'
  AND l.EVENT_TYPE = 'NFTLocked'
  AND l.TX_SUCCEEDED = true
  AND l.BLOCK_TIMESTAMP >= '2021-01-01'
  AND LOWER(l.EVENT_DATA:to::STRING) = LOWER('0x7541bafd155b683e')
  AND u.EVENT_DATA:id::STRING IS NULL;

-- What the backfill script would produce for this wallet
WITH burned_nfts AS (
  SELECT EVENT_DATA:id::STRING AS NFT_ID
  FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
  WHERE EVENT_TYPE = 'MomentNFTBurned'
    AND TX_SUCCEEDED = true
    AND BLOCK_TIMESTAMP >= '2021-01-01'
),
deposit_events AS (
  SELECT
    EVENT_DATA:id::STRING AS NFT_ID,
    LOWER(EVENT_DATA:to::STRING) AS Wallet_Address,
    BLOCK_TIMESTAMP
  FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
  WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay'
    AND EVENT_TYPE = 'Deposit'
    AND TX_SUCCEEDED = true
    AND BLOCK_TIMESTAMP >= '2021-01-01'
    AND LOWER(EVENT_DATA:to::STRING) = LOWER('0x7541bafd155b683e')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
),
withdraw_events AS (
  SELECT
    EVENT_DATA:id::STRING AS NFT_ID,
    BLOCK_TIMESTAMP
  FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
  WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay'
    AND EVENT_TYPE = 'Withdraw'
    AND TX_SUCCEEDED = true
    AND BLOCK_TIMESTAMP >= '2021-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
),
unlocked_nfts AS (
  SELECT d.NFT_ID, d.Wallet_Address, d.BLOCK_TIMESTAMP AS last_event_ts
  FROM deposit_events AS d
  LEFT JOIN withdraw_events AS w
    ON d.NFT_ID = w.NFT_ID
    AND w.BLOCK_TIMESTAMP >= d.BLOCK_TIMESTAMP
  WHERE w.NFT_ID IS NULL
    AND d.NFT_ID NOT IN (SELECT NFT_ID FROM burned_nfts)
),
locked_events AS (
  SELECT
    EVENT_DATA:id::STRING AS NFT_ID,
    LOWER(EVENT_DATA:to::STRING) AS Wallet_Address,
    BLOCK_TIMESTAMP
  FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
  WHERE EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker'
    AND EVENT_TYPE = 'NFTLocked'
    AND TX_SUCCEEDED = true
    AND BLOCK_TIMESTAMP >= '2021-01-01'
    AND LOWER(EVENT_DATA:to::STRING) = LOWER('0x7541bafd155b683e')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
),
unlocked_events AS (
  SELECT
    BLOCK_TIMESTAMP,
    EVENT_DATA:id::STRING AS NFT_ID
  FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
  WHERE EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker'
    AND EVENT_TYPE = 'NFTUnlocked'
    AND TX_SUCCEEDED = true
    AND BLOCK_TIMESTAMP >= '2021-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
),
locked_nfts AS (
  SELECT l.NFT_ID, l.Wallet_Address, l.BLOCK_TIMESTAMP AS last_event_ts
  FROM locked_events AS l
  LEFT JOIN unlocked_events AS u
    ON l.NFT_ID = u.NFT_ID
    AND u.BLOCK_TIMESTAMP >= l.BLOCK_TIMESTAMP
  WHERE u.NFT_ID IS NULL
    AND l.NFT_ID NOT IN (SELECT NFT_ID FROM burned_nfts)
),
locked_ids AS (
  SELECT DISTINCT NFT_ID FROM locked_nfts
),
unlocked_nfts_filtered AS (
  SELECT NFT_ID, Wallet_Address, last_event_ts
  FROM unlocked_nfts
  WHERE NFT_ID NOT IN (SELECT NFT_ID FROM locked_ids)
),
all_holdings AS (
  SELECT NFT_ID, Wallet_Address, FALSE AS is_locked, last_event_ts FROM unlocked_nfts_filtered
  UNION
  SELECT NFT_ID, Wallet_Address, TRUE AS is_locked, last_event_ts FROM locked_nfts
)
SELECT 
  'Expected count from backfill script' AS metric,
  COUNT(*) AS count
FROM all_holdings;
