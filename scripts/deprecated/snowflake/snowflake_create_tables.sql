-- ============================================================
-- CREATE MATERIALIZED TABLES (MUCH FASTER THAN VIEWS!)
-- Run this to create fast-query tables instead of slow views
-- ============================================================

USE DATABASE ALLDAY_VIEWER;
USE SCHEMA ALLDAY;

-- Drop existing tables first
DROP TABLE IF EXISTS ALLDAY_CORE_NFT_METADATA;
DROP TABLE IF EXISTS ALLDAY_WALLET_HOLDINGS_CURRENT;

-- ============================================================
-- TABLE 1: ALLDAY_CORE_NFT_METADATA
-- ============================================================

CREATE OR REPLACE TABLE ALLDAY_CORE_NFT_METADATA AS
WITH burned_nfts AS (
  SELECT
    EVENT_DATA:id::STRING as NFT_ID
  FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
  WHERE 
    EVENT_TYPE='MomentNFTBurned' AND
    TX_SUCCEEDED = true AND
    BLOCK_TIMESTAMP >= '2021-01-01'
),

minted_nfts AS (
    SELECT 
        event_data:id::string AS nft_id,
        event_data:editionID::string AS edition_id,
        event_data:serialNumber::string AS serial_number,
        tx_id,
        block_timestamp
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'MomentNFTMinted'
      AND tx_succeeded = TRUE
),

edition_metadata AS (
    SELECT 
        event_data:id::string AS edition_id,
        event_data:maxMintSize::string AS max_mint_size,
        event_data:playID::string AS play_id,
        event_data:seriesID::string AS series_id,
        event_data:setID::string AS set_id,
        event_data:tier::string AS tier
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'EditionCreated'
      AND tx_succeeded = TRUE
),

play_metadata AS (
    SELECT 
        event_data:id::STRING as play_id,
        MAX(CASE WHEN metadata.value:key.value::STRING = 'playerFirstName' 
                 THEN metadata.value:value.value::STRING END) as first_name,
        MAX(CASE WHEN metadata.value:key.value::STRING = 'playerLastName' 
                 THEN metadata.value:value.value::STRING END) as last_name,
        MAX(CASE WHEN metadata.value:key.value::STRING = 'teamName' 
                 THEN metadata.value:value.value::STRING END) as team_name,
        MAX(CASE WHEN metadata.value:key.value::STRING = 'playerPosition' 
                 THEN metadata.value:value.value::STRING END) as position,
        MAX(CASE WHEN metadata.value:key.value::STRING = 'playerNumber' 
                 THEN metadata.value:value.value::STRING END) as jersey_number
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS,
         TABLE(FLATTEN(event_data:metadata)) as metadata
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'PlayCreated'
      AND tx_succeeded = TRUE
    GROUP BY event_data:id::STRING
),

series_names AS (
    SELECT 
        event_data:id::string AS series_id,
        event_data:name::string AS series_name
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'SeriesCreated'
      AND tx_succeeded = TRUE
),

set_names AS (
    SELECT 
        event_data:id::string AS set_id,
        event_data:name::string AS set_name
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'SetCreated'
      AND tx_succeeded = TRUE
)

SELECT 
    m.nft_id,
    m.edition_id,
    e.max_mint_size,
    e.play_id,
    e.series_id,
    s.series_name,
    e.set_id,
    st.set_name,
    e.tier,
    m.serial_number,
    p.first_name,
    p.last_name,
    p.team_name,
    p.position,
    p.jersey_number
FROM minted_nfts m
LEFT JOIN edition_metadata e ON m.edition_id = e.edition_id
LEFT JOIN play_metadata p ON e.play_id = p.play_id
LEFT JOIN series_names s ON e.series_id = s.series_id
LEFT JOIN set_names st ON e.set_id = st.set_id
WHERE m.nft_id IS NOT NULL
  AND m.nft_id NOT IN (SELECT nft_id FROM burned_nfts);

SELECT 'ALLDAY_CORE_NFT_METADATA created with ' || COUNT(*) || ' rows' as status FROM ALLDAY_CORE_NFT_METADATA;


-- ============================================================
-- TABLE 2: ALLDAY_WALLET_HOLDINGS_CURRENT
-- FIXED: Better logic to track current NFT owners
-- ============================================================

CREATE OR REPLACE TABLE ALLDAY_WALLET_HOLDINGS_CURRENT AS
WITH burned_nfts AS (
  SELECT EVENT_DATA:id::STRING as NFT_ID
  FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
  WHERE EVENT_TYPE='MomentNFTBurned' 
    AND TX_SUCCEEDED = true 
    AND BLOCK_TIMESTAMP >= '2021-01-01'
),

-- All deposit and withdraw events combined
all_transfers AS (
  -- Deposits: NFT arriving at a wallet
  SELECT 
    EVENT_DATA:id::STRING as NFT_ID,
    EVENT_DATA:to::STRING as wallet_address,
    'DEPOSIT' as event_type,
    BLOCK_HEIGHT,
    BLOCK_TIMESTAMP
  FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
  WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay'
    AND EVENT_TYPE = 'Deposit'
    AND TX_SUCCEEDED = true
    AND BLOCK_TIMESTAMP >= '2021-01-01'
    AND EVENT_DATA:to::STRING IS NOT NULL
  
  UNION ALL
  
  -- Withdraws: NFT leaving a wallet
  SELECT 
    EVENT_DATA:id::STRING as NFT_ID,
    EVENT_DATA:from::STRING as wallet_address,
    'WITHDRAW' as event_type,
    BLOCK_HEIGHT,
    BLOCK_TIMESTAMP
  FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
  WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay'
    AND EVENT_TYPE = 'Withdraw'
    AND TX_SUCCEEDED = true
    AND BLOCK_TIMESTAMP >= '2021-01-01'
    AND EVENT_DATA:from::STRING IS NOT NULL
),

-- For each NFT, get the most recent event
-- If most recent is DEPOSIT, that wallet owns it
-- If most recent is WITHDRAW, the NFT left that wallet
latest_transfer AS (
  SELECT 
    NFT_ID,
    wallet_address,
    event_type,
    BLOCK_HEIGHT,
    BLOCK_TIMESTAMP
  FROM all_transfers
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY NFT_ID 
    ORDER BY BLOCK_HEIGHT DESC, 
             -- If same block, WITHDRAW happens before DEPOSIT (NFT moves)
             CASE WHEN event_type = 'DEPOSIT' THEN 1 ELSE 0 END DESC
  ) = 1
),

-- Current holders: NFTs where the last event was a DEPOSIT
current_holders AS (
  SELECT 
    NFT_ID,
    wallet_address,
    BLOCK_TIMESTAMP as last_event_ts
  FROM latest_transfer
  WHERE event_type = 'DEPOSIT'
    AND NFT_ID NOT IN (SELECT NFT_ID FROM burned_nfts)
),

-- Lock events
lock_events AS (
  SELECT 
    EVENT_DATA:id::STRING as NFT_ID, 
    EVENT_DATA:to::STRING as locker_wallet,
    BLOCK_TIMESTAMP,
    BLOCK_HEIGHT
  FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
  WHERE EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker'
    AND EVENT_TYPE = 'NFTLocked'
    AND TX_SUCCEEDED = true
    AND BLOCK_TIMESTAMP >= '2021-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
),

-- Unlock events
unlock_events AS (
  SELECT 
    EVENT_DATA:id::STRING as NFT_ID,
    BLOCK_TIMESTAMP,
    BLOCK_HEIGHT
  FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
  WHERE EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker'
    AND EVENT_TYPE = 'NFTUnlocked'
    AND TX_SUCCEEDED = true
    AND BLOCK_TIMESTAMP >= '2021-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
),

-- Determine locked status: locked if last lock is after last unlock (or no unlock)
locked_status AS (
  SELECT 
    l.NFT_ID,
    l.locker_wallet,
    CASE 
      WHEN u.NFT_ID IS NULL THEN TRUE  -- Never unlocked
      WHEN l.BLOCK_HEIGHT > u.BLOCK_HEIGHT THEN TRUE  -- Locked after last unlock
      ELSE FALSE
    END as is_locked
  FROM lock_events l
  LEFT JOIN unlock_events u ON l.NFT_ID = u.NFT_ID
)

-- Final result: current holders with locked status
SELECT 
  h.wallet_address,
  h.NFT_ID,
  COALESCE(ls.is_locked, FALSE) as is_locked,
  h.last_event_ts
FROM current_holders h
LEFT JOIN locked_status ls ON h.NFT_ID = ls.NFT_ID;

SELECT 'ALLDAY_WALLET_HOLDINGS_CURRENT created with ' || COUNT(*) || ' rows' as status FROM ALLDAY_WALLET_HOLDINGS_CURRENT;

-- Show breakdown
SELECT 
  'Total NFTs: ' || COUNT(*) || ', Locked: ' || SUM(CASE WHEN is_locked THEN 1 ELSE 0 END) || ', Unlocked: ' || SUM(CASE WHEN NOT is_locked THEN 1 ELSE 0 END) as status
FROM ALLDAY_WALLET_HOLDINGS_CURRENT;

-- Check a specific wallet (NicelyDone)
SELECT 
  'NicelyDone (0xb7700366fa738a43): ' || COUNT(*) || ' total, ' || 
  SUM(CASE WHEN is_locked THEN 1 ELSE 0 END) || ' locked, ' ||
  SUM(CASE WHEN NOT is_locked THEN 1 ELSE 0 END) || ' unlocked' as status
FROM ALLDAY_WALLET_HOLDINGS_CURRENT 
WHERE LOWER(wallet_address) = '0xb7700366fa738a43';
