-- =====================================================
-- SNOWFLAKE FULL REFRESH SCRIPT
-- Run these in Snowflake to refresh all analytics tables
-- =====================================================

-- =====================================================
-- STEP 1: REFRESH ALLDAY_CORE_NFT_METADATA
-- Based on metadata_query.sql
-- =====================================================

CREATE OR REPLACE TABLE NFL_ALLDAY.ANALYTICS.ALLDAY_CORE_NFT_METADATA AS

WITH minted_nfts AS (
    SELECT 
        event_data:id::string        AS nft_id,
        event_data:editionID::string AS edition_id,
        TRY_TO_NUMBER(event_data:serialNumber::string) AS serial_number,
        block_timestamp              AS minted_ts
    FROM flow_onchain_core_data.core.fact_events 
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'MomentNFTMinted'
      AND tx_succeeded = TRUE
),

edition_metadata AS (
    SELECT 
        event_data:id::string        AS edition_id,
        event_data:playID::string    AS play_id,
        event_data:seriesID::string  AS series_id,
        event_data:setID::string     AS set_id,
        event_data:tier::string      AS tier,
        TRY_TO_NUMBER(event_data:maxMintSize::string)  AS max_mint_size
    FROM flow_onchain_core_data.core.fact_events
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'EditionCreated'
      AND tx_succeeded = TRUE
),

play_metadata AS (
    SELECT 
      event_data:id::STRING AS play_id,
      MAX(CASE WHEN metadata.value:key.value::STRING = 'playerFirstName' THEN metadata.value:value.value::STRING END) AS first_name,
      MAX(CASE WHEN metadata.value:key.value::STRING = 'playerLastName'  THEN metadata.value:value.value::STRING END) AS last_name,
      MAX(CASE WHEN metadata.value:key.value::STRING = 'teamName'       THEN metadata.value:value.value::STRING END) AS team_name,
      MAX(CASE WHEN metadata.value:key.value::STRING = 'playerPosition' THEN metadata.value:value.value::STRING END) AS position,
      MAX(CASE WHEN metadata.value:key.value::STRING = 'playerNumber'   THEN metadata.value:value.value::STRING END) AS jersey_number
    FROM flow_onchain_core_data.core.fact_events,
         TABLE(FLATTEN(event_data:metadata)) AS metadata
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'PlayCreated'
      AND tx_succeeded = TRUE
    GROUP BY event_data:id::STRING
),

series_names AS (
    SELECT 
        event_data:id::string   AS series_id,
        event_data:name::string AS series_name
    FROM flow_onchain_core_data.core.fact_events
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'SeriesCreated'
      AND tx_succeeded = TRUE
),

set_names AS (
    SELECT 
        event_data:id::string   AS set_id,
        event_data:name::string AS set_name
    FROM flow_onchain_core_data.core.fact_events
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'SetCreated'
      AND tx_succeeded = TRUE
)

SELECT
  m.nft_id            AS NFT_ID,
  m.edition_id        AS EDITION_ID,
  e.play_id           AS PLAY_ID,
  e.series_id         AS SERIES_ID,
  e.set_id            AS SET_ID,
  e.tier              AS TIER,
  m.serial_number     AS SERIAL_NUMBER,
  e.max_mint_size     AS MAX_MINT_SIZE,
  p.first_name        AS FIRST_NAME,
  p.last_name         AS LAST_NAME,
  p.team_name         AS TEAM_NAME,
  p.position          AS POSITION,
  p.jersey_number     AS JERSEY_NUMBER,
  s.series_name       AS SERIES_NAME,
  st.set_name         AS SET_NAME
FROM minted_nfts m
LEFT JOIN edition_metadata e ON m.edition_id = e.edition_id
LEFT JOIN play_metadata    p ON e.play_id   = p.play_id
LEFT JOIN series_names     s ON e.series_id = s.series_id
LEFT JOIN set_names        st ON e.set_id   = st.set_id;

SELECT 'ALLDAY_CORE_NFT_METADATA refreshed: ' || COUNT(*) || ' rows' AS status 
FROM NFL_ALLDAY.ANALYTICS.ALLDAY_CORE_NFT_METADATA;


-- =====================================================
-- STEP 2: REFRESH ALLDAY_WALLET_HOLDINGS_CURRENT
-- Based on Holdings_events_query.sql
-- =====================================================

CREATE OR REPLACE TABLE NFL_ALLDAY.ANALYTICS.ALLDAY_WALLET_HOLDINGS_CURRENT AS

WITH burned_nfts AS (
    SELECT event_data:id::STRING AS nft_id
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
    WHERE event_type = 'MomentNFTBurned'
      AND tx_succeeded = TRUE
      AND block_timestamp >= '2021-01-01'
),

events AS (
    SELECT
        event_data:id::string        AS nft_id,
        LOWER(event_data:to::string)   AS to_addr,
        LOWER(event_data:from::string) AS from_addr,
        event_type                     AS event_type,
        block_timestamp                AS block_timestamp,
        block_height                   AS block_height,
        event_index                    AS event_index
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type IN ('Deposit', 'Withdraw')
      AND tx_succeeded = TRUE
      AND block_timestamp >= '2021-01-01'
),

latest AS (
    SELECT
        nft_id,
        to_addr,
        from_addr,
        event_type,
        block_timestamp,
        ROW_NUMBER() OVER (
          PARTITION BY nft_id
          ORDER BY block_timestamp DESC, event_index DESC
        ) AS rn
    FROM events
),

-- Check for locked NFTs
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
        block_timestamp
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.b6f2481eba4df97b.NFTLocker'
      AND event_type = 'NFTUnlocked'
      AND tx_succeeded = TRUE
      AND block_timestamp >= '2021-01-01'
),

lock_status AS (
    SELECT DISTINCT
        l.nft_id,
        TRUE AS is_locked
    FROM locked_events l
    LEFT JOIN unlocked_events u 
        ON l.nft_id = u.nft_id 
        AND u.block_timestamp > l.block_timestamp
    WHERE u.nft_id IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY l.nft_id ORDER BY l.block_timestamp DESC) = 1
)

SELECT
    l.to_addr AS WALLET_ADDRESS,
    l.nft_id AS NFT_ID,
    COALESCE(ls.is_locked, FALSE) AS IS_LOCKED,
    l.block_timestamp AS LAST_EVENT_TS
FROM latest l
LEFT JOIN lock_status ls ON l.nft_id = ls.nft_id
WHERE l.rn = 1
  AND l.event_type = 'Deposit'
  AND l.to_addr IS NOT NULL
  AND l.nft_id NOT IN (SELECT nft_id FROM burned_nfts);

SELECT 'ALLDAY_WALLET_HOLDINGS_CURRENT refreshed: ' || COUNT(*) || ' rows' AS status 
FROM NFL_ALLDAY.ANALYTICS.ALLDAY_WALLET_HOLDINGS_CURRENT;

-- Verify your wallet count
SELECT 'Your wallet (0x7541bafd155b683e): ' || COUNT(*) || ' moments' AS status
FROM NFL_ALLDAY.ANALYTICS.ALLDAY_WALLET_HOLDINGS_CURRENT 
WHERE wallet_address = '0x7541bafd155b683e';

