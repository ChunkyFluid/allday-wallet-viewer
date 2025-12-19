-- snowflake_backfill_metadata.sql
-- Rebuild core metadata tables in Snowflake from FLOW_ONCHAIN_CORE_DATA events.
-- Outputs: nft_core_metadata (one row per nft_id with edition, play, set, series, tier, max_mint_size, player/team/position).

USE DATABASE ALLDAY_VIEWER;
USE SCHEMA ALLDAY;

-- 1) Series
WITH series_names AS (
  SELECT
    event_data:id::string AS series_id,
    event_data:name::string AS series_name,
    ROW_NUMBER() OVER (PARTITION BY event_data:id::string ORDER BY block_height DESC, event_index DESC) AS rn
  FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
  WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
    AND event_type = 'SeriesCreated'
    AND tx_succeeded = TRUE
),

-- 2) Sets
set_names AS (
  SELECT
    event_data:id::string AS set_id,
    event_data:name::string AS set_name,
    ROW_NUMBER() OVER (PARTITION BY event_data:id::string ORDER BY block_height DESC, event_index DESC) AS rn
  FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
  WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
    AND event_type = 'SetCreated'
    AND tx_succeeded = TRUE
),

-- 3) Editions
edition_metadata AS (
  SELECT
    event_data:id::string AS edition_id,
    event_data:maxMintSize::string AS max_mint_size,
    event_data:playID::string AS play_id,
    event_data:seriesID::string AS series_id,
    event_data:setID::string AS set_id,
    event_data:tier::string AS tier,
    ROW_NUMBER() OVER (PARTITION BY event_data:id::string ORDER BY block_height DESC, event_index DESC) AS rn
  FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
  WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
    AND event_type = 'EditionCreated'
    AND tx_succeeded = TRUE
),

-- 4) Plays + player/team/position metadata
play_metadata AS (
  SELECT
    event_data:id::STRING AS play_id,
    MAX(CASE WHEN metadata.value:key.value::STRING = 'playerFirstName' THEN metadata.value:value.value::STRING END) AS first_name,
    MAX(CASE WHEN metadata.value:key.value::STRING = 'playerLastName'  THEN metadata.value:value.value::STRING END) AS last_name,
    MAX(CASE WHEN metadata.value:key.value::STRING = 'teamName'       THEN metadata.value:value.value::STRING END) AS team_name,
    MAX(CASE WHEN metadata.value:key.value::STRING = 'playerPosition' THEN metadata.value:value.value::STRING END) AS position,
    MAX(CASE WHEN metadata.value:key.value::STRING = 'playerNumber'   THEN metadata.value:value.value::STRING END) AS jersey_number,
    ROW_NUMBER() OVER (PARTITION BY event_data:id::STRING ORDER BY block_height DESC, event_index DESC) AS rn
  FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS,
       TABLE(FLATTEN(event_data:metadata)) AS metadata
  WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
    AND event_type = 'PlayCreated'
    AND tx_succeeded = TRUE
  GROUP BY event_data:id::STRING, block_height, event_index
),

-- 5) Mint events to enumerate nft_id ↔ edition_id + serial
minted_nfts AS (
  SELECT
    event_data:id::string AS nft_id,
    event_data:editionID::string AS edition_id,
    event_data:serialNumber::string AS serial_number,
    ROW_NUMBER() OVER (PARTITION BY event_data:id::string ORDER BY block_height DESC, event_index DESC) AS rn
  FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
  WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
    AND event_type = 'MomentNFTMinted'
    AND tx_succeeded = TRUE
),

-- 6) Final metadata rows per nft_id
nft_rows AS (
  SELECT
    m.nft_id,
    m.edition_id,
    m.serial_number,
    e.max_mint_size,
    e.play_id,
    e.series_id,
    e.set_id,
    e.tier,
    p.first_name,
    p.last_name,
    p.team_name,
    p.position,
    p.jersey_number,
    s.series_name,
    st.set_name,
    ROW_NUMBER() OVER (PARTITION BY m.nft_id ORDER BY m.rn) AS rn
  FROM minted_nfts m
  LEFT JOIN edition_metadata e ON e.edition_id = m.edition_id AND e.rn = 1
  LEFT JOIN play_metadata p ON p.play_id = e.play_id AND p.rn = 1
  LEFT JOIN series_names s ON s.series_id = e.series_id AND s.rn = 1
  LEFT JOIN set_names st ON st.set_id = e.set_id AND st.rn = 1
)

-- 7) Truncate and reload nft_core_metadata
SELECT 'About to truncate and reload nft_core_metadata' AS info;

BEGIN;
  TRUNCATE TABLE nft_core_metadata;
  INSERT INTO nft_core_metadata (
    nft_id, edition_id, play_id, series_id, set_id, tier, serial_number,
    max_mint_size, first_name, last_name, team_name, position, jersey_number,
    series_name, set_name
  )
  SELECT
    nft_id, edition_id, play_id, series_id, set_id, tier, serial_number,
    max_mint_size, first_name, last_name, team_name, position, jersey_number,
    series_name, set_name
  FROM nft_rows
  WHERE rn = 1;
COMMIT;

SELECT 'Reload complete' AS info, COUNT(*) AS rows_loaded FROM nft_core_metadata;
