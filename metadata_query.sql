WITH minted_nfts AS (
    SELECT 
        event_data:id::string        AS "nft_id",
        event_data:editionID::string AS "edition_id",
        TRY_TO_NUMBER(event_data:serialNumber::string) AS "serial_number",
        block_timestamp              AS "minted_ts"
    FROM flow_onchain_core_data.core.fact_events 
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'MomentNFTMinted'
      AND tx_succeeded = TRUE
),

edition_metadata AS (
    SELECT 
        event_data:id::string        AS "edition_id",
        event_data:playID::string    AS "play_id",
        event_data:seriesID::string  AS "series_id",
        event_data:setID::string     AS "set_id",
        event_data:tier::string      AS "tier",
        TRY_TO_NUMBER(event_data:maxMintSize::string)  AS "max_mint_size"
    FROM flow_onchain_core_data.core.fact_events
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'EditionCreated'
      AND tx_succeeded = TRUE
),

play_metadata AS (
    SELECT 
      event_data:id::STRING AS "play_id",
      MAX(CASE WHEN metadata.value:key.value::STRING = 'playerFirstName' THEN metadata.value:value.value::STRING END) AS "first_name",
      MAX(CASE WHEN metadata.value:key.value::STRING = 'playerLastName'  THEN metadata.value:value.value::STRING END) AS "last_name",
      MAX(CASE WHEN metadata.value:key.value::STRING = 'teamName'       THEN metadata.value:value.value::STRING END) AS "team_name",
      MAX(CASE WHEN metadata.value:key.value::STRING = 'playerPosition' THEN metadata.value:value.value::STRING END) AS "position",
      MAX(CASE WHEN metadata.value:key.value::STRING = 'playerNumber'   THEN metadata.value:value.value::STRING END) AS "jersey_number"
    FROM flow_onchain_core_data.core.fact_events,
         TABLE(FLATTEN(event_data:metadata)) AS metadata
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'PlayCreated'
      AND tx_succeeded = TRUE
    GROUP BY event_data:id::STRING
),

series_names AS (
    SELECT 
        event_data:id::string   AS "series_id",
        event_data:name::string AS "series_name"
    FROM flow_onchain_core_data.core.fact_events
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'SeriesCreated'
      AND tx_succeeded = TRUE
),

set_names AS (
    SELECT 
        event_data:id::string   AS "set_id",
        event_data:name::string AS "set_name"
    FROM flow_onchain_core_data.core.fact_events
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'SetCreated'
      AND tx_succeeded = TRUE
)

SELECT
  m."nft_id",
  m."edition_id",
  e."play_id",
  e."series_id",
  e."set_id",
  e."tier",
  m."serial_number",
  e."max_mint_size",
  p."first_name",
  p."last_name",
  p."team_name",
  p."position",
  p."jersey_number",
  s."series_name",
  st."set_name"
FROM minted_nfts m
LEFT JOIN edition_metadata e ON m."edition_id" = e."edition_id"
LEFT JOIN play_metadata    p ON e."play_id"   = p."play_id"
LEFT JOIN series_names     s ON e."series_id" = s."series_id"
LEFT JOIN set_names        st ON e."set_id"   = st."set_id"
ORDER BY m."minted_ts" DESC
LIMIT 100000;