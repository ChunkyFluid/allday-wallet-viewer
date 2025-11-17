WITH burned_nfts AS (
  SELECT
    EVENT_DATA:id::STRING as NFT_ID
  FROM 
    FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
  WHERE 
    EVENT_TYPE='MomentNFTBurned' AND
    TX_SUCCEEDED = true AND
    BLOCK_TIMESTAMP >= '2021-01-01'
),

minted_nfts AS (
    -- Get all minted AllDay NFTs with basic info
    SELECT 
        event_data:id::string AS nft_id,
        event_data:editionID::string AS edition_id,
        event_data:serialNumber::string AS serial_number,
        tx_id,
        block_timestamp
    FROM flow_onchain_core_data.core.fact_events 
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'MomentNFTMinted'
      AND tx_succeeded = TRUE
),

edition_metadata AS (
    -- Get edition metadata including playID, seriesID, setID, tier, maxMintSize
    SELECT 
        event_data:id::string AS edition_id,
        event_data:maxMintSize::string AS max_mint_size,
        event_data:playID::string AS play_id,
        event_data:seriesID::string AS series_id,
        event_data:setID::string AS set_id,
        event_data:tier::string AS tier
    FROM flow_onchain_core_data.core.fact_events 
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'EditionCreated'
      AND tx_succeeded = TRUE
),

series_names AS (
    -- Get series names
    SELECT 
        event_data:id::string AS series_id,
        event_data:name::string AS series_name
    FROM flow_onchain_core_data.core.fact_events 
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'SeriesCreated'
      AND tx_succeeded = TRUE
),

set_names AS (
    -- Get set names
    SELECT 
        event_data:id::string AS set_id,
        event_data:name::string AS set_name
    FROM flow_onchain_core_data.core.fact_events 
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'SetCreated'
      AND tx_succeeded = TRUE
),

core_nft_metadata AS (
-- Final query joining all metadata
  SELECT 
    m.nft_id,
    m.edition_id AS editionID,
    e.max_mint_size AS maxMintSize,
    e.play_id AS playID,
    e.series_id AS seriesID,
    s.series_name AS seriesName,
    e.set_id AS setID,
    st.set_name AS setName,
    e.tier,
    m.serial_number AS serialNumber
  FROM minted_nfts m
  LEFT JOIN edition_metadata e ON m.edition_id = e.edition_id
  LEFT JOIN series_names s ON e.series_id = s.series_id
  LEFT JOIN set_names st ON e.set_id = st.set_id
  WHERE m.nft_id IS NOT NULL
  ORDER BY m.nft_id::int
),
    
my_deposit_events AS (
  SELECT 
    TX_ID, 
    BLOCK_TIMESTAMP, 
    BLOCK_HEIGHT,
    EVENT_INDEX, 
    EVENT_TYPE,
    EVENT_DATA:id::STRING as NFT_ID, 
    EVENT_DATA:to::STRING as Wallet_Address
  FROM 
    FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
  WHERE 
    EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay' AND
    LOWER(EVENT_DATA:to::STRING) = LOWER('0x7541bafd155b683e') AND
    EVENT_TYPE = 'Deposit' AND
    TX_SUCCEEDED = true AND
    BLOCK_TIMESTAMP >= '2021-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
),

my_withdraw_events AS (
  SELECT 
    TX_ID, 
    BLOCK_TIMESTAMP, 
    BLOCK_HEIGHT,
    EVENT_INDEX, 
    EVENT_TYPE,
    EVENT_DATA:id::STRING as NFT_ID, 
    EVENT_DATA:from::STRING as Wallet_Address
  FROM 
    FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
  WHERE 
    EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay' AND
    LOWER(EVENT_DATA:from::STRING) = LOWER('0x7541bafd155b683e') AND
    EVENT_TYPE = 'Withdraw' AND
    TX_SUCCEEDED = true AND
    BLOCK_TIMESTAMP >= '2021-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
),

my_unlocked_nfts AS (
  SELECT 
    d.TX_ID, 
    d.BLOCK_TIMESTAMP, 
    d.BLOCK_HEIGHT,
    d.EVENT_INDEX, 
    d.EVENT_TYPE,
    d.NFT_ID, 
    d.Wallet_Address,
    -- Generate NFL All Day marketplace URL
    'https://nflallday.com/moments/' || d.NFT_ID as nfl_allday_url
  FROM
    my_deposit_events AS d LEFT JOIN
    my_withdraw_events AS w ON d.NFT_ID = w.NFT_ID AND w.BLOCK_TIMESTAMP >= d.BLOCK_TIMESTAMP
  WHERE
    w.NFT_ID IS NULL AND
    d.NFT_ID NOT IN ((SELECT NFT_ID FROM burned_nfts))
),

my_locked_events AS (
  SELECT 
    TX_ID, 
    BLOCK_TIMESTAMP, 
    BLOCK_HEIGHT,
    EVENT_INDEX, 
    EVENT_TYPE,
    EVENT_DATA:id::STRING as NFT_ID, 
    EVENT_DATA:to::STRING as Wallet_Address
  FROM 
    FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
  WHERE 
    EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' AND
    LOWER(EVENT_DATA:to::STRING) = LOWER('0x7541bafd155b683e') AND
    EVENT_TYPE = 'NFTLocked' AND
    TX_SUCCEEDED = true AND
    BLOCK_TIMESTAMP >= '2021-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
),

my_unlocked_events AS (
  SELECT 
    BLOCK_TIMESTAMP, 
    BLOCK_HEIGHT,
    EVENT_DATA:id::STRING as NFT_ID
  FROM 
    FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
  WHERE 
    EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' AND
    LOWER(EVENT_DATA:from::STRING) = LOWER('0x7541bafd155b683e') AND
    EVENT_TYPE = 'NFTUnlocked' AND
    TX_SUCCEEDED = true AND
    BLOCK_TIMESTAMP >= '2021-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
),

my_locked_nfts AS (
  SELECT
    l.TX_ID, 
    l.BLOCK_TIMESTAMP, 
    l.BLOCK_HEIGHT,
    l.EVENT_INDEX, 
    l.EVENT_TYPE,
    l.NFT_ID, 
    l.Wallet_Address,
    -- Generate NFL All Day marketplace URL
    'https://nflallday.com/moments/' || l.NFT_ID as nfl_allday_url
  FROM
    my_locked_events as l LEFT JOIN 
    my_unlocked_events as u ON l.NFT_ID=u.NFT_ID AND u.BLOCK_TIMESTAMP >= l.BLOCK_TIMESTAMP
  WHERE
    u.NFT_ID IS NULL AND
    l.NFT_ID NOT IN ( (SELECT NFT_ID FROM burned_nfts) )
),

my_nfts AS (
  (SELECT * FROM my_unlocked_nfts) UNION 
  (SELECT * FROM my_locked_nfts)
)

  SELECT 
    d.serialNumber,
    d.setName,
    d.tier,
    d.seriesName,
    d.maxMintSize,
    d.playID,
    d.setID,
    -- Generate NFL All Day marketplace URL
    'https://nflallday.com/moments/' || d.nft_id as nfl_allday_url,
    c.Wallet_Address,
    c.EVENT_TYPE,
    d.editionID,
    'https://nflallday.com/listing/moment/' || d.editionID,
    d.seriesID,
    d.nft_id,
    c.block_timestamp
    
  FROM
    core_nft_metadata as d INNER JOIN 
    my_nfts as c ON d.nft_id = c.NFT_ID
  ORDER BY
    c.block_timestamp
    