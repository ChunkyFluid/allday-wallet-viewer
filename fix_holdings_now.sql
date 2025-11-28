-- JUST RUN THIS ENTIRE SCRIPT AS ONE QUERY
-- It will take 2-5 minutes to complete

CREATE OR REPLACE TABLE NFL_ALLDAY.ANALYTICS.ALLDAY_WALLET_HOLDINGS_CURRENT AS

WITH burned_nfts AS (
    SELECT event_data:id::STRING AS nft_id
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
    WHERE event_type = 'MomentNFTBurned'
      AND tx_succeeded = TRUE
      AND block_timestamp >= '2021-01-01'
),

all_deposits AS (
    SELECT
        event_data:id::STRING AS nft_id,
        LOWER(event_data:to::STRING) AS wallet_address,
        block_timestamp,
        block_height,
        event_index
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'Deposit'
      AND tx_succeeded = TRUE
      AND block_timestamp >= '2021-01-01'
),

all_withdraws AS (
    SELECT
        event_data:id::STRING AS nft_id,
        LOWER(event_data:from::STRING) AS wallet_address,
        block_timestamp,
        block_height,
        event_index
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'Withdraw'
      AND tx_succeeded = TRUE
      AND block_timestamp >= '2021-01-01'
),

-- Combine and find latest event per NFT
all_events AS (
    SELECT nft_id, wallet_address, block_timestamp, block_height, event_index, 'Deposit' AS event_type
    FROM all_deposits
    UNION ALL
    SELECT nft_id, wallet_address, block_timestamp, block_height, event_index, 'Withdraw' AS event_type
    FROM all_withdraws
),

latest_per_nft AS (
    SELECT *
    FROM all_events
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY nft_id 
        ORDER BY block_timestamp DESC, block_height DESC, event_index DESC
    ) = 1
),

-- Locked NFTs
locked_nfts AS (
    SELECT DISTINCT event_data:id::STRING AS nft_id
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS l
    WHERE event_contract = 'A.b6f2481eba4df97b.NFTLocker'
      AND event_type = 'NFTLocked'
      AND tx_succeeded = TRUE
      AND NOT EXISTS (
          SELECT 1 FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS u
          WHERE u.event_contract = 'A.b6f2481eba4df97b.NFTLocker'
            AND u.event_type = 'NFTUnlocked'
            AND u.event_data:id::STRING = l.event_data:id::STRING
            AND u.block_timestamp > l.block_timestamp
            AND u.tx_succeeded = TRUE
      )
)

SELECT
    l.wallet_address AS WALLET_ADDRESS,
    l.nft_id AS NFT_ID,
    CASE WHEN lk.nft_id IS NOT NULL THEN TRUE ELSE FALSE END AS IS_LOCKED,
    l.block_timestamp AS LAST_EVENT_TS
FROM latest_per_nft l
LEFT JOIN locked_nfts lk ON l.nft_id = lk.nft_id
WHERE l.event_type = 'Deposit'
  AND l.wallet_address IS NOT NULL
  AND l.nft_id NOT IN (SELECT nft_id FROM burned_nfts WHERE nft_id IS NOT NULL);

