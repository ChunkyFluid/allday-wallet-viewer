-- FIXED: Include BOTH regular holdings AND locked NFTs
-- This should give you ~2840 moments

CREATE OR REPLACE TABLE NFL_ALLDAY.ANALYTICS.ALLDAY_WALLET_HOLDINGS_CURRENT AS

WITH burned_nfts AS (
    SELECT event_data:id::STRING AS nft_id
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
    WHERE event_type = 'MomentNFTBurned'
      AND tx_succeeded = TRUE
      AND block_timestamp >= '2021-01-01'
),

-- All transfer events
all_transfers AS (
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

-- Latest transfer event per NFT (determines current holder)
latest_transfer AS (
    SELECT *
    FROM all_transfers
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY nft_id 
        ORDER BY block_timestamp DESC, block_height DESC, event_index DESC
    ) = 1
),

-- NFTs held in wallets (latest event is Deposit)
regular_holdings AS (
    SELECT 
        wallet AS wallet_address,
        nft_id,
        FALSE AS is_locked,
        block_timestamp AS last_event_ts
    FROM latest_transfer
    WHERE event_type = 'Deposit'
      AND wallet IS NOT NULL
      AND nft_id NOT IN (SELECT nft_id FROM burned_nfts WHERE nft_id IS NOT NULL)
),

-- Locked NFTs (NFTLocked events without corresponding NFTUnlocked)
locked_nfts AS (
    SELECT 
        LOWER(l.event_data:to::STRING) AS wallet_address,
        l.event_data:id::STRING AS nft_id,
        TRUE AS is_locked,
        l.block_timestamp AS last_event_ts
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS l
    WHERE l.event_contract = 'A.b6f2481eba4df97b.NFTLocker'
      AND l.event_type = 'NFTLocked'
      AND l.tx_succeeded = TRUE
      AND l.block_timestamp >= '2021-01-01'
      AND NOT EXISTS (
          SELECT 1 
          FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS u
          WHERE u.event_contract = 'A.b6f2481eba4df97b.NFTLocker'
            AND u.event_type = 'NFTUnlocked'
            AND u.event_data:id::STRING = l.event_data:id::STRING
            AND u.block_timestamp > l.block_timestamp
            AND u.tx_succeeded = TRUE
      )
      AND l.event_data:id::STRING NOT IN (SELECT nft_id FROM burned_nfts WHERE nft_id IS NOT NULL)
),

-- Combine: Regular holdings + Locked NFTs (locked takes precedence)
combined AS (
    SELECT wallet_address, nft_id, is_locked, last_event_ts
    FROM locked_nfts
    
    UNION ALL
    
    SELECT wallet_address, nft_id, is_locked, last_event_ts
    FROM regular_holdings
    WHERE nft_id NOT IN (SELECT nft_id FROM locked_nfts)
)

SELECT 
    wallet_address AS WALLET_ADDRESS,
    nft_id AS NFT_ID,
    is_locked AS IS_LOCKED,
    last_event_ts AS LAST_EVENT_TS
FROM combined
WHERE wallet_address IS NOT NULL;

-- Verify total rows
SELECT 'Total holdings: ' || COUNT(*) AS status 
FROM NFL_ALLDAY.ANALYTICS.ALLDAY_WALLET_HOLDINGS_CURRENT;

-- Verify YOUR wallet count (should be ~2840)
SELECT 'Your wallet: ' || COUNT(*) || ' moments (' || 
       SUM(CASE WHEN is_locked THEN 1 ELSE 0 END) || ' locked, ' ||
       SUM(CASE WHEN NOT is_locked THEN 1 ELSE 0 END) || ' unlocked)'
       AS status
FROM NFL_ALLDAY.ANALYTICS.ALLDAY_WALLET_HOLDINGS_CURRENT 
WHERE wallet_address = '0x7541bafd155b683e';

