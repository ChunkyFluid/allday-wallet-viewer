-- For each NFT, find the latest Deposit/Withdraw event.
-- If the latest event is a Deposit, the "to" address is treated as the current owner.
-- If the latest event is a Withdraw, we ignore it (NFT not considered held).

WITH events AS (
    SELECT
        event_data:id::string        AS "nft_id",
        LOWER(event_data:to::string)   AS "to_addr",
        LOWER(event_data:from::string) AS "from_addr",
        event_type                     AS "event_type",
        block_timestamp                AS "block_timestamp",
        block_height                   AS "block_height",
        event_index                    AS "event_index"
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type IN ('Deposit', 'Withdraw')
      AND tx_succeeded = TRUE
      AND block_timestamp >= '2021-01-01'
),

latest AS (
    SELECT
        "nft_id",
        "to_addr",
        "from_addr",
        "event_type",
        "block_timestamp",
        ROW_NUMBER() OVER (
          PARTITION BY "nft_id"
          ORDER BY "block_timestamp" DESC, "event_index" DESC
        ) AS "rn"
    FROM events
)

SELECT
    "nft_id",
    "to_addr" AS "wallet_address",
    "block_timestamp"
FROM latest
WHERE "rn" = 1
  AND "event_type" = 'Deposit'
  AND "to_addr" IS NOT NULL
ORDER BY "block_timestamp" DESC
LIMIT 50000;
