-- Compute average sale price (90d) and last sale per edition

WITH sales AS (
    SELECT
        event_data:editionID::string AS "edition_id",
        TRY_TO_NUMBER(event_data:price::string) AS "price",
        block_timestamp              AS "block_timestamp"
    FROM flow_onchain_core_data.core.fact_events
WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
  AND event_type IN (
    'MomentPurchased',
    'PurchaseAccepted',
    'ListingCompleted',
    'SaleCompleted'
  )
  AND tx_succeeded = TRUE
  AND block_timestamp >= '2021-01-01'
),
agg AS (
    SELECT
        "edition_id",
        AVG("price")                     AS "asp_90d",
        MAX("block_timestamp")           AS "last_sale_ts"
    FROM sales
    WHERE "price" IS NOT NULL
    GROUP BY "edition_id"
),

last_sales AS (
    SELECT
        s."edition_id",
        s."price"         AS "last_sale",
        s."block_timestamp"
    FROM sales s
    JOIN agg a
      ON s."edition_id" = a."edition_id"
     AND s."block_timestamp" = a."last_sale_ts"
)

SELECT
    a."edition_id",
    a."asp_90d",
    l."last_sale",
    l."block_timestamp" AS "last_sale_ts"
FROM agg a
LEFT JOIN last_sales l
  ON a."edition_id" = l."edition_id"
ORDER BY a."edition_id"
LIMIT 100000;
