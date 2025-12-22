import fs from 'fs';

const content = fs.readFileSync('missing_nft_ids.txt', 'utf8');
const ids = content.split(',').map(id => id.replace(/'/g, ''));
const CHUNK_SIZE = 8000;

for (let i = 0; i < ids.length; i += CHUNK_SIZE) {
    const chunkId = Math.floor(i / CHUNK_SIZE) + 1;
    const chunk = ids.slice(i, i + CHUNK_SIZE);

    // Using VALUES clause to create a virtual table of IDs
    const valuesList = chunk.map(id => `('${id}')`).join(',');

    const query = `
-- CHUNK ${chunkId} of 4
WITH missing_ids AS (
    SELECT column1 as NFT_ID FROM VALUES ${valuesList}
)
SELECT 
    LOWER(EVENT_DATA:to::STRING) as WALLET_ADDRESS,
    EVENT_DATA:id::STRING as NFT_ID,
    MIN(BLOCK_TIMESTAMP) as ACQUIRED_AT
FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay'
  AND EVENT_TYPE = 'Deposit'
  AND TX_SUCCEEDED = true
  AND EVENT_DATA:id::STRING IN (SELECT NFT_ID FROM missing_ids)
GROUP BY 1, 2;
    `;

    fs.writeFileSync(`fix_dates_query_${chunkId}.sql`, query);
}

console.log('Created 4 query files: fix_dates_query_1.sql to fix_dates_query_4.sql');
