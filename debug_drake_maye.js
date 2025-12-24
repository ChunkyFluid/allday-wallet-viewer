import { pgQuery } from './db.js';

async function checkDrake() {
    const nftId = '10576026';
    console.log(`Checking DB for NFT ${nftId}...`);

    // 1. Check Holdings
    const holding = await pgQuery(`SELECT * FROM holdings WHERE nft_id = $1`, [nftId]);
    console.log(`\nHoldings Entry:`);
    console.table(holding.rows);

    // 2. Check Metadata
    const meta = await pgQuery(`SELECT * FROM nft_core_metadata_v2 WHERE nft_id = $1`, [nftId]);
    console.log(`\nMetadata Entry:`);
    if (meta.rows.length === 0) {
        console.log('❌ NO METADATA FOUND! This is likely why it is not showing.');
    } else {
        console.table(meta.rows);
    }
    process.exit();
}

checkDrake();
