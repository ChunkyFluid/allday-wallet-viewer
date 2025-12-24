import { pgQuery } from './db.js';

async function inspectNfts(nftIds) {
    try {
        for (const nftId of nftIds) {
            console.log(`\n--- Inspecting NFT ${nftId} ---`);

            console.log('wallet_holdings:');
            const wh = await pgQuery('SELECT * FROM wallet_holdings WHERE nft_id = $1', [nftId]);
            console.log(wh.rows);

            console.log('holdings:');
            const h = await pgQuery('SELECT * FROM holdings WHERE nft_id = $1', [nftId]);
            console.log(h.rows);

            console.log('nft_core_metadata_v2:');
            const m = await pgQuery('SELECT * FROM nft_core_metadata_v2 WHERE nft_id = $1', [nftId]);
            console.log(m.rows);
        }

    } catch (err) {
        console.error('Error:', err);
    } finally {
        process.exit(0);
    }
}

inspectNfts(['6063904', '6049871']);
