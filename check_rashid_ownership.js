// Check ownership for all Rashid Shaheed #1096 NFT IDs
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';
const NFT_IDS = ['4099792', '5659892', '8751347', '5430071'];

async function check() {
    for (const nftId of NFT_IDS) {
        console.log(`\n=== NFT ID ${nftId} ===`);

        const h = await pgQuery(`SELECT wallet_address, is_locked, acquired_at FROM holdings WHERE nft_id = $1`, [nftId]);
        console.log('holdings:', h.rows.length > 0 ? h.rows[0] : 'NOT FOUND');

        const wh = await pgQuery(`SELECT wallet_address, is_locked FROM wallet_holdings WHERE nft_id = $1`, [nftId]);
        console.log('wallet_holdings:', wh.rows.length > 0 ? wh.rows[0] : 'NOT FOUND');
    }

    process.exit(0);
}
check();
