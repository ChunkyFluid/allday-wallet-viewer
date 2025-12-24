// Fix Ja'Tavion Sanders (remove if not owned) and Shedeur Sanders date
import { pgQuery } from './db.js';
import * as flowService from './services/flow-blockchain.js';

const WALLET = '0x7541bafd155b683e';

async function fix() {
    console.log('=== FIXING SANDERS MOMENTS ===\n');

    // Get blockchain NFTs
    const blockchainNfts = await flowService.getWalletNFTIds(WALLET);
    const blockchainSet = new Set(blockchainNfts.map(id => id.toString()));

    // 1. Check Ja'Tavion Sanders #31
    const jatavion = await pgQuery(`
        SELECT h.nft_id, m.first_name, m.last_name, m.serial_number
        FROM wallet_holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND LOWER(m.last_name) = 'sanders'
          AND m.serial_number = 31
    `, [WALLET]);

    if (jatavion.rows.length > 0) {
        const nftId = jatavion.rows[0].nft_id;
        const onBlockchain = blockchainSet.has(nftId);
        console.log(`Ja'Tavion Sanders #31 (NFT ${nftId}):`);
        console.log(`  - In wallet_holdings: YES`);
        console.log(`  - On blockchain: ${onBlockchain ? 'YES' : 'NO'}`);

        if (!onBlockchain) {
            await pgQuery(`DELETE FROM wallet_holdings WHERE wallet_address = $1 AND nft_id = $2`, [WALLET, nftId]);
            console.log(`  ✅ REMOVED from wallet_holdings (not owned)`);
        }
    } else {
        console.log('Ja\'Tavion Sanders #31: Not in wallet_holdings');
    }

    // 2. Fix Shedeur Sanders date to yesterday
    const shedeur = await pgQuery(`
        SELECT h.nft_id, m.serial_number
        FROM wallet_holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND LOWER(m.first_name) = 'shedeur'
          AND LOWER(m.last_name) = 'sanders'
    `, [WALLET]);

    console.log(`\nShedeur Sanders moments found: ${shedeur.rowCount}`);

    if (shedeur.rowCount > 0) {
        // Set to yesterday
        const yesterday = new Date();
        yesterday.setDate(yesterday.getDate() - 1);

        for (const row of shedeur.rows) {
            await pgQuery(`
                UPDATE wallet_holdings 
                SET last_event_ts = $1 
                WHERE wallet_address = $2 AND nft_id = $3
            `, [yesterday, WALLET, row.nft_id]);
        }
        console.log(`✅ Set ${shedeur.rowCount} Shedeur Sanders moments to yesterday (12/23/2024)`);
    }

    // Verify
    const final = await pgQuery(`SELECT COUNT(*) as c FROM wallet_holdings WHERE wallet_address = $1`, [WALLET]);
    console.log(`\nFinal wallet_holdings count: ${final.rows[0].c}`);

    process.exit(0);
}

fix().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
