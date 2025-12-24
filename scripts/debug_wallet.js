// scripts/debug_wallet.js
import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";

dotenv.config();

const wallet = process.argv[2] || '0x07943321994c72c6';

async function debugWallet() {
    console.log(`\n=== Debugging wallet: ${wallet} ===\n`);

    // 1. Check holdings
    const holdings = await pgQuery(
        `SELECT nft_id, is_locked, acquired_at FROM holdings WHERE wallet_address = $1`,
        [wallet.toLowerCase()]
    );
    console.log(`1. holdings: ${holdings.rows.length} rows`);
    if (holdings.rows.length > 0) {
        console.log('   NFT IDs:', holdings.rows.map(r => r.nft_id).join(', '));
    }

    // 2. Check wallet_profiles
    const profile = await pgQuery(
        `SELECT display_name, source, last_checked FROM wallet_profiles WHERE wallet_address = $1`,
        [wallet.toLowerCase()]
    );
    console.log(`\n2. wallet_profiles: ${profile.rows.length} rows`);
    if (profile.rows.length > 0) {
        console.log('   Display name:', profile.rows[0].display_name || '(NULL)');
        console.log('   Last checked:', profile.rows[0].last_checked);
    }

    // 3. Check nft_core_metadata for these NFTs
    if (holdings.rows.length > 0) {
        const nftIds = holdings.rows.map(r => r.nft_id);
        const metadata = await pgQuery(
            `SELECT nft_id, first_name, last_name, tier, serial_number FROM nft_core_metadata WHERE nft_id = ANY($1::text[])`,
            [nftIds]
        );
        console.log(`\n3. nft_core_metadata: ${metadata.rows.length}/${nftIds.length} NFTs have metadata`);
        if (metadata.rows.length > 0) {
            for (const m of metadata.rows) {
                console.log(`   NFT ${m.nft_id}: ${m.first_name} ${m.last_name} - ${m.tier} #${m.serial_number}`);
            }
        }

        // Show which NFTs are missing metadata
        const foundIds = new Set(metadata.rows.map(r => r.nft_id));
        const missing = nftIds.filter(id => !foundIds.has(id));
        if (missing.length > 0) {
            console.log(`   MISSING metadata for: ${missing.join(', ')}`);
        }
    }

    // 4. Test Dapper API for this wallet
    console.log(`\n4. Fetching profile from Dapper API...`);
    try {
        const res = await fetch(`https://open.meetdapper.com/profile?address=${wallet}`, {
            headers: { 'user-agent': 'allday-wallet-viewer/1.0' }
        });
        if (res.ok) {
            const data = await res.json();
            console.log('   Dapper response:', JSON.stringify(data, null, 2));
        } else {
            console.log(`   Dapper returned HTTP ${res.status}`);
        }
    } catch (e) {
        console.log(`   Dapper error: ${e.message}`);
    }

    console.log('\n=== Debug complete ===\n');
}

debugWallet()
    .then(() => process.exit(0))
    .catch(e => { console.error(e); process.exit(1); });
