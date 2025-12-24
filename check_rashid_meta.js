// Check if Rashid has proper metadata to be displayed
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function check() {
    // Find the Rashid we added (NFT ID 4098385)
    console.log('=== CHECKING RASHID SHAHEED #189 (NFT 4098385) ===\n');

    // Check if it's in holdings
    const holding = await pgQuery(`SELECT * FROM holdings WHERE nft_id = '4098385'`);
    console.log('In holdings table:', holding.rows.length > 0 ? 'YES' : 'NO');
    if (holding.rows.length > 0) console.log(holding.rows[0]);

    // Check if it has metadata
    const meta = await pgQuery(`SELECT * FROM nft_core_metadata_v2 WHERE nft_id = '4098385'`);
    console.log('\nIn metadata table:', meta.rows.length > 0 ? 'YES' : 'NO');
    if (meta.rows.length > 0) console.log(meta.rows[0]);

    // Check ALL Rashid Shaheed NFTs owned by user
    console.log('\n=== ALL RASHID SHAHEED OWNED BY USER ===');
    const allRashid = await pgQuery(`
        SELECT h.nft_id, m.first_name, m.last_name, m.serial_number, m.team_name
        FROM holdings h
        LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND (LOWER(m.first_name) = 'rashid' OR m.nft_id IS NULL)
        ORDER BY h.nft_id
    `, [WALLET]);
    console.table(allRashid.rows);

    // Check NFTs without metadata
    console.log('\n=== NFTs IN HOLDINGS WITHOUT METADATA ===');
    const noMeta = await pgQuery(`
        SELECT h.nft_id
        FROM holdings h
        LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1 AND m.nft_id IS NULL
    `, [WALLET]);
    console.log('NFTs without metadata:', noMeta.rows.length);
    if (noMeta.rows.length > 0) {
        console.log('NFT IDs:', noMeta.rows.map(r => r.nft_id).join(', '));
    }

    process.exit(0);
}
check();
