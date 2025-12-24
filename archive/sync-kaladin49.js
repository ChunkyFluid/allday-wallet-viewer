import { pgQuery } from './db.js';

async function syncKaladin49() {
    try {
        const walletAddress = '0xd3914b2bfb2bd59d';

        console.log(`Syncing wallet ${walletAddress} from blockchain...\n`);

        // Make a request to the sync endpoint
        const response = await fetch(`http://localhost:3000/api/sync-wallet?address=${walletAddress}`);
        const data = await response.json();

        console.log('Sync result:');
        console.log(JSON.stringify(data, null, 2));

        if (data.ok) {
            console.log(`\n✅ Successfully synced ${data.totalNfts || 0} NFTs`);

            // Now check for Bo Nix again
            console.log('\nChecking for Bo Nix after sync...\n');

            const boNixQuery = `
        SELECT m.nft_id, m.first_name, m.last_name, m.set_name, m.serial_number, m.tier
        FROM nft_core_metadata_v2 m
        INNER JOIN wallet_holdings wh ON m.nft_id = wh.nft_id
        WHERE wh.wallet_address = $1
          AND m.last_name ILIKE '%nix%'
        ORDER BY m.nft_id DESC
      `;

            const boNix = await pgQuery(boNixQuery, [walletAddress]);

            if (boNix.rows.length > 0) {
                console.log(`✅ Found ${boNix.rows.length} Bo Nix moments:`);
                boNix.rows.forEach(row => {
                    console.log(`  - NFT ${row.nft_id}: ${row.first_name} ${row.last_name} #${row.serial_number}`);
                    console.log(`    ${row.set_name} (${row.tier})`);
                });
            } else {
                console.log('❌ Still no Bo Nix moments found');
            }
        } else {
            console.log(`\n❌ Sync failed: ${data.error || 'Unknown error'}`);
        }

    } catch (error) {
        console.error('\nError:', error.message);
    } finally {
        process.exit();
    }
}

syncKaladin49();
