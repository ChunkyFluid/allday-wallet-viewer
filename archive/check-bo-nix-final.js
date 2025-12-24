import { pgQuery } from './db.js';

async function checkBoNixNow() {
    try {
        const walletAddress = '0x93914b2bfb28d59d';

        console.log(`Checking for Bo Nix moments in Kaladin49's wallet (${walletAddress})...\n`);

        // Search for Bo Nix
        const boNixQuery = `
      SELECT m.nft_id, m.first_name, m.last_name, m.set_name, m.serial_number, m.tier, wh.is_locked
      FROM wallet_holdings wh
      INNER JOIN nft_core_metadata_v2 m ON wh.nft_id = m.nft_id
      WHERE wh.wallet_address = $1
        AND m.last_name ILIKE '%nix%'
      ORDER BY m.nft_id DESC
    `;

        const boNix = await pgQuery(boNixQuery, [walletAddress]);

        if (boNix.rows.length > 0) {
            console.log(`✅ Found ${boNix.rows.length} Bo Nix moment(s):\n`);
            boNix.rows.forEach(row => {
                console.log(`  🏈 NFT ${row.nft_id}: ${row.first_name} ${row.last_name} #${row.serial_number}`);
                console.log(`     Set: ${row.set_name}`);
                console.log(`     Tier: ${row.tier}`);
                console.log(`     Locked: ${row.is_locked ? 'Yes' : 'No'}`);
                console.log('');
            });
        } else {
            console.log('❌ No Bo Nix moments found in this wallet');
        }

        // Show recent moments
        console.log('\nMost recent 10 moments in wallet:');
        const recentQuery = `
      SELECT m.nft_id, m.first_name, m.last_name, m.set_name, m.serial_number
      FROM wallet_holdings wh
      INNER JOIN nft_core_metadata_v2 m ON wh.nft_id = m.nft_id
      WHERE wh.wallet_address = $1
      ORDER BY m.nft_id DESC
      LIMIT 10
    `;

        const recent = await pgQuery(recentQuery, [walletAddress]);
        recent.rows.forEach((row, i) => {
            console.log(`  ${i + 1}. ${row.first_name} ${row.last_name} #${row.serial_number} - ${row.set_name}`);
        });

    } catch (error) {
        console.error('\nError:', error.message);
    } finally {
        process.exit();
    }
}

checkBoNixNow();
