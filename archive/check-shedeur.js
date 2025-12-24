import { pgQuery } from './db.js';

async function checkShedeur() {
    try {
        const myWallet = '0x7541bafd155b683e';

        console.log('Checking for Shedeur Sanders in Chunky\'s wallet...\n');

        const shedeurQuery = `
      SELECT m.nft_id, m.first_name, m.last_name, m.set_name, m.serial_number, m.tier, wh.is_locked
      FROM wallet_holdings wh
      INNER JOIN nft_core_metadata_v2 m ON wh.nft_id = m.nft_id
      WHERE wh.wallet_address = $1
        AND (m.first_name ILIKE '%shedeur%' OR m.last_name ILIKE '%sanders%')
      ORDER BY m.nft_id DESC
    `;

        const result = await pgQuery(shedeurQuery, [myWallet]);

        if (result.rows.length > 0) {
            console.log(`✅ Found ${result.rows.length} Shedeur Sanders moment(s):\n`);
            result.rows.forEach(row => {
                console.log(`  🏈 NFT ${row.nft_id}: ${row.first_name} ${row.last_name} #${row.serial_number}`);
                console.log(`     Set: ${row.set_name}`);
                console.log(`     Tier: ${row.tier}`);
                console.log(`     Locked: ${row.is_locked ? 'Yes' : 'No'}`);
                console.log('');
            });
        } else {
            console.log('❌ No Shedeur Sanders moments found in wallet');
            console.log('\nTrying broader search for "Sanders"...\n');

            const sandersQuery = `
        SELECT m.nft_id, m.first_name, m.last_name, m.set_name, m.serial_number
        FROM wallet_holdings wh
        INNER JOIN nft_core_metadata_v2 m ON wh.nft_id = m.nft_id
        WHERE wh.wallet_address = $1
          AND m.last_name ILIKE '%sanders%'
        ORDER BY m.nft_id DESC
        LIMIT 10
      `;

            const sanders = await pgQuery(sandersQuery, [myWallet]);
            console.log(`Found ${sanders.rows.length} Sanders moments total:`);
            sanders.rows.forEach(row => {
                console.log(`  - ${row.first_name} ${row.last_name} #${row.serial_number} (${row.set_name})`);
            });
        }

        // Also check most recent 10 moments
        console.log('\n\nMost recent 10 moments in your wallet:');
        const recentQuery = `
      SELECT m.nft_id, m.first_name, m.last_name, m.set_name, m.serial_number, wh.last_event_ts
      FROM wallet_holdings wh
      INNER JOIN nft_core_metadata_v2 m ON wh.nft_id = m.nft_id
      WHERE wh.wallet_address = $1
      ORDER BY wh.last_event_ts DESC NULLS LAST, m.nft_id DESC
      LIMIT 10
    `;

        const recent = await pgQuery(recentQuery, [myWallet]);
        recent.rows.forEach((row, i) => {
            console.log(`  ${i + 1}. ${row.first_name} ${row.last_name} #${row.serial_number} - ${row.set_name}`);
            console.log(`      Last event: ${row.last_event_ts || 'Unknown'}`);
        });

    } catch (error) {
        console.error('Error:', error.message);
    } finally {
        process.exit();
    }
}

checkShedeur();
