import { pgQuery } from './db.js';

async function findBoNix() {
    try {
        // Check nft_core_metadata_v2 structure
        const metadataColsQuery = `
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_name = 'nft_core_metadata_v2'
      ORDER BY ordinal_position
    `;

        const metadataCols = await pgQuery(metadataColsQuery);
        console.log('nft_core_metadata_v2 table columns:');
        metadataCols.rows.forEach(r => console.log(`  - ${r.column_name}: ${r.data_type}`));
        console.log('\n');

        // Get sample from metadata
        const metadataSampleQuery = `
      SELECT *
      FROM nft_core_metadata_v2
      LIMIT 1
    `;

        const metadataSample = await pgQuery(metadataSampleQuery);
        console.log('Sample nft_core_metadata_v2 row:');
        console.log(JSON.stringify(metadataSample.rows[0], null, 2));
        console.log('\n');

        // Search for Bo Nix in metadata
        const boNixMetadataQuery = `
      SELECT nft_id, first_name, last_name, team_name, series_name, set_name, tier, serial_number
      FROM nft_core_metadata_v2
      WHERE last_name ILIKE '%nix%'
      ORDER BY nft_id DESC
      LIMIT 20
    `;

        const boNixMetadata = await pgQuery(boNixMetadataQuery);
        console.log(`Found ${boNixMetadata.rows.length} Bo Nix moments in nft_core_metadata_v2:`);
        boNixMetadata.rows.forEach(row => {
            console.log(`  NFT ${row.nft_id}: ${row.first_name} ${row.last_name} #${row.serial_number}`);
            console.log(`    ${row.set_name} (${row.tier})`);
        });
        console.log('\n');

        // Now check which ones are in Kaladin49's wallet
        const walletBoNixQuery = `
      SELECT m.nft_id, m.first_name, m.last_name, m.team_name, m.set_name, m.tier, m.serial_number,
             wh.is_locked, wh.last_event_ts
      FROM nft_core_metadata_v2 m
      INNER JOIN wallet_holdings wh ON m.nft_id = wh.nft_id
      WHERE wh.wallet_address = '0xd3914b2bfb2bd59d'
        AND m.last_name ILIKE '%nix%'
      ORDER BY m.nft_id DESC
    `;

        const walletBoNix = await pgQuery(walletBoNixQuery);
        console.log(`⭐ Found ${walletBoNix.rows.length} Bo Nix moments in Kaladin49's wallet:`);
        if (walletBoNix.rows.length > 0) {
            walletBoNix.rows.forEach(row => {
                console.log(`  - NFT ${row.nft_id}: ${row.first_name} ${row.last_name} #${row.serial_number}`);
                console.log(`    ${row.set_name} (${row.tier}), Locked: ${row.is_locked}`);
                console.log(`    Last event: ${row.last_event_ts}`);
            });
        } else {
            console.log('  ❌ No Bo Nix moments found in wallet!');
        }
        console.log('\n');

        // Check most recent moments in wallet
        const recentWalletQuery = `
      SELECT m.nft_id, m.first_name, m.last_name, m.set_name, m.serial_number, wh.last_event_ts
      FROM wallet_holdings wh
      LEFT JOIN nft_core_metadata_v2 m ON wh.nft_id = m.nft_id
      WHERE wh.wallet_address = '0xd3914b2bfb2bd59d'
      ORDER BY wh.last_event_ts DESC NULLS LAST
      LIMIT 10
    `;

        const recentWallet = await pgQuery(recentWalletQuery);
        console.log(`Most recent 10 moments in Kaladin49's wallet:`);
        recentWallet.rows.forEach((row, i) => {
            const playerName = row.first_name && row.last_name ? `${row.first_name} ${row.last_name}` : 'Unknown Player';
            console.log(`  ${i + 1}. ${playerName} #${row.serial_number || '?'} - ${row.set_name || 'Unknown Set'}`);
            console.log(`      NFT: ${row.nft_id}, Last Event: ${row.last_event_ts || 'N/A'}`);
        });

    } catch (error) {
        console.error('\nError:', error.message);
        console.error(error);
    } finally {
        process.exit();
    }
}

findBoNix();
