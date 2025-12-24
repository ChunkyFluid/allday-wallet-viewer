import { pgQuery } from './db.js';

async function findLockedMoments() {
    try {
        const myWallet = '0x7541bafd155b683e';

        console.log('=== Finding Locked Moments Issue ===\n');

        // Check database for locked moments
        const dbLockedQuery = await pgQuery(`
      SELECT COUNT(*) as locked_count
      FROM wallet_holdings
      WHERE wallet_address = $1 AND is_locked = true
    `, [myWallet]);

        console.log(`Database shows: ${dbLockedQuery.rows[0].locked_count} locked NFTs`);

        // Check specific Shedeur Sanders #31
        const shedeur31Query = await pgQuery(`
      SELECT wh.nft_id, m.serial_number, m.set_name, wh.is_locked
      FROM wallet_holdings wh
      LEFT JOIN nft_core_metadata_v2 m ON wh.nft_id = m.nft_id
      WHERE wh.wallet_address = $1
        AND m.serial_number = 31
        AND (m.first_name ILIKE '%shedeur%' OR m.last_name ILIKE '%sanders%')
    `, [myWallet]);

        console.log(`\nShedeur Sanders #31 in database:`);
        if (shedeur31Query.rows.length > 0) {
            shedeur31Query.rows.forEach(row => {
                console.log(`  NFT ${row.nft_id}: #${row.serial_number} ${row.set_name}`);
                console.log(`  Locked: ${row.is_locked}`);
            });
        } else {
            console.log('  ❌ NOT FOUND IN DATABASE');
        }

        // Query for Regal Rookies set
        const regalQuery = await pgQuery(`
      SELECT nft_id, first_name, last_name, serial_number, set_name
      FROM nft_core_metadata_v2
      WHERE set_name ILIKE '%regal%'
        AND (first_name ILIKE '%shedeur%' OR last_name ILIKE '%sanders%')
      ORDER BY serial_number
    `, []);

        console.log(`\n\nShedeur Sanders "Regal Rookies" in metadata:`);
        if (regalQuery.rows.length > 0) {
            regalQuery.rows.forEach(row => {
                console.log(`  NFT ${row.nft_id}: ${row.first_name} ${row.last_name} #${row.serial_number}`);
            });
        } else {
            console.log('  ❌ NOT FOUND - metadata may not be synced yet');
        }

        // Check if there are OTHER wallet addresses in the users table
        const usersQuery = await pgQuery(`
      SELECT default_wallet_address, display_name, email
      FROM users
      WHERE email ILIKE '%chunky%' OR display_name ILIKE '%chunky%'
        OR default_wallet_address = $1
    `, [myWallet]);

        console.log(`\n\nUser accounts:`);
        usersQuery.rows.forEach(row => {
            console.log(`  ${row.display_name || row.email}: ${row.default_wallet_address}`);
        });

    } catch (error) {
        console.error('Error:', error.message);
    } finally {
        process.exit();
    }
}

findLockedMoments();
