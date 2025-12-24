import { pgQuery } from './db.js';

async function checkUserStructure() {
    try {
        // Check users table structure
        const usersColsQuery = `
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_name = 'users'
      ORDER BY ordinal_position
    `;

        const usersCols = await pgQuery(usersColsQuery);
        console.log('Users table columns:');
        usersCols.rows.forEach(r => console.log(`  - ${r.column_name}: ${r.data_type}`));
        console.log('\n');

        // Search for Kaladin49 in users table
        const usersQuery = `
      SELECT *
      FROM users
      WHERE default_wallet_address = '0xd3914b2bfb2bd59d'
      LIMIT 5
    `;

        const users = await pgQuery(usersQuery);
        console.log(`Found ${users.rows.length} users with wallet 0xd3914b2bfb2bd59d:`);
        if (users.rows.length > 0) {
            console.log(JSON.stringify(users.rows, null, 2));
        }

        // Check wallet_profiles
        const profilesQuery = `
      SELECT *
      FROM wallet_profiles
      WHERE wallet_address = '0xd3914b2bfb2bd59d'
    `;

        const profiles = await pgQuery(profilesQuery);
        console.log(`\nWallet profiles for 0xd3914b2bfb2bd59d:`);
        if (profiles.rows.length > 0) {
            console.log(JSON.stringify(profiles.rows, null, 2));
        } else {
            console.log('  No profile found');
        }

        // Check if wallet has ANY NFTs in database
        const anyNftsQuery = `
      SELECT COUNT(*) as count
      FROM wallet_holdings
      WHERE wallet_address = '0xd3914b2bfb2bd59d'
    `;

        const anyNfts = await pgQuery(anyNftsQuery);
        console.log(`\nNFTs in database for this wallet: ${anyNfts.rows[0].count}`);

    } catch (error) {
        console.error('\nError:', error.message);
    } finally {
        process.exit();
    }
}

checkUserStructure();
