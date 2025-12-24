// Check actual database state for Junglerules
import { pgQuery } from './db.js';
import * as dotenv from 'dotenv';
dotenv.config();

const WALLET = '0xcfd9bad75352b43b';

async function checkDatabaseState() {
    console.log('\n=== DATABASE STATE CHECK ===\n');

    try {
        // Check wallet_holdings table
        const holdingsCount = await pgQuery(
            `SELECT 
                COUNT(*) FILTER (WHERE is_locked = false) as unlocked,
                COUNT(*) FILTER (WHERE is_locked = true) as locked,
                COUNT(*) as total
             FROM wallet_holdings 
             WHERE wallet_address = $1`,
            [WALLET]
        );

        console.log('wallet_holdings table:');
        console.log('  Unlocked:', holdingsCount.rows[0].unlocked);
        console.log('  Locked:', holdingsCount.rows[0].locked);
        console.log('  Total:', holdingsCount.rows[0].total);

        // Check if there's a holdings table too
        try {
            const altCount = await pgQuery(
                `SELECT COUNT(*) as count FROM holdings WHERE owner_address = $1`,
                [WALLET]
            );
            console.log('\nholdings table:', altCount.rows[0].count);
        } catch (e) {
            console.log('\nholdings table: does not exist or no data');
        }

        // Check what the API would return
        console.log('\n=== SIMULATING API QUERY ===\n');

        const apiSimulation = await pgQuery(
            `SELECT 
                COUNT(*) as total_moments,
                COUNT(*) FILTER (WHERE wh.is_locked = false) as unlocked_count,
                COUNT(*) FILTER (WHERE wh.is_locked = true) as locked_count
             FROM wallet_holdings wh
             WHERE wh.wallet_address = $1`,
            [WALLET]
        );

        console.log('API would return:');
        console.log('  Total:', apiSimulation.rows[0].total_moments);
        console.log('  Unlocked:', apiSimulation.rows[0].unlocked_count);
        console.log('  Locked:', apiSimulation.rows[0].locked_count);

        console.log('\n=== EXPECTED (NFL All Day) ===');
        console.log('  Total: 2638');
        console.log('  Unlocked: 1556');
        console.log('  Locked: 1082');

        const diff = parseInt(apiSimulation.rows[0].total_moments) - 2638;
        console.log(`\n=== DISCREPANCY: ${diff} extra moments ===`);

    } catch (err) {
        console.error('Error:', err.message);
    }

    process.exit(0);
}

checkDatabaseState();
