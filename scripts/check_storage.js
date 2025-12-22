import { pgQuery } from '../db.js';

async function checkStorage() {
    try {
        console.log('\n--- TOP 10 TABLES BY SIZE ---\n');
        const query = `
            SELECT
                relname AS table_name,
                pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
                pg_size_pretty(pg_relation_size(relid)) AS table_size,
                pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) AS index_size
            FROM pg_catalog.pg_statio_user_tables
            ORDER BY pg_total_relation_size(relid) DESC
            LIMIT 10;
        `;
        const res = await pgQuery(query);
        console.table(res.rows);

        console.log('\n--- OVERALL DATABASE SIZE ---\n');
        const dbSizeQuery = `SELECT pg_size_pretty(pg_database_size(current_database())) as db_size`;
        const dbRes = await pgQuery(dbSizeQuery);
        console.log(`Current Database Size: ${dbRes.rows[0].db_size}`);

        process.exit(0);
    } catch (err) {
        console.error(err);
        process.exit(1);
    }
}

checkStorage();
