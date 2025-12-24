import { pgQuery } from "./db.js";

async function checkSchema() {
    try {
        const res = await pgQuery("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'nft_core_metadata_v2'");
        console.table(res.rows);
    } catch (err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}

checkSchema();
