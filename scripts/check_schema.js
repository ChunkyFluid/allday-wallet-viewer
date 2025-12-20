import { pgQuery } from '../db.js';

async function checkSchema() {
    try {
        console.log('--- TABLE: wallet_holdings ---');
        const whC = await pgQuery("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'wallet_holdings'");
        console.log(whC.rows);

        console.log('\n--- TABLE: holdings ---');
        const hC = await pgQuery("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'holdings'");
        console.log(hC.rows);

        console.log('\n--- TABLE: nfts ---');
        const nftsC = await pgQuery("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'nfts'");
        console.log(nftsC.rows);

        console.log('\n--- TABLE: nft_core_metadata_v2 ---');
        const metadataC = await pgQuery("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'nft_core_metadata_v2'");
        console.log(metadataC.rows);

        process.exit(0);
    } catch (err) {
        console.error(err);
        process.exit(1);
    }
}

checkSchema();
