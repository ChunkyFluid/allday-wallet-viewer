import { pgQuery } from "../db.js";

async function checkLocked() {
    const wallet = '0x7541bafd155b683e';

    // Check wallet_holdings
    const wh = await pgQuery(
        `SELECT COUNT(*) as total, 
            COUNT(*) FILTER (WHERE is_locked = true) as locked 
     FROM wallet_holdings WHERE wallet_address = $1`,
        [wallet]
    );
    console.log('wallet_holdings:', wh.rows[0]);

    // Check new holdings table
    const h = await pgQuery(
        `SELECT COUNT(*) as total, 
            COUNT(*) FILTER (WHERE is_locked = true) as locked 
     FROM holdings WHERE wallet_address = $1`,
        [wallet]
    );
    console.log('holdings (new):', h.rows[0]);

    // Check sample of matching NFTs
    const sample = await pgQuery(
        `SELECT wh.nft_id, wh.is_locked 
     FROM wallet_holdings wh 
     WHERE wh.wallet_address = $1 AND wh.is_locked = true
     LIMIT 5`,
        [wallet]
    );
    console.log('Sample locked NFTs from wallet_holdings:', sample.rows);

    // Test the exact query pattern used by server
    const testQuery = await pgQuery(
        `SELECT COUNT(*) as total,
            COUNT(*) FILTER (WHERE COALESCE(h.is_locked, hn.is_locked, false)) as locked
     FROM (SELECT unnest(ARRAY['7532035', '596569', '1392368']::text[]) as nft_id) nft_ids
     LEFT JOIN wallet_holdings h 
       ON h.nft_id = nft_ids.nft_id::text 
       AND LOWER(h.wallet_address) = LOWER('0x7541bafd155b683e')
     LEFT JOIN holdings hn
       ON hn.nft_id = nft_ids.nft_id::text
       AND LOWER(hn.wallet_address) = LOWER('0x7541bafd155b683e')`,
        []
    );
    console.log('Test query result:', testQuery.rows[0]);

    // Check if those specific NFTs are locked
    const lockedCheck = await pgQuery(
        `SELECT nft_id, is_locked FROM wallet_holdings 
     WHERE wallet_address = $1 AND nft_id IN ('7532035', '596569', '1392368')`,
        [wallet]
    );
    console.log('Locked status for sample NFTs:', lockedCheck.rows);

    process.exit(0);
}

checkLocked();
