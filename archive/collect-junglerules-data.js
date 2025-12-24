import { pgQuery } from './db.js';
import snowflake from 'snowflake-sdk';
import * as fcl from "@onflow/fcl";
import dotenv from 'dotenv';
import fs from 'fs';
dotenv.config();

const ADDRESS = '0xcfd9bad75352b43b';
fcl.config().put("accessNode.api", "https://rest-mainnet.onflow.org");

async function collectJunglerulesData() {
    console.log("Collecting data for Junglerules...");
    const results = {
        unlocked: { local: [], snowflake: [], ghosts: [], missing: [] },
        locked: { local: [], blockchain: [], ghosts: [] }
    };

    // 1. Unlocked
    const localUnlockedRes = await pgQuery(`SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1 AND is_locked = false`, [ADDRESS]);
    results.unlocked.local = localUnlockedRes.rows.map(r => r.nft_id);

    const cfg = {
        account: process.env.SNOWFLAKE_ACCOUNT,
        username: process.env.SNOWFLAKE_USERNAME,
        password: process.env.SNOWFLAKE_PASSWORD,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE,
        database: process.env.SNOWFLAKE_DATABASE,
        schema: 'ALLDAY',
        role: process.env.SNOWFLAKE_ROLE
    };
    const conn = snowflake.createConnection(cfg);
    await new Promise((res, rej) => conn.connect(err => err ? rej(err) : res()));

    const sfRows = await new Promise((res, rej) => {
        conn.execute({
            sqlText: `SELECT NFT_ID FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_WALLET_HOLDINGS_CURRENT WHERE WALLET_ADDRESS = '${ADDRESS}'`,
            complete: (err, stmt, rows) => err ? rej(err) : res(rows)
        });
    });
    results.unlocked.snowflake = sfRows.map(r => r.NFT_ID);
    conn.destroy(() => { });

    const localUnlockedSet = new Set(results.unlocked.local);
    const sfUnlockedSet = new Set(results.unlocked.snowflake);

    results.unlocked.ghosts = results.unlocked.local.filter(id => !sfUnlockedSet.has(id));
    results.unlocked.missing = results.unlocked.snowflake.filter(id => !localUnlockedSet.has(id));

    // 2. Locked
    const localLockedRes = await pgQuery(`SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1 AND is_locked = true`, [ADDRESS]);
    results.locked.local = localLockedRes.rows.map(r => r.nft_id);

    const script = `
        import NFTLocker from 0xb6f2481eba4df97b
        import AllDay from 0xe4cf4bdc1751c65d
        access(all) fun main(address: Address): [UInt64] {
            let account = getAccount(address)
            let lockerRef = account.capabilities.get<&{NFTLocker.LockedCollection}>(NFTLocker.CollectionPublicPath).borrow()
            if lockerRef == nil { return [] }
            let allDayType = Type<@AllDay.NFT>()
            return lockerRef!.getIDs(nftType: allDayType) ?? []
        }
    `;
    const bcIds = await fcl.query({ cadence: script, args: (arg, t) => [arg(ADDRESS, t.Address)] });
    results.locked.blockchain = bcIds.map(id => id.toString());

    const bcLockedSet = new Set(results.locked.blockchain);
    results.locked.ghosts = results.locked.local.filter(id => !bcLockedSet.has(id));

    fs.writeFileSync('junglerules_audit_results.json', JSON.stringify(results, null, 2));
    console.log("Results saved to junglerules_audit_results.json");
    process.exit(0);
}

collectJunglerulesData().catch(err => {
    console.error(err);
    process.exit(1);
});
