import { pgQuery } from './db.js';
import * as fcl from "@onflow/fcl";
import * as t from "@onflow/types";
import dotenv from 'dotenv';
dotenv.config();

const ADDRESS = '0xcfd9bad75352b43b';

fcl.config().put("accessNode.api", "https://rest-mainnet.onflow.org");

async function checkLocked() {
    const localRes = await pgQuery(`
    SELECT nft_id FROM wallet_holdings 
    WHERE wallet_address = $1 AND is_locked = true
  `, [ADDRESS]);
    const localLockedIds = localRes.rows.map(r => r.nft_id);
    console.log(`Local Locked Count: ${localLockedIds.length}`);

    const script = `
    import NFTLocker from 0xb6f2481eba4df97b
    import AllDay from 0xe4cf4bdc1751c65d
    
    access(all) fun main(address: Address): [UInt64] {
        let account = getAccount(address)
        // Try the legacy interface/method which seemed to work before?
        // It was likely using LockerPublic interface.
        let lockerRef = account.capabilities.get<&{NFTLocker.LockerPublic}>(
            NFTLocker.CollectionPublicPath
        ).borrow()
        
        if lockerRef == nil {
            return []
        }
        
        return lockerRef!.getLockedNFTIDs()
    }
  `;

    try {
        const blockchainLockedIds = await fcl.query({
            cadence: script,
            args: (arg, t) => [arg(ADDRESS, t.Address)]
        });
        const blockchainSet = new Set(blockchainLockedIds.map(id => id.toString()));
        console.log(`Blockchain Locked Count: ${blockchainSet.size}`);

        const ghosts = localLockedIds.filter(id => !blockchainSet.has(id.toString()));
        console.log(`Locked Ghost Count: ${ghosts.length}`);
        if (ghosts.length > 0) {
            console.log(`Sample ghost: ${ghosts[0]}`);
            console.log(`GHOST_LOCKED_IDS: ${JSON.stringify(ghosts)}`);
        }
    } catch (err) {
        console.error(`Blockchain query failed:`, err.message);
    }
    process.exit(0);
}

checkLocked();
