import * as fcl from "@onflow/fcl";
import { getWalletNFTIds } from './services/flow-blockchain.js';
import dotenv from 'dotenv';
dotenv.config();

const ADDRESS = '0xcfd9bad75352b43b';
fcl.config().put("accessNode.api", "https://rest-mainnet.onflow.org");

async function checkChildAccounts() {
    console.log(`Checking child accounts for ${ADDRESS}...`);

    // Script to get child addresses via HybridCustody
    // Using address 0xd8a7e05a7ac670c0 as per local file
    const script = `
        import HybridCustody from 0xd8a7e05a7ac670c0
        
        access(all) fun main(parent: Address): [Address] {
            let acct = getAccount(parent)
            
            // Check if the account has a HybridCustody Manager via Public capability!
            // Wait, manager is usually in Storage. Public capability exposes ManagerPublic.
            let manager = acct.capabilities.get<&{HybridCustody.ManagerPublic}>(
                HybridCustody.ManagerPublicPath
            ).borrow()
            
            if manager == nil {
                return [] 
            }
            
            return manager!.getChildAddresses()
        }
    `;

    try {
        const childAddresses = await fcl.query({
            cadence: script,
            args: (arg, t) => [arg(ADDRESS, t.Address)]
        });
        console.log(`Found ${childAddresses.length} child accounts.`);

        if (childAddresses.length > 0) {
            console.log(`Child Addresses: ${childAddresses.join(', ')}`);

            let totalChildMoments = 0;
            for (const child of childAddresses) {
                console.log(`\nChecking moments in child ${child}...`);
                const ids = await getWalletNFTIds(child);
                console.log(`  Found ${ids.length} moments.`);
                totalChildMoments += ids.length;
            }
            console.log(`\nTotal moments in child accounts: ${totalChildMoments}`);
        }
    } catch (err) {
        console.error("Error checking child accounts:", err.message);
    }
    process.exit(0);
}

checkChildAccounts();
