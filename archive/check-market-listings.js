import * as fcl from "@onflow/fcl";
import dotenv from 'dotenv';
dotenv.config();

const ADDRESS = '0xcfd9bad75352b43b';
fcl.config().put("accessNode.api", "https://rest-mainnet.onflow.org");

// TopShot market address
const MARKET_ADDRESS = "0xc1e4f4f4c4257510";

async function checkMarketListings() {
    console.log(`Checking Marketplace Listings for ${ADDRESS}...`);

    // Script to get IDs from TopShot Market V3 using Market.SalePublic interface
    const script = `
        import Market from 0xc1e4f4f4c4257510
        import TopShotMarketV3 from 0xc1e4f4f4c4257510
        
        access(all) fun main(address: Address): [UInt64] {
            let acct = getAccount(address)
            // Use the path we found earlier: /public/topshotSalev3Collection
            let path = PublicPath(identifier: "topshotSalev3Collection")!
            
            // Borrow as Market.SalePublic which defines getIDs()
            let cap = acct.capabilities.get<&{Market.SalePublic}>(path)
            
            if !cap.check() { return [] }
            
            return cap.borrow()!.getIDs()
        }
    `;

    try {
        const ids = await fcl.query({
            cadence: script,
            args: (arg, t) => [arg(ADDRESS, t.Address)]
        });
        console.log(`Found ${ids.length} items for sale in TopShotMarketV3.`);
        if (ids.length > 0) {
            console.log("Sample IDs:", ids.slice(0, 10));
        }

        // Check if ANY of these match our missing "Locked" list?
        // I'll print them all and maybe copy-paste or just count them.
        // The missing count is ~1082 (Locked) + ?? (Ghost).
        // If I see IDs that were "Locked" locally, then successful match!

        // I'll iterate through a known list of "Locked" IDs from DB?
        // I don't have that loaded here. I'll just output the count.

    } catch (err) {
        console.error("Error:", err.message);
    }
    process.exit(0);
}

checkMarketListings();
