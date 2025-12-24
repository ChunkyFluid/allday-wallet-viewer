import * as fcl from "@onflow/fcl";
import dotenv from 'dotenv';
dotenv.config();

const ADDRESS = '0xcfd9bad75352b43b';
fcl.config().put("accessNode.api", "https://rest-mainnet.onflow.org");

async function outputMarketPaths() {
    console.log(`Inspecting Marketplace paths for ${ADDRESS}...`);

    const script = `
        access(all) fun main(address: Address): [String] {
            let acct = getAccount(address)
            let paths: [String] = []
            
            // Check common Market paths
            // TopShot Market V3 (often used for Dapper Sports)
            let path1 = PublicPath(identifier: "topshotSalev3Collection")!
            if acct.capabilities.get<&AnyResource>(path1).check() {
                paths.append("topshotSalev3Collection")
            }
            
            // Generic Market
            let path2 = PublicPath(identifier: "marketSaleCollection")!
            if acct.capabilities.get<&AnyResource>(path2).check() {
                paths.append("marketSaleCollection")
            }
            
            // AllDay Market (if specific)??
            let path3 = PublicPath(identifier: "allDaySaleCollection")!
             if acct.capabilities.get<&AnyResource>(path3).check() {
                paths.append("allDaySaleCollection")
            }
            
            return paths
        }
    `;

    try {
        const results = await fcl.query({
            cadence: script,
            args: (arg, t) => [arg(ADDRESS, t.Address)]
        });
        console.log("Market Paths Found:", results);
    } catch (err) {
        console.error("Error:", err.message);
    }
    process.exit(0);
}

outputMarketPaths();
