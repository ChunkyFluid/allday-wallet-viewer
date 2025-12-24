import * as fcl from "@onflow/fcl";
import dotenv from 'dotenv';
dotenv.config();

const ADDRESS = '0xcfd9bad75352b43b';
fcl.config().put("accessNode.api", "https://rest-mainnet.onflow.org");

async function inspectCapabilities() {
    console.log(`Inspecting public paths for ${ADDRESS}...`);

    // Script to list public paths (requires iteration on storage, which we can't do easily on public)
    // But we can check specific paths.

    const script = `
        import NFTLocker from 0xb6f2481eba4df97b
        
        access(all) fun main(address: Address): [String] {
            let account = getAccount(address)
            let paths: [String] = []
            
            // Check standard Locker path
            if account.capabilities.get<&{NFTLocker.LockedCollection}>(NFTLocker.CollectionPublicPath).check() {
                paths.append("NFTLocker.CollectionPublicPath (LockedCollection)")
            } else {
                // Check if it exists but is a different type
                // We can't easily check Generic type without a type argument
                paths.append("NFTLocker.CollectionPublicPath: FAILED CHECK")
            }
            
            // Check via deprecated getCapability to be sure
            // ..
            
            return paths
        }
    `;

    try {
        const results = await fcl.query({
            cadence: script,
            args: (arg, t) => [arg(ADDRESS, t.Address)]
        });
        console.log("Results:", results);
    } catch (err) {
        console.error("Error:", err.message);
    }
    process.exit(0);
}

inspectCapabilities();
