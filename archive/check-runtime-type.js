import * as fcl from "@onflow/fcl";
import dotenv from 'dotenv';
dotenv.config();

const ADDRESS = '0xcfd9bad75352b43b';
fcl.config().put("accessNode.api", "https://rest-mainnet.onflow.org");

async function checkRuntimeType() {
    console.log(`Checking runtime type of Locked Collection at ${ADDRESS}...`);

    // Using AnyResource to inspect type
    const script = `
        import NFTLocker from 0xb6f2481eba4df97b
        
        access(all) fun main(address: Address): String {
            let account = getAccount(address)
            let cap = account.capabilities.get<&AnyResource>(NFTLocker.CollectionPublicPath)
            
            if !cap.check() {
                return "Capability Invalid or path empty"
            }
            
            let ref = cap.borrow()
            if ref == nil {
                return "Borrow failed (nil)"
            }
            
            return ref!.getType().identifier
        }
    `;

    try {
        const typeId = await fcl.query({
            cadence: script,
            args: (arg, t) => [arg(ADDRESS, t.Address)]
        });
        console.log("Runtime Type:", typeId);
    } catch (err) {
        console.error("Error:", err.message);
    }
    process.exit(0);
}

checkRuntimeType();
