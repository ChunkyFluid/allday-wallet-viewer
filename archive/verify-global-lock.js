import * as fcl from "@onflow/fcl";
import dotenv from 'dotenv';
dotenv.config();

// One of the "valid" locked IDs (from my manual checks or just random pick)
// Wait, I don't know which one is valid vs ghost yet.
// I'll pick a few from the 1131 list.
const IDS = ['1008827', '5647407', '7059723'];

fcl.config().put("accessNode.api", "https://rest-mainnet.onflow.org");

async function checkGlobalLock() {
    console.log(`Checking Global NFTLocker for IDs: ${IDS.join(', ')}`);

    // We access the contract-level function via a script
    // access(all) view fun getNFTLockerDetails(id: UInt64, nftType: Type): NFTLocker.LockedData?

    const script = `
        import NFTLocker from 0xb6f2481eba4df97b
        import AllDay from 0xe4cf4bdc1751c65d
        import NonFungibleToken from 0x1d7e57aa55817448
        
        access(all) fun main(ids: [UInt64]): {UInt64: String} {
            let res: {UInt64: String} = {}
            let type1 = Type<@AllDay.NFT>()
            let type2 = Type<@NonFungibleToken.NFT>()
            
            for id in ids {
                let d1 = NFTLocker.getNFTLockerDetails(id: id, nftType: type1)
                if d1 != nil {
                   res[id] = "LOCKED (AllDay)"
                   continue
                }
                
                let d2 = NFTLocker.getNFTLockerDetails(id: id, nftType: type2)
                if d2 != nil {
                   res[id] = "LOCKED (Generic)"
                   continue
                }
                
                res[id] = "NOT LOCKED"
            }
            return res
        }
    `;

    try {
        const results = await fcl.query({
            cadence: script,
            args: (arg, t) => [arg(IDS.map(String), t.Array(t.UInt64))]
        });

        console.log("Results:", results);
    } catch (err) {
        console.error("Error:", err.message);
    }
    process.exit(0);
}

checkGlobalLock();
