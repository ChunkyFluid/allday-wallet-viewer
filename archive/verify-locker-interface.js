import * as fcl from "@onflow/fcl";
import dotenv from 'dotenv';
dotenv.config();

const ADDRESS = '0xcfd9bad75352b43b';
fcl.config().put("accessNode.api", "https://rest-mainnet.onflow.org");

async function verifyInterface() {
    console.log(`Testing Locker Interfaces for ${ADDRESS}...`);

    // Test 1: Concrete Type
    const script1 = `
        import NFTLocker from 0xb6f2481eba4df97b
        import AllDay from 0xe4cf4bdc1751c65d
        
        access(all) fun main(address: Address): [UInt64] {
            let acct = getAccount(address)
            let cap = acct.capabilities.get<&NFTLocker.Collection>(NFTLocker.CollectionPublicPath)
            if !cap.check() { return [] }
            let ref = cap.borrow()!
            let allDayType = Type<@AllDay.NFT>()
            return ref.getIDs(nftType: allDayType) ?? []
        }
    `;

    // Test 2: LockerPublic
    const script2 = `
        import NFTLocker from 0xb6f2481eba4df97b
        import AllDay from 0xe4cf4bdc1751c65d
        
        access(all) fun main(address: Address): [UInt64] {
            let acct = getAccount(address)
            let cap = acct.capabilities.get<&{NFTLocker.LockerPublic}>(NFTLocker.CollectionPublicPath)
            if !cap.check() { 
                // Try old path? LockerPublicPath
                let cap2 = acct.capabilities.get<&{NFTLocker.LockerPublic}>(NFTLocker.LockerPublicPath)
                if !cap2.check() { return [] }
                return cap2.borrow()!.getLockedNFTIDs() ?? []
            }
            return cap.borrow()!.getLockedNFTIDs() ?? []
        }
    `;

    try {
        console.log("Attempting Concrete Type check...");
        const ids1 = await fcl.query({ cadence: script1, args: (arg, t) => [arg(ADDRESS, t.Address)] });
        console.log("Concrete Type IDs:", ids1.length);
    } catch (e) { console.log("Concrete Type failed:", e.message); }

    try {
        console.log("Attempting LockerPublic check...");
        const ids2 = await fcl.query({ cadence: script2, args: (arg, t) => [arg(ADDRESS, t.Address)] });
        console.log("LockerPublic IDs:", ids2.length);
    } catch (e) { console.log("LockerPublic failed:", e.message); }

    process.exit(0);
}

verifyInterface();
