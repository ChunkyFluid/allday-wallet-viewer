// Get all locked NFT IDs for a wallet from NFTLocker contract
// The NFTLocker stores locked NFTs separately from the main wallet
// This script queries for AllDay NFTs specifically

import NFTLocker from 0xb6f2481eba4df97b
import AllDay from 0xe4cf4bdc1751c65d

access(all) fun main(address: Address): [UInt64] {
    let account = getAccount(address)
    
    // Try to borrow the locked collection
    let lockerRef = account.capabilities.get<&{NFTLocker.LockedCollection}>(
        NFTLocker.CollectionPublicPath
    ).borrow()
    
    if lockerRef == nil {
        return []
    }
    
    // Get locked IDs for AllDay NFT type specifically
    let allDayType = Type<@AllDay.NFT>()
    let lockedIds = lockerRef!.getIDs(nftType: allDayType)
    
    return lockedIds ?? []
}
