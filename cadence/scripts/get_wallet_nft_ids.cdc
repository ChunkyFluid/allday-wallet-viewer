// Get all NFT IDs owned by a wallet address
// Based on NFL All Day Cadence documentation: Collection NFT IDs
// https://developers.dapperlabs.com/NFL%20All%20Day/Cadence/Moment%20NFT/Collection%20NFT%20IDs

// Get all NFT IDs owned by a wallet address
// Based on NFL All Day Cadence documentation: Collection NFT IDs
// Updated for Cadence 1.0+ syntax

import AllDay from 0xe4cf4bdc1751c65d
import NonFungibleToken from 0x1d7e57aa55817448

access(all) fun main(address: Address): [UInt64] {
    let account = getAccount(address)
    
    // Get the AllDay Collection reference
    // In Cadence 1.0+, use capabilities.get<Type>(path) instead of getCapability
    var collectionRef = account.capabilities.get<&{NonFungibleToken.CollectionPublic}>(AllDay.CollectionPublicPath)
        .borrow()
    
    // If not found, try alternative paths
    if collectionRef == nil {
        // Try /public/AllDayCollection
        collectionRef = account.capabilities.get<&{NonFungibleToken.CollectionPublic}>(/public/AllDayCollection)
            .borrow()
    }
    
    if collectionRef == nil {
        return [] // Return empty array if collection not found
    }
    
    // Get all NFT IDs in the collection
    return collectionRef!.getIDs()
}

