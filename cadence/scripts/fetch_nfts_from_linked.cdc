// Fetch NFTs from linked child accounts that the parent can control
// Parent wallet can discover and potentially withdraw NFTs from linked Dapper wallets

import HybridCustody from 0xd8a7e05a7ac670c0
import NonFungibleToken from 0x1d7e57aa55817448
import AllDay from 0xe4cf4bdc1751c65d
import MetadataViews from 0x1d7e57aa55817448

// Returns a mapping of child address -> array of NFT IDs that parent can access
access(all) fun main(parent: Address): {Address: [UInt64]} {
    let acct = getAuthAccount<auth(Storage) &Account>(parent)
    
    let manager = acct.storage.borrow<auth(HybridCustody.Manage) &HybridCustody.Manager>(
        from: HybridCustody.ManagerStoragePath
    )
    
    if manager == nil {
        return {} // No linked accounts
    }
    
    var result: {Address: [UInt64]} = {}
    let providerType = Type<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>()
    
    // Iterate through all child addresses
    for childAddress in manager!.getChildAddresses() {
        let childAcct = getAuthAccount<auth(Storage, Capabilities) &Account>(childAddress)
        let childAccount = manager!.borrowAccount(addr: childAddress)
        
        if childAccount == nil {
            continue
        }
        
        var nftIds: [UInt64] = []
        
        // Check storage paths for AllDay NFT collections
        for storagePath in childAcct.storage.storagePaths {
            // Try to borrow as AllDay Collection
            if let collection = childAcct.storage.borrow<&AllDay.Collection>(from: storagePath) {
                // Check if parent has capability to withdraw from this collection
                for controller in childAcct.capabilities.storage.getControllers(forPath: storagePath) {
                    if !controller.borrowType.isSubtype(of: providerType) {
                        continue
                    }
                    
                    // Check if we can access this capability from the child account
                    if let cap = childAccount!.getCapability(controllerID: controller.capabilityID, type: providerType) {
                        let providerCap = cap as! Capability<&{NonFungibleToken.Provider}>
                        
                        if providerCap.check() {
                            // We have access! Get all NFT IDs
                            nftIds = collection.getIDs()
                            break
                        }
                    }
                }
            }
        }
        
        if nftIds.length > 0 {
            result[childAddress] = nftIds
        }
    }
    
    return result
}
