// Atomic P2P NFT Swap between two parties using Hybrid Custody
// Both parties sign this transaction to swap NFTs from their linked Dapper wallets
// 
// Flow: 
// 1. Party A creates trade offer (off-chain)
// 2. Party B accepts and both parties sign this transaction
// 3. NFTs are swapped atomically on-chain

import HybridCustody from 0xd8a7e05a7ac670c0
import NonFungibleToken from 0x1d7e57aa55817448
import AllDay from 0xe4cf4bdc1751c65d

transaction(
    // Party A (initiator) parameters
    partyAChildAddress: Address,
    partyANftIds: [UInt64],
    // Party B (acceptor) parameters  
    partyBChildAddress: Address,
    partyBNftIds: [UInt64]
) {
    
    let providerCapA: Capability<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>
    let providerCapB: Capability<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>
    let receiverRefA: &{NonFungibleToken.CollectionPublic}
    let receiverRefB: &{NonFungibleToken.CollectionPublic}
    
    prepare(partyA: auth(Storage) &Account, partyB: auth(Storage) &Account) {
        let providerType = Type<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>()
        
        // === PARTY A SETUP ===
        let managerA = partyA.storage.borrow<auth(HybridCustody.Manage) &HybridCustody.Manager>(
            from: HybridCustody.ManagerStoragePath
        ) ?? panic("Party A: Could not borrow HybridCustody Manager")
        
        let childAccountA = managerA.borrowAccount(addr: partyAChildAddress) 
            ?? panic("Party A: Child account not linked")
        
        let childAcctA = getAuthAccount<auth(Storage, Capabilities) &Account>(partyAChildAddress)
        
        // Find Party A's provider capability
        var foundCapA: Capability<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>? = nil
        for storagePath in childAcctA.storage.storagePaths {
            if childAcctA.storage.borrow<&AllDay.Collection>(from: storagePath) != nil {
                for controller in childAcctA.capabilities.storage.getControllers(forPath: storagePath) {
                    if controller.borrowType.isSubtype(of: providerType) {
                        if let cap = childAccountA.getCapability(controllerID: controller.capabilityID, type: providerType) {
                            let typedCap = cap as! Capability<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>
                            if typedCap.check() {
                                foundCapA = typedCap
                                break
                            }
                        }
                    }
                }
            }
            if foundCapA != nil { break }
        }
        self.providerCapA = foundCapA ?? panic("Party A: No NFT provider capability")
        
        // Party A receives NFTs at their child address
        self.receiverRefA = getAccount(partyAChildAddress)
            .capabilities.get<&{NonFungibleToken.CollectionPublic}>(AllDay.CollectionPublicPath)
            .borrow()
            ?? panic("Party A: Could not borrow collection")
        
        // === PARTY B SETUP ===
        let managerB = partyB.storage.borrow<auth(HybridCustody.Manage) &HybridCustody.Manager>(
            from: HybridCustody.ManagerStoragePath
        ) ?? panic("Party B: Could not borrow HybridCustody Manager")
        
        let childAccountB = managerB.borrowAccount(addr: partyBChildAddress) 
            ?? panic("Party B: Child account not linked")
        
        let childAcctB = getAuthAccount<auth(Storage, Capabilities) &Account>(partyBChildAddress)
        
        // Find Party B's provider capability
        var foundCapB: Capability<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>? = nil
        for storagePath in childAcctB.storage.storagePaths {
            if childAcctB.storage.borrow<&AllDay.Collection>(from: storagePath) != nil {
                for controller in childAcctB.capabilities.storage.getControllers(forPath: storagePath) {
                    if controller.borrowType.isSubtype(of: providerType) {
                        if let cap = childAccountB.getCapability(controllerID: controller.capabilityID, type: providerType) {
                            let typedCap = cap as! Capability<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>
                            if typedCap.check() {
                                foundCapB = typedCap
                                break
                            }
                        }
                    }
                }
            }
            if foundCapB != nil { break }
        }
        self.providerCapB = foundCapB ?? panic("Party B: No NFT provider capability")
        
        // Party B receives NFTs at their child address
        self.receiverRefB = getAccount(partyBChildAddress)
            .capabilities.get<&{NonFungibleToken.CollectionPublic}>(AllDay.CollectionPublicPath)
            .borrow()
            ?? panic("Party B: Could not borrow collection")
    }
    
    execute {
        let providerA = self.providerCapA.borrow() ?? panic("Could not borrow Party A provider")
        let providerB = self.providerCapB.borrow() ?? panic("Could not borrow Party B provider")
        
        // Withdraw Party A's NFTs and deposit to Party B
        for nftId in partyANftIds {
            let nft <- providerA.withdraw(withdrawID: nftId)
            self.receiverRefB.deposit(token: <-nft)
        }
        
        // Withdraw Party B's NFTs and deposit to Party A
        for nftId in partyBNftIds {
            let nft <- providerB.withdraw(withdrawID: nftId)
            self.receiverRefA.deposit(token: <-nft)
        }
    }
    
    post {
        // Swap completed atomically
        true
    }
}
