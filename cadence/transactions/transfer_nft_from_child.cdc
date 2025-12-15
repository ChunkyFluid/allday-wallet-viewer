// Transfer AllDay NFT from a linked child account (Dapper) to another address
// The parent (Flow wallet) signs this transaction to move NFT from their linked Dapper wallet

import HybridCustody from 0xd8a7e05a7ac670c0
import NonFungibleToken from 0x1d7e57aa55817448
import AllDay from 0xe4cf4bdc1751c65d

transaction(childAddress: Address, nftId: UInt64, recipientAddress: Address) {
    
    let providerCap: Capability<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>
    let receiverRef: &{NonFungibleToken.CollectionPublic}
    
    prepare(parent: auth(Storage) &Account) {
        // Borrow the HybridCustody Manager
        let manager = parent.storage.borrow<auth(HybridCustody.Manage) &HybridCustody.Manager>(
            from: HybridCustody.ManagerStoragePath
        ) ?? panic("Could not borrow HybridCustody Manager - no linked accounts")
        
        // Borrow the child account
        let childAccount = manager.borrowAccount(addr: childAddress) 
            ?? panic("Child account not found or not linked")
        
        // Get the AllDay collection storage path
        let childAcct = getAuthAccount<auth(Storage, Capabilities) &Account>(childAddress)
        
        // Find the capability for NFT withdrawal
        let providerType = Type<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>()
        var foundCap: Capability<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>? = nil
        
        // Iterate through storage to find the AllDay collection with provider capability
        for storagePath in childAcct.storage.storagePaths {
            if childAcct.storage.borrow<&AllDay.Collection>(from: storagePath) != nil {
                for controller in childAcct.capabilities.storage.getControllers(forPath: storagePath) {
                    if !controller.borrowType.isSubtype(of: providerType) {
                        continue
                    }
                    
                    if let cap = childAccount.getCapability(controllerID: controller.capabilityID, type: providerType) {
                        let typedCap = cap as! Capability<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>
                        if typedCap.check() {
                            foundCap = typedCap
                            break
                        }
                    }
                }
            }
            if foundCap != nil {
                break
            }
        }
        
        self.providerCap = foundCap ?? panic("Could not find NFT provider capability for child account")
        
        // Get reference to recipient's collection
        self.receiverRef = getAccount(recipientAddress)
            .capabilities.get<&{NonFungibleToken.CollectionPublic}>(AllDay.CollectionPublicPath)
            .borrow()
            ?? panic("Could not borrow recipient's NFT collection")
    }
    
    execute {
        // Withdraw NFT from child account
        let provider = self.providerCap.borrow() 
            ?? panic("Could not borrow NFT provider")
        let nft <- provider.withdraw(withdrawID: nftId)
        
        // Deposit to recipient
        self.receiverRef.deposit(token: <-nft)
    }
    
    post {
        // Verify NFT was transferred
        true
    }
}
