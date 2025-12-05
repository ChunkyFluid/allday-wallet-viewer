// Get metadata for a specific NFT ID
// Based on NFL All Day Cadence documentation: Moment NFT Properties
// https://developers.dapperlabs.com/NFL%20All%20Day/Cadence/Moment%20NFT/Moment%20NFT%20Properties

// Get metadata for a specific NFT ID
// Based on NFL All Day Cadence documentation: Moment NFT Properties
// Updated for Cadence 1.0+ syntax

import AllDay from 0xe4cf4bdc1751c65d
import MetadataViews from 0x1d7e57aa55817448
import NonFungibleToken from 0x1d7e57aa55817448

access(all) fun main(address: Address, nftId: UInt64): {String: String}? {
    let account = getAccount(address)
    
    // Get the AllDay Collection reference
    // In Cadence 1.0+, use capabilities.get<Type>(path)
    let collectionRef = account.capabilities.get<&{NonFungibleToken.CollectionPublic, MetadataViews.ResolverCollection}>(AllDay.CollectionPublicPath)
        .borrow()
        ?? return nil
    
    // Borrow the NFT
    let nft = collectionRef.borrowNFT(id: nftId)
        ?? return nil
    
    // Get metadata views
    let resolver = nft as! &{MetadataViews.Resolver}
    
    // Try to get Display view
    let display = MetadataViews.getDisplay(resolver)
    let displayData: {String: String} = {}
    
    if display != nil {
        if let name = display!.name {
            displayData["name"] = name
        }
        if let description = display!.description {
            displayData["description"] = description
        }
        if let thumbnail = display!.thumbnail {
            displayData["thumbnail"] = thumbnail.url
        }
    }
    
    // Try to get Traits view for edition, play, series, set info
    let traits = MetadataViews.getTraits(resolver)
    if traits != nil {
        for trait in traits! {
            displayData[trait.name] = trait.value.toString()
        }
    }
    
    // Get edition info
    let editions = MetadataViews.getEditions(resolver)
    if editions != nil && editions!.editions.length > 0 {
        let edition = editions!.editions[0]
        displayData["edition_id"] = edition.name
        displayData["max_mint_size"] = edition.maxSize.toString()
        displayData["serial_number"] = edition.serialNumber.toString()
    }
    
    // Get serial number
    let serial = MetadataViews.getSerial(resolver)
    if serial != nil {
        displayData["serial"] = serial!.number.toString()
    }
    
    return displayData
}

