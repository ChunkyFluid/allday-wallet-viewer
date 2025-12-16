-- Migration: Add missing columns to trades table
-- Run this to fix the trading page errors

-- Add missing columns to trades table
DO $$ 
BEGIN
    -- Add initiator_child_wallet if not exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'trades' AND column_name = 'initiator_child_wallet') THEN
        ALTER TABLE trades ADD COLUMN initiator_child_wallet TEXT;
    END IF;
    
    -- Add target_child_wallet if not exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'trades' AND column_name = 'target_child_wallet') THEN
        ALTER TABLE trades ADD COLUMN target_child_wallet TEXT;
    END IF;
    
    -- Add initiator_signed if not exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'trades' AND column_name = 'initiator_signed') THEN
        ALTER TABLE trades ADD COLUMN initiator_signed BOOLEAN DEFAULT FALSE;
    END IF;
    
    -- Add target_signed if not exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'trades' AND column_name = 'target_signed') THEN
        ALTER TABLE trades ADD COLUMN target_signed BOOLEAN DEFAULT FALSE;
    END IF;
    
    -- Add initiator_nft_ids if not exists (JSONB format)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'trades' AND column_name = 'initiator_nft_ids') THEN
        ALTER TABLE trades ADD COLUMN initiator_nft_ids JSONB DEFAULT '[]';
    END IF;
    
    -- Add target_nft_ids if not exists (JSONB format)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'trades' AND column_name = 'target_nft_ids') THEN
        ALTER TABLE trades ADD COLUMN target_nft_ids JSONB DEFAULT '[]';
    END IF;
END $$;

-- Show current table structure
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'trades' 
ORDER BY ordinal_position;
