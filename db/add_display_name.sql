-- Add display_name column to users table if it doesn't exist
ALTER TABLE public.users 
ADD COLUMN IF NOT EXISTS display_name VARCHAR(50);

-- Add created_at column if it doesn't exist (should already be there)
ALTER TABLE public.users 
ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();

