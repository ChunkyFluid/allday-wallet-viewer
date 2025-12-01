-- Make password_hash nullable to support Flow wallet and OAuth logins
-- These authentication methods don't use passwords

ALTER TABLE public.users 
ALTER COLUMN password_hash DROP NOT NULL;

