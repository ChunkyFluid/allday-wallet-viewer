# Authentication Setup Guide

This application now supports three authentication methods:
1. **Email/Password** - Traditional authentication
2. **Flow Wallet** - Dapper, Blocto, Ledger, and other Flow-compatible wallets
3. **Google OAuth** - Google account sign-in

## Environment Variables Required

Add these to your `.env` file:

```env
# Google OAuth (optional - only needed for Google sign-in)
GOOGLE_CLIENT_ID=your_google_client_id
GOOGLE_CLIENT_SECRET=your_google_client_secret
GOOGLE_CALLBACK_URL=http://localhost:3000/api/auth/google/callback

# Session secret (required)
SESSION_SECRET=your_random_session_secret_here
```

## Google OAuth Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the Google+ API
4. Go to "Credentials" → "Create Credentials" → "OAuth 2.0 Client ID"
5. Set application type to "Web application"
6. Add authorized redirect URI: `http://localhost:3000/api/auth/google/callback` (or your production URL)
7. Copy the Client ID and Client Secret to your `.env` file

## Flow Wallet Authentication

Flow wallet authentication uses FCL (Flow Client Library) and works automatically with:
- **Dapper Wallet** - Main Flow wallet
- **Blocto** - Popular Flow wallet
- **Ledger** - Hardware wallet support
- Any other FCL-compatible wallet

No additional configuration needed - it uses Flow's discovery service to find available wallets.

## Installation

Run this command to install the required packages:

```bash
npm install @onflow/fcl @onflow/types passport passport-google-oauth20
```

## How It Works

### Flow Wallet Login
1. User clicks "Connect Flow Wallet" button
2. FCL shows wallet discovery modal
3. User selects their wallet (Dapper, Blocto, etc.)
4. User approves connection in their wallet
5. Wallet address is sent to `/api/login-flow`
6. Backend creates/updates user with `flow:wallet_address` as email
7. Session is created and user is logged in

### Google Login
1. User clicks "Continue with Google" button
2. Redirects to Google OAuth consent screen
3. User approves
4. Google redirects back to `/api/auth/google/callback`
5. Backend creates/updates user with `google:google_id` as email
6. Session is created and user is redirected to home page

### Email/Password Login
1. User enters email and password
2. Backend verifies credentials
3. Session is created and user is logged in

## User Storage

All authentication methods store users in the `public.users` table:
- Email format: `flow:0x...` for Flow wallets, `google:123456` for Google, regular email for email/password
- `password_hash` is NULL for OAuth/wallet logins
- `default_wallet_address` is set for Flow wallet logins

## Testing

1. **Flow Wallet**: Click "Connect Flow Wallet" and select a wallet from the modal
2. **Google**: Click "Continue with Google" (requires Google OAuth setup)
3. **Email/Password**: Use existing login form

