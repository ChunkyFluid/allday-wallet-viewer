# NFL All Day – Wallet Viewer

A tiny full‑stack app that lets a user paste a Flow wallet address (e.g., `0x7541bafd155b683e`), runs the Snowflake query, and renders results.

## Quick Start
```bash
npm install
cp .env.example .env
# Fill in .env with your Snowflake creds
npm run dev
# open http://localhost:3000
```

**Tech**
- Backend: Node.js + Express + snowflake-sdk
- Frontend: Static HTML + Tailwind (CDN) + vanilla JS fetch()
- Address is bound as a SQL parameter in 4 places (no string interpolation).
