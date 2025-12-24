import { pgQuery } from './db.js';
import dotenv from 'dotenv';
dotenv.config({ override: true });

async function testConnection() {
    console.log("Testing DB Connection...");
    console.log("PGHOST:", process.env.PGHOST);
    console.log("PGUSER:", process.env.PGUSER ? "CHECK (Len: " + process.env.PGUSER.length + ")" : "MISSING");
    console.log("PGPASSWORD:", process.env.PGPASSWORD ? "CHECK (Len: " + process.env.PGPASSWORD.length + ")" : "MISSING");
    console.log("PGDATABASE:", process.env.PGDATABASE);
    console.log("PGSSLMODE:", process.env.PGSSLMODE);

    try {
        const res = await pgQuery('SELECT NOW()');
        console.log("Connection Successful! Time:", res.rows[0].now);
    } catch (err) {
        console.error("Connection Failed:", err.message);
        console.error("Code:", err.code);
        if (err.code === '28000') {
            console.error("Auth failed. Check username/password.");
        }
    }
    process.exit(0);
}

testConnection();
