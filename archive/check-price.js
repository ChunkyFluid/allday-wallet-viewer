import { pgQuery } from "./db.js";

async function checkPrice() {
    try {
        const res = await pgQuery("SELECT * FROM edition_price_scrape WHERE edition_id = '5175'");
        console.log(JSON.stringify(res.rows, null, 2));
    } catch (err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}

checkPrice();
