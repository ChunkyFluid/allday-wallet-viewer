// scripts/build_editions_from_moments_csv.js
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { parse } from "csv-parse";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function fileExists(filePath) {
    try {
        fs.accessSync(filePath, fs.constants.F_OK);
        return true;
    } catch {
        return false;
    }
}

async function buildEditions() {
    const dataDir = path.join(__dirname, "..", "data");
    const momentsPath = path.join(dataDir, "moments.csv");
    const editionsPath = path.join(dataDir, "editions.csv");

    console.log("Looking for moments.csv at:", momentsPath);

    if (!fileExists(momentsPath)) {
        console.error("❌ moments.csv not found. Create/export it first.");
        process.exit(1);
    }

    console.log("Found moments.csv, scanning for unique edition_ids...");

    const editionSet = new Set();

    const stream = fs.createReadStream(momentsPath).pipe(
        parse({
            // Force headers to lowercase: nft_id, edition_id, etc.
            columns: (header) => header.map((h) => String(h).trim().toLowerCase()),
            skip_empty_lines: true,
            trim: true
        })
    );

    let rowCount = 0;

    for await (const row of stream) {
        rowCount += 1;
        if (rowCount === 1) {
            console.log("First row keys:", Object.keys(row));
        }

        const ed = row.edition_id ? String(row.edition_id).trim() : "";

        if (ed) {
            editionSet.add(ed);
        }

        if (rowCount % 100000 === 0) {
            console.log(`Scanned ${rowCount} rows from moments.csv, unique editions so far: ${editionSet.size}`);
        }
    }

    console.log(`Finished scanning. Total rows read: ${rowCount}, unique editions: ${editionSet.size}`);

    console.log("Writing editions.csv to:", editionsPath);

    const out = fs.createWriteStream(editionsPath, { encoding: "utf8" });

    // Header matches your load_editions_from_csv.js expectations
    out.write("edition_id,set_id,set_name,series_id,series_name,tier,max_mint_size\n");

    for (const ed of editionSet) {
        // For now we don’t know set/series/tier/max_mint_size -> leave blank/null
        out.write(`${ed},,,,,,\n`);
    }

    out.end();

    await new Promise((resolve) => out.on("finish", resolve));

    console.log(`✅ editions.csv written with ${editionSet.size} unique edition_ids`);
}

buildEditions().catch((err) => {
    console.error("Unexpected error in buildEditions:", err);
    process.exit(1);
});
