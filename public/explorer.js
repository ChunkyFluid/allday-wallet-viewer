document.addEventListener("DOMContentLoaded", () => {
    const playerInput = document.getElementById("ex-player");
    const teamInput = document.getElementById("ex-team");
    const tierSelect = document.getElementById("ex-tier");
    const seriesInput = document.getElementById("ex-series");
    const setInput = document.getElementById("ex-set");
    const searchBtn = document.getElementById("ex-search-btn");

    const table = document.getElementById("ex-table");
    const tbody = document.getElementById("ex-tbody");
    const empty = document.getElementById("ex-empty");
    const summary = document.getElementById("ex-results-summary");

    function buildQuery() {
        const params = new URLSearchParams();
        const player = playerInput.value.trim();
        const team = teamInput.value.trim();
        const tier = tierSelect.value.trim();
        const series = seriesInput.value.trim();
        const set = setInput.value.trim();

        if (player) params.set("player", player);
        if (team) params.set("team", team);
        if (tier) params.set("tier", tier);
        if (series) params.set("series", series);
        if (set) params.set("set", set);
        params.set("limit", "200");

        return params.toString();
    }

    function renderRows(rows) {
        tbody.innerHTML = "";
        if (!rows || !rows.length) {
            table.style.display = "none";
            empty.style.display = "block";
            empty.textContent = "No moments matched your filters.";
            return;
        }

        empty.style.display = "none";
        table.style.display = "table";

        rows.forEach((r) => {
            const tr = document.createElement("tr");
            const playerName = [r.first_name, r.last_name].filter(Boolean).join(" ");
            const serial = r.serial_number != null ? r.serial_number : "";
            const maxMint = r.max_mint_size != null ? r.max_mint_size : "";
            const serialText = serial || maxMint ? `${serial || "–"} / ${maxMint || "?"}` : "";

            const setName = r.set_name || "";
            const seriesName = r.series_name || "";
            const tier = r.tier || "";

            const marketUrl = r.edition_id ? `https://nflallday.com/listing/moment/${r.edition_id}` : "";
            const momentUrl = r.nft_id ? `https://nflallday.com/moments/${r.nft_id}` : "";

            tr.innerHTML = `
        <td><code>${r.nft_id || ""}</code></td>
        <td>${playerName || "<span style='color:var(--text-muted);font-style:italic;'>Unknown</span>"}</td>
        <td>${r.team_name || ""}</td>
        <td>${r.position || ""}</td>
        <td>${tier}</td>
        <td>${serialText}</td>
        <td>${seriesName}</td>
        <td>${setName}</td>
        <td>
          ${momentUrl ? `<a href="${momentUrl}" target="_blank" rel="noopener">Moment</a>` : ""}
          ${marketUrl ? ` · <a href="${marketUrl}" target="_blank" rel="noopener">Market</a>` : ""}
        </td>
      `;
            tbody.appendChild(tr);
        });
    }

    async function runSearch() {
        const qs = buildQuery();
        if (!qs) {
            summary.textContent = "Add at least one filter before searching.";
            table.style.display = "none";
            empty.style.display = "block";
            empty.textContent = "No search yet.";
            return;
        }

        summary.textContent = "Searching…";
        empty.style.display = "block";
        empty.textContent = "Loading moments…";
        table.style.display = "none";

        try {
            const res = await fetch(`/api/search-moments?${qs}`);
            const data = await res.json();
            if (!data.ok) {
                throw new Error(data.error || "Search failed");
            }

            summary.textContent = `Found ${data.count} moment(s) (showing up to 200).`;
            renderRows(data.rows || []);
        } catch (err) {
            console.error(err);
            summary.textContent = "Search failed.";
            empty.style.display = "block";
            empty.textContent = "Something went wrong. Try again.";
            table.style.display = "none";
        }
    }

    if (searchBtn) {
        searchBtn.addEventListener("click", (e) => {
            e.preventDefault();
            runSearch();
        });
    }

    // Optional: run a quick default search (e.g., common base set) on load
    // Uncomment if you want:
    // teamInput.value = "";
    // tierSelect.value = "Common";
    // runSearch();
});
