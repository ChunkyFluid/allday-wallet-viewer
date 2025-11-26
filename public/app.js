// public/app.js

const PAGE_SIZE = 200;

let allMoments = [];
let filteredMoments = [];
let currentSortKey = "last_event_ts";
let currentSortDir = "desc";
let currentPage = 1;

function getEls() {
    return {
        form: document.getElementById("wallet-form"),
        walletInput: document.getElementById("wallet-input"),
        tbody: document.getElementById("wallet-tbody"),
        title: document.getElementById("moments-title"),
        exportBtn: document.getElementById("export-csv"),
        pagerPrev: document.getElementById("wallet-page-prev"),
        pagerNext: document.getElementById("wallet-page-next"),
        pagerInfo: document.getElementById("wallet-page-info"),
        summaryCard: document.getElementById("wallet-summary-card"),
        filterTeam: document.getElementById("filter-team"),
        filterPlayer: document.getElementById("filter-player"),
        filterSeries: document.getElementById("filter-series"),
        filterSet: document.getElementById("filter-set"),
        filterTier: document.getElementById("filter-tier"),
        filterPosition: document.getElementById("filter-position"),
        filterLocked: document.getElementById("filter-locked"),
        resetFilters: document.getElementById("reset-filters"),
        table: document.getElementById("wallet-table")
    };
}

function tierToClass(tier) {
    const t = (tier || "").toLowerCase();
    if (t === "common") return "chip-common";
    if (t === "uncommon") return "chip-uncommon";
    if (t === "rare") return "chip-rare";
    if (t === "legendary") return "chip-legendary";
    if (t === "ultimate") return "chip-ultimate";
    return "";
}

function formatDate(ts) {
    if (!ts) return "";
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return ts;
    return d.toLocaleString();
}

function formatPrice(v) {
    if (v == null) return "";
    const num = Number(v);
    if (Number.isNaN(num)) return "";
    return `$${num.toFixed(2)}`;
}

function formatUsd(value) {
    const num = Number(value);
    if (!Number.isFinite(num) || num <= 0) return "$0.00";
    return num.toLocaleString(undefined, {
        style: "currency",
        currency: "USD",
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });
}

function computeTopSets(moments, limit = 5) {
    const setCounts = new Map();
    for (const m of moments) {
        const setName = m.set_name;
        if (!setName) continue;
        setCounts.set(setName, (setCounts.get(setName) || 0) + 1);
    }
    return Array.from(setCounts.entries())
        .map(([name, count]) => ({ name, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, limit);
}

function computeTopPlayers(moments, limit = 5) {
    const playerCounts = new Map();
    for (const m of moments) {
        const playerName = [m.first_name, m.last_name].filter(Boolean).join(" ");
        if (!playerName) continue;
        playerCounts.set(playerName, (playerCounts.get(playerName) || 0) + 1);
    }
    return Array.from(playerCounts.entries())
        .map(([name, count]) => ({ name, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, limit);
}

// Filter by tier (called from tier chip onclick handlers)
window.filterByTier = function(tier) {
    const els = getEls();
    if (!els || !els.filterTier) return;
    
    // Find the actual tier value in the dropdown options (case-insensitive match)
    const tierLower = tier.toLowerCase();
    let actualTierValue = null;
    
    for (const option of els.filterTier.options) {
        if (option.value && option.value.toLowerCase() === tierLower) {
            actualTierValue = option.value;
            break;
        }
    }
    
    // If we found a matching option, use it; otherwise try the original value
    const targetValue = actualTierValue || tier;
    
    // If clicking the same tier that's already selected, clear the filter
    if (els.filterTier.value === targetValue) {
        els.filterTier.value = "";
    } else {
        els.filterTier.value = targetValue;
    }
    
    applyFilters();
    applySort();
    renderPage(1);
};

function buildFilterOptions() {
    const els = getEls();
    const teams = new Set();
    const players = new Set();
    const series = new Set();
    const sets = new Set();
    const positions = new Set();
    const tiers = new Set();

    for (const r of allMoments) {
        if (r.team_name) teams.add(r.team_name);
        const playerName = [r.first_name, r.last_name].filter(Boolean).join(" ");
        if (playerName) players.add(playerName);
        if (r.series_name) series.add(r.series_name);
        if (r.set_name) sets.add(r.set_name);
        if (r.position) positions.add(r.position);
        if (r.tier) tiers.add(r.tier);
    }

    function fillSelect(select, values, { includeAllLabel = "All" } = {}) {
        if (!select) return;
        select.innerHTML = "";
        const optAll = document.createElement("option");
        optAll.value = "";
        optAll.textContent = includeAllLabel;
        select.appendChild(optAll);

        [...values]
            .sort((a, b) => String(a).localeCompare(String(b)))
            .forEach((v) => {
                const opt = document.createElement("option");
                opt.value = v;
                opt.textContent = v;
                select.appendChild(opt);
            });
    }

    fillSelect(els.filterTeam, teams, { includeAllLabel: "All teams" });
    fillSelect(els.filterPlayer, players, { includeAllLabel: "All players" });
    fillSelect(els.filterSeries, series, { includeAllLabel: "All series" });
    fillSelect(els.filterSet, sets, { includeAllLabel: "All sets" });
    fillSelect(els.filterTier, tiers, { includeAllLabel: "All tiers" });
    fillSelect(els.filterPosition, positions, { includeAllLabel: "All positions" });

    if (els.filterLocked) {
        els.filterLocked.innerHTML = "";
        const optAll = document.createElement("option");
        optAll.value = "";
        optAll.textContent = "All";
        els.filterLocked.appendChild(optAll);

        const optUnlocked = document.createElement("option");
        optUnlocked.value = "unlocked";
        optUnlocked.textContent = "Unlocked only";
        els.filterLocked.appendChild(optUnlocked);

        const optLocked = document.createElement("option");
        optLocked.value = "locked";
        optLocked.textContent = "Locked only";
        els.filterLocked.appendChild(optLocked);
    }
}

function applyFilters() {
    const els = getEls();
    const team = els.filterTeam?.value || "";
    const player = els.filterPlayer?.value || "";
    const series = els.filterSeries?.value || "";
    const setName = els.filterSet?.value || "";
    const tier = els.filterTier?.value || "";
    const position = els.filterPosition?.value || "";
    const lockedVal = els.filterLocked?.value || "";

    filteredMoments = allMoments.filter((r) => {
        if (team && r.team_name !== team) return false;

        if (player) {
            const name = [r.first_name, r.last_name].filter(Boolean).join(" ");
            if (name !== player) return false;
        }

        if (series && r.series_name !== series) return false;
        if (setName && r.set_name !== setName) return false;
        if (tier && (r.tier || "").toLowerCase() !== tier.toLowerCase()) return false;
        if (position && r.position !== position) return false;

        if (lockedVal === "locked" && !r.is_locked) return false;
        if (lockedVal === "unlocked" && r.is_locked) return false;

        return true;
    });
}

function applySort() {
    const key = currentSortKey;
    const dir = currentSortDir;

    const getValue = (row) => {
        if (key === "playerName") {
            return [row.first_name, row.last_name].filter(Boolean).join(" ");
        }
        return row[key];
    };

    filteredMoments.sort((a, b) => {
        let va = getValue(a);
        let vb = getValue(b);

        if (va == null && vb == null) return 0;
        if (va == null) return dir === "asc" ? 1 : -1;
        if (vb == null) return dir === "asc" ? -1 : 1;

        if (
            key === "serial_number" ||
            key === "max_mint_size" ||
            key === "low_ask_usd" ||
            key === "avg_sale_usd" ||
            key === "top_sale_usd"
        ) {
            va = Number(va) || 0;
            vb = Number(vb) || 0;
        } else if (key === "is_locked") {
            va = !!va;
            vb = !!vb;
            if (va === vb) return 0;
            return dir === "asc" ? (va ? 1 : -1) : va ? -1 : 1;
        } else if (key === "last_event_ts") {
            va = new Date(va).getTime() || 0;
            vb = new Date(vb).getTime() || 0;
        } else {
            va = String(va).toLowerCase();
            vb = String(vb).toLowerCase();
        }

        if (va < vb) return dir === "asc" ? -1 : 1;
        if (va > vb) return dir === "asc" ? 1 : -1;
        return 0;
    });
}

function renderPage(page) {
    const els = getEls();
    if (!els.tbody) return;

    const total = filteredMoments.length;
    const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

    currentPage = Math.max(1, Math.min(page, totalPages));

    const start = (currentPage - 1) * PAGE_SIZE;
    const end = Math.min(start + PAGE_SIZE, total);
    const slice = filteredMoments.slice(start, end);

    els.tbody.innerHTML = "";

    for (const r of slice) {
        const tr = document.createElement("tr");
        const playerName = [r.first_name, r.last_name].filter(Boolean).join(" ");
        const tierClass = tierToClass(r.tier);
        const tierHtml = r.tier ? `<span class="chip-pill-tier ${tierClass}">${r.tier}</span>` : "";

        const serial = r.serial_number ?? "";
        const max = r.max_mint_size ?? "";
        const momentUrl = r.nft_id ? `https://nflallday.com/moments/${r.nft_id}` : "";
        const marketUrl = r.edition_id ? `https://nflallday.com/listing/moment/${r.edition_id}` : "";

        const lowAsk = formatPrice(r.low_ask_usd);
        const avgSale = formatPrice(r.avg_sale_usd);
        const topSale = formatPrice(r.top_sale_usd);

        tr.innerHTML = `
      <td>${playerName || "(unknown)"}</td>
      <td>${r.team_name || ""}</td>
      <td>${r.position || ""}</td>
      <td>${tierHtml}</td>
      <td>${serial}</td>
      <td>${max}</td>
      <td>${r.series_name || ""}</td>
      <td>${r.set_name || ""}</td>
      <td>${lowAsk}</td>
      <td>${avgSale}</td>
      <td>${topSale}</td>
      <td>
        <span class="locked-pill ${r.is_locked ? "locked" : "unlocked"}">
          ${r.is_locked ? "Locked" : "Unlocked"}
        </span>
      </td>
      <td>${formatDate(r.last_event_ts)}</td>
      <td>
        <div class="link-group">
          ${momentUrl ? `<a href="${momentUrl}" target="_blank" rel="noopener">Moment</a>` : ""}
          ${marketUrl ? `<a href="${marketUrl}" target="_blank" rel="noopener">Market</a>` : ""}
        </div>
      </td>
    `;
        els.tbody.appendChild(tr);
    }

    if (els.title) {
        els.title.textContent = `Moments – ${total.toLocaleString()} total`;
    }
    if (els.pagerInfo) {
        const from = total ? start + 1 : 0;
        const to = end;
        els.pagerInfo.textContent = `Page ${currentPage} of ${totalPages} • showing ${from}-${to} of ${total}`;
    }
    if (els.pagerPrev) {
        els.pagerPrev.disabled = currentPage <= 1;
    }
    if (els.pagerNext) {
        els.pagerNext.disabled = currentPage >= totalPages;
    }

    updateSortHeaderClasses();
}

function updateSortHeaderClasses() {
    const els = getEls();
    if (!els.table) return;
    const ths = els.table.querySelectorAll("th.sortable");
    ths.forEach((th) => {
        th.classList.remove("sort-asc", "sort-desc");
        const key = th.getAttribute("data-sort-key");
        if (key === currentSortKey) {
            th.classList.add(currentSortDir === "asc" ? "sort-asc" : "sort-desc");
        }
    });
}

// Client-side cache for prices and filters
const priceCache = new Map();
const filterCache = new Map();

async function fetchWalletSummary(wallet) {
    const els = getEls();
    if (!els.summaryCard) return;

    els.summaryCard.style.display = "none";
    els.summaryCard.innerHTML = "";

    try {
        const res = await fetch(`/api/wallet-summary?wallet=${encodeURIComponent(wallet)}`);
        const data = await res.json();
        if (!data.ok) return;

        const stats = data.stats || {};
        const byTier = stats.byTier || {};

        const addressLabel = data.displayName ? `${data.displayName} · ${data.wallet}` : data.wallet;

        const holdingsLastSyncedAt = data.holdingsLastSyncedAt || data.holdingsLastEventTs || null;
        const pricesLastScrapedAt = data.pricesLastScrapedAt || null;

        // Format dollar values nicely
        const floorVal = Number(stats.floorValue || 0);
        const aspVal = Number(stats.aspValue || 0);

        const floorText = floorVal.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        });

        const aspText = aspVal.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        });

        const holdingsSyncText = holdingsLastSyncedAt ? formatDate(holdingsLastSyncedAt) : "Unknown";
        const pricesSyncText = pricesLastScrapedAt ? formatDate(pricesLastScrapedAt) : "Unknown";

        els.summaryCard.innerHTML = `
      <div class="wallet-summary-main">
        <div class="wallet-summary-label">Wallet overview</div>
        <div class="wallet-summary-address">${addressLabel}</div>
        <div class="wallet-summary-chips">
          <span class="chip">Total: ${stats.momentsTotal ?? 0}</span>
          <span class="chip">Unlocked: ${stats.unlockedCount ?? 0}</span>
          <span class="chip">Locked: ${stats.lockedCount ?? 0}</span>
          <span class="chip" title="Sum of per-edition lowest asks from edition_price_scrape across all copies in this wallet.">Floor value: $${floorText}</span>
          <span class="chip" title="Sum of per-edition average sale prices (ASP) from edition_price_scrape across all copies.">ASP value: $${aspText}</span>
        </div>
        <div class="wallet-summary-chips">
          <span class="chip">Holdings last sync: ${holdingsSyncText}</span>
          <span class="chip">Prices last scrape: ${pricesSyncText}</span>
        </div>
      </div>
      <div class="wallet-summary-chips">
        <span class="chip-pill-tier chip-common" style="cursor: pointer;" onclick="filterByTier('Common')" title="Click to filter by Common tier">Common: ${byTier.Common ?? 0}</span>
        <span class="chip-pill-tier chip-uncommon" style="cursor: pointer;" onclick="filterByTier('Uncommon')" title="Click to filter by Uncommon tier">Uncommon: ${byTier.Uncommon ?? 0}</span>
        <span class="chip-pill-tier chip-rare" style="cursor: pointer;" onclick="filterByTier('Rare')" title="Click to filter by Rare tier">Rare: ${byTier.Rare ?? 0}</span>
        <span class="chip-pill-tier chip-legendary" style="cursor: pointer;" onclick="filterByTier('Legendary')" title="Click to filter by Legendary tier">Legendary: ${byTier.Legendary ?? 0}</span>
        <span class="chip-pill-tier chip-ultimate" style="cursor: pointer;" onclick="filterByTier('Ultimate')" title="Click to filter by Ultimate tier">Ultimate: ${byTier.Ultimate ?? 0}</span>
      </div>
    `;
        els.summaryCard.style.display = "flex";
    } catch (err) {
        console.error("fetchWalletSummary error", err);
    }
}

function updateSummaryWithStats(wallet) {
    const els = getEls();
    if (!els.summaryCard || !allMoments || allMoments.length === 0) return;

    const topSets = computeTopSets(allMoments);
    const topPlayers = computeTopPlayers(allMoments);

    // Find the main summary div and append stats
    const mainDiv = els.summaryCard.querySelector(".wallet-summary-main");
    if (!mainDiv) return;

    // Remove any existing stats divs
    const existingStats = mainDiv.querySelectorAll(".wallet-stats-extra");
    existingStats.forEach(el => el.remove());

    // Add new stats
    if (topSets.length > 0 || topPlayers.length > 0) {
        const statsDiv = document.createElement("div");
        statsDiv.className = "wallet-stats-extra";
        statsDiv.style.cssText = "margin-top: 0.5rem;";
        
        if (topSets.length > 0) {
            const setsDiv = document.createElement("div");
            setsDiv.className = "wallet-summary-chips";
            setsDiv.style.cssText = "margin-top: 0.5rem;";
            setsDiv.innerHTML = `
                <span style="font-size: 0.7rem; color: var(--text-muted); margin-right: 0.5rem;">Top sets:</span>
                ${topSets.map(s => `<span class="chip" style="font-size: 0.7rem;">${s.name} (${s.count})</span>`).join('')}
            `;
            statsDiv.appendChild(setsDiv);
        }
        
        if (topPlayers.length > 0) {
            const playersDiv = document.createElement("div");
            playersDiv.className = "wallet-summary-chips";
            playersDiv.style.cssText = "margin-top: 0.5rem;";
            playersDiv.innerHTML = `
                <span style="font-size: 0.7rem; color: var(--text-muted); margin-right: 0.5rem;">Top players:</span>
                ${topPlayers.map(p => `<span class="chip" style="font-size: 0.7rem;">${p.name} (${p.count})</span>`).join('')}
            `;
            statsDiv.appendChild(playersDiv);
        }
        
        mainDiv.appendChild(statsDiv);
    }
}

async function fetchWalletMoments(wallet) {
    const res = await fetch(`/api/query?wallet=${encodeURIComponent(wallet)}`);
    const data = await res.json();
    if (!data.ok) {
        throw new Error(data.error || "Failed to load wallet");
    }
    return data.rows || [];
}

async function attachPricesToMoments(moments) {
    const editionIds = [...new Set(moments.map((r) => r.edition_id).filter(Boolean))];
    if (!editionIds.length) return;

    // Check cache first
    const uncachedIds = [];
    const lowAskMap = {};
    const aspMap = {};
    const topSaleMap = {};

    for (const id of editionIds) {
        const cached = priceCache.get(id);
        if (cached) {
            if (cached.lowAsk != null) lowAskMap[id] = cached.lowAsk;
            if (cached.asp != null) aspMap[id] = cached.asp;
            if (cached.topSale != null) topSaleMap[id] = cached.topSale;
        } else {
            uncachedIds.push(id);
        }
    }

    // For very large wallets, a single /api/prices?editions=... URL can
    // exceed browser/server limits. Chunk the requests and merge results.
    const chunkSize = 200;

    try {
        for (let i = 0; i < uncachedIds.length; i += chunkSize) {
            const chunk = uncachedIds.slice(i, i + chunkSize);
            const qs = encodeURIComponent(chunk.join(","));
            const res = await fetch(`/api/prices?editions=${qs}`);
            const data = await res.json();

            if (data && data.ok !== false) {
                const low = data.lowAsk || {};
                const asp = data.asp || {};
                const top = data.topSale || {};

                // Merge into maps
                Object.assign(lowAskMap, low);
                Object.assign(aspMap, asp);
                Object.assign(topSaleMap, top);

                // Cache results
                for (const id of chunk) {
                    priceCache.set(id, {
                        lowAsk: low[id] ?? null,
                        asp: asp[id] ?? null,
                        topSale: top[id] ?? null
                    });
                }
            }
        }

        for (const r of moments) {
            const id = r.edition_id;
            if (!id) continue;
            const low = lowAskMap[id];
            const avg = aspMap[id];
            const top = topSaleMap[id];

            r.low_ask_usd = low != null ? Number(low) : null;
            r.avg_sale_usd = avg != null ? Number(avg) : null;
            r.top_sale_usd = top != null ? Number(top) : null;
        }
    } catch (err) {
        console.error("attachPricesToMoments error", err);
    }
}

function wireEvents() {
    const els = getEls();
    if (!els.form || !els.walletInput) return;

    els.form.addEventListener("submit", (e) => {
        e.preventDefault();
        const wallet = (els.walletInput.value || "").trim();
        if (!wallet) return;
        window.runQuery(wallet);
    });

    const filterEls = [
        els.filterTeam,
        els.filterPlayer,
        els.filterSeries,
        els.filterSet,
        els.filterTier,
        els.filterPosition,
        els.filterLocked
    ].filter(Boolean);

    filterEls.forEach((sel) => {
        sel.addEventListener("change", () => {
            applyFilters();
            applySort();
            renderPage(1);
        });
    });

    if (els.resetFilters) {
        els.resetFilters.addEventListener("click", () => {
            filterEls.forEach((sel) => {
                sel.value = "";
            });
            applyFilters();
            applySort();
            renderPage(1);
        });
    }

    if (els.table) {
        const ths = els.table.querySelectorAll("th.sortable");
        ths.forEach((th) => {
            th.addEventListener("click", () => {
                const key = th.getAttribute("data-sort-key");
                if (!key) return;
                if (currentSortKey === key) {
                    currentSortDir = currentSortDir === "asc" ? "desc" : "asc";
                } else {
                    currentSortKey = key;
                    currentSortDir = "asc";
                }
                applySort();
                renderPage(1);
            });
        });
    }

    if (els.pagerPrev) {
        els.pagerPrev.addEventListener("click", () => {
            renderPage(currentPage - 1);
        });
    }
    if (els.pagerNext) {
        els.pagerNext.addEventListener("click", () => {
            renderPage(currentPage + 1);
        });
    }

    if (els.exportBtn) {
        els.exportBtn.addEventListener("click", () => {
            exportCsv();
        });
    }
}

function exportCsv() {
    const rows = filteredMoments;
    if (!rows.length) return;

    const headers = [
        "wallet_address",
        "nft_id",
        "edition_id",
        "play_id",
        "series_id",
        "set_id",
        "tier",
        "serial_number",
        "max_mint_size",
        "first_name",
        "last_name",
        "team_name",
        "position",
        "jersey_number",
        "series_name",
        "set_name",
        "is_locked",
        "last_event_ts",
        "low_ask_usd",
        "avg_sale_usd",
        "top_sale_usd"
    ];

    const lines = [];
    lines.push(headers.join(","));

    for (const r of rows) {
        const line = headers
            .map((h) => {
                let v = r[h];
                if (v == null) v = "";
                const s = String(v).replace(/"/g, '""');
                if (s.search(/[",\n]/) >= 0) {
                    return `"${s}"`;
                }
                return s;
            })
            .join(",");
        lines.push(line);
    }

    const csv = lines.join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);

    const a = document.createElement("a");
    a.href = url;
    a.download = "wallet-moments.csv";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

// This is what the HTML calls
window.runQuery = async function runQuery(walletRaw) {
    const els = getEls();
    const wallet = (walletRaw || "").trim().toLowerCase();
    if (!wallet) return;

    if (els.title) {
        els.title.textContent = "Loading wallet…";
    }
    if (els.tbody) {
        els.tbody.innerHTML = "";
    }
    if (els.pagerInfo) {
        els.pagerInfo.textContent = "";
    }

    // Clear any previous error state
    const errorDiv = document.getElementById("wallet-error");
    if (errorDiv) {
        errorDiv.remove();
    }

    try {
        await fetchWalletSummary(wallet);
        allMoments = await fetchWalletMoments(wallet);

        await attachPricesToMoments(allMoments);

        // Update summary with mini stats now that we have moments
        updateSummaryWithStats(wallet);

        buildFilterOptions();
        applyFilters();
        applySort();
        renderPage(1);
    } catch (err) {
        console.error("runQuery error", err);
        const errorMsg = err.message || String(err);
        
        if (els.title) {
            els.title.textContent = "Failed to load wallet";
        }
        
        // Show user-friendly error with retry button
        if (els.tbody) {
            els.tbody.innerHTML = "";
            const errorDiv = document.createElement("div");
            errorDiv.id = "wallet-error";
            errorDiv.style.cssText = "padding: 2rem; text-align: center; color: var(--text-muted);";
            errorDiv.innerHTML = `
                <div style="margin-bottom: 1rem; color: var(--danger);">❌ ${errorMsg}</div>
                <button class="btn-primary" onclick="window.runQuery('${wallet}')" style="margin-top: 0.5rem;">Retry</button>
            `;
            els.tbody.appendChild(errorDiv);
        }
    }
};

document.addEventListener("DOMContentLoaded", () => {
    wireEvents();
    updateSortHeaderClasses();
});
